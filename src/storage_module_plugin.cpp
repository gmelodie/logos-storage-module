#include "storage_module_plugin.h"

#include "MixConfig.h"
#include "ffi.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using json = nlohmann::json;

// Module package version, injected at build time from metadata.json.
#ifndef STORAGE_MODULE_VERSION
#define STORAGE_MODULE_VERSION "0.0.0-dev"
#endif

// This is the upper bound for the time we typically expect for most
// operations to complete.
#define DEFAULT_TIMEOUT_MS 1000
// Time we'll wait before a manifest is retrieved from the network,
// even over mix.
#define FETCH_MANIFEST_TIMEOUT_MS 30000
// The constructor builds the whole node, so it gets a longer budget.
#define CREATE_TIMEOUT_MS 10000

// ---------------------------------------------------------------------------
// Storage Module — libstorage C++ wrapper
//
// libstorage functions are asynchronous: a command is dispatched to a worker
// thread and the result arrives via a reply callback (ffi.h).  This file implements
// several callback context types (a strategy pattern) that handle results in
// different ways.  The type hierarchy is:
//
//   AsyncCallbackBase
//     │  Base for all fire-and-forget async contexts.  The dispatcher calls
//     │  handleResponse() and deletes the context.
//     │
//     ├── RunningEventCtx   – start/stop: emits a named event to the host
//     │                       and moves the node's running flag.
//     ├── ConnectCtx        – connect: emits storageConnect.
//     ├── UploadFileCtx     – file upload: done event.
//     └── UploadChunkCtx    – single-chunk upload: emits progress event.
//
//   SyncCtx (ffi.h)
//     Synchronous-wait pattern: the caller allocates a SyncCtx on the heap,
//     issues the command, then blocks on the condvar.  On callback arrival
//     the result is copied into the context and the condvar is signalled.
//     An "abandoned" flag handles the rare race where the caller times out
//     before the callback fires — in that case the callback itself deletes
//     the context instead of the caller.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// base64 encoding helper — needed to safely embed binary chunk data in JSON.
// nlohmann::json::dump() requires valid UTF-8; raw download chunks are
// arbitrary bytes and will throw type_error.316 without encoding.
// ---------------------------------------------------------------------------
static std::string base64Encode(const char* data, size_t len) {
    static const char kTable[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    for (size_t i = 0; i < len; i += 3) {
        const unsigned char b0 = static_cast<unsigned char>(data[i]);
        const unsigned char b1 = (i + 1 < len) ? static_cast<unsigned char>(data[i + 1]) : 0;
        const unsigned char b2 = (i + 2 < len) ? static_cast<unsigned char>(data[i + 2]) : 0;
        out += kTable[b0 >> 2];
        out += kTable[((b0 & 0x03) << 4) | (b1 >> 4)];
        out += (i + 1 < len) ? kTable[((b1 & 0x0F) << 2) | (b2 >> 6)] : '=';
        out += (i + 2 < len) ? kTable[b2 & 0x3F] : '=';
    }
    return out;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

static constexpr int64_t DEFAULT_CHUNK_SIZE = 65536;

static std::string fromMsg(const char* msg, size_t len) {
    return (msg && len > 0) ? std::string(msg, len) : std::string();
}

// ---------------------------------------------------------------------------
// JSON event helpers — build and emit common response patterns.
//
// Each helper takes a pointer-to-member to the typed event method declared
// in `storage_module_plugin.h`'s `logos_events:` block; the method bodies
// are codegen-emitted in `storage_module_events_cdylib.cpp` and dispatch
// through `LogosModuleContext::emitEventImpl_`.
//
// Serialization of the JSON payload runs inside libstorage callbacks where
// uncaught exceptions would be fatal; do that first under a try/catch and
// only invoke the typed event method when we have a valid string.
// ---------------------------------------------------------------------------

using StorageEvent = void (StorageModuleImpl::*)(const std::string&);

static void emitJsonEvent(StorageModuleImpl* impl, StorageEvent emit,
                          const json& payload, const char* caller) {
    std::string data;
    try {
        data = payload.dump();
    } catch (const std::exception& e) {
        fprintf(stderr, "%s: failed to serialize event payload: %s\n", caller, e.what());
        return;
    } catch (...) {
        fprintf(stderr, "%s: failed to serialize event payload (unknown error)\n", caller);
        return;
    }
    (impl->*emit)(data);
}

static void emitBasicResponse(StorageModuleImpl* impl, StorageEvent emit,
                              int ret, const std::string& message, const char* caller) {
    json j;
    j["success"] = (ret == RET_OK);
    j["message"] = message;
    emitJsonEvent(impl, emit, j, caller);
}

// `total` is fileSize on upload, the manifest's datasetSize on download.
static void emitSessionProgress(StorageModuleImpl* impl, StorageEvent emit,
                                const std::string& sessionId, int64_t bytes,
                                int64_t total, const char* caller) {
    json j;
    j["success"] = true;
    j["sessionId"] = sessionId;
    j["bytes"] = bytes;
    j["total"] = total;
    emitJsonEvent(impl, emit, j, caller);
}

static void emitSessionResult(StorageModuleImpl* impl, StorageEvent emit,
                              int ret, const std::string& sessionId,
                              const std::string& message,
                              const std::string& okField, const char* caller) {
    json j;
    j["success"] = (ret == RET_OK);
    j["sessionId"] = sessionId;
    if (ret == RET_OK && !okField.empty()) j[okField] = message;
    else if (ret != RET_OK) j["error"] = message;
    emitJsonEvent(impl, emit, j, caller);
}

// ---------------------------------------------------------------------------
// Transfer progress — libstorage publishes it as events on its event thread.
//
// init() registers a listener for on_upload_progress, on_download_chunk and
// on_download_done.  The handlers match a running transfer by session ID
// (upload) or CID (download) and emit "storageUploadProgress" /
// "storageDownloadProgress":
//
//   upload, file-mode download:
//     Throttled to at most one event per percentage point (max 100 events
//     total) to avoid flooding the caller.  When the total is 0 (unknown),
//     every event is forwarded.
//     JSON payload: {success:true, sessionId, bytes, total}
//
//   chunk-mode download:
//     The data is base64-encoded in the "chunk" field.
//     JSON payload: {success:true, sessionId, chunk:<base64>}
// ---------------------------------------------------------------------------

struct Progress {
    int64_t total = 0;
    int64_t done = 0;
    int64_t pending = 0;
    int lastPercent = -1;
    // Download only: deliver the bytes themselves instead of a byte count.
    bool asChunks = false;
};

// Returns 0 while still inside the percentage point reported last.
static int64_t stepProgress(Progress& p, int64_t bytes) {
    p.done += bytes;
    p.pending += bytes;
    if (p.total > 0) {
        int percent = static_cast<int>((p.done * 100LL) / p.total);
        if (percent <= p.lastPercent) return 0;
        p.lastPercent = percent;
    }
    int64_t batch = p.pending;
    p.pending = 0;
    return batch;
}

// Upload session IDs restart at zero on every node, so the owner is part of the key.
using TransferKey = std::pair<const StorageModuleImpl*, std::string>;

struct StepResult {
    bool tracked = false;
    bool asChunks = false;
    int64_t batch = 0;
    int64_t total = 0;
};

// The event thread and the dispatch thread both touch a transfer, so the table owns the lock.
class TransferTable {
public:
    void track(const TransferKey& key, const Progress& p) {
        std::lock_guard<std::mutex> lock(mtx);
        transfers[key] = p;
    }

    void forget(const TransferKey& key) {
        std::lock_guard<std::mutex> lock(mtx);
        transfers.erase(key);
    }

    void forgetOwner(const StorageModuleImpl* owner) {
        std::lock_guard<std::mutex> lock(mtx);
        for (auto it = transfers.begin(); it != transfers.end();) {
            it = (it->first.first == owner) ? transfers.erase(it) : std::next(it);
        }
    }

    StepResult advance(const TransferKey& key, int64_t bytes) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = transfers.find(key);
        if (it == transfers.end()) return {};

        StepResult step;
        step.tracked = true;
        step.asChunks = it->second.asChunks;
        step.total = it->second.total;
        if (!step.asChunks) step.batch = stepProgress(it->second, bytes);
        return step;
    }

private:
    std::mutex mtx;
    std::map<TransferKey, Progress> transfers;
};

static TransferTable uploads;
static TransferTable downloads;

static void onUploadProgressEvent(const OnUploadProgressPayload* evt, void* userData) {
    if (!evt || !userData) return;
    auto* impl = static_cast<StorageModuleImpl*>(userData);
    std::string sessionId = fromFfi(evt->sessionId);

    StepResult step = uploads.advance({impl, sessionId},
                                      static_cast<int64_t>(evt->storedBytes));
    if (step.batch == 0) return;

    emitSessionProgress(impl, &StorageModuleImpl::storageUploadProgress, sessionId,
                        step.batch, step.total, "onUploadProgressEvent");
}

static void onDownloadChunkEvent(const OnDownloadChunkPayload* evt, void* userData) {
    if (!evt || !userData) return;
    auto* impl = static_cast<StorageModuleImpl*>(userData);
    std::string cid = fromFfi(evt->cid);

    StepResult step = downloads.advance({impl, cid}, static_cast<int64_t>(evt->data.len));
    if (!step.tracked) return;

    if (step.asChunks) {
        json j;
        j["success"] = true;
        j["sessionId"] = cid;
        j["chunk"] = base64Encode(reinterpret_cast<const char*>(evt->data.data), evt->data.len);
        emitJsonEvent(impl, &StorageModuleImpl::storageDownloadProgress, j,
                      "onDownloadChunkEvent");
        return;
    }

    if (step.batch == 0) return;
    emitSessionProgress(impl, &StorageModuleImpl::storageDownloadProgress, cid,
                        step.batch, step.total, "onDownloadChunkEvent");
}

// Comes after the last chunk on the same queue, which the stream reply does not.
// Emits "storageDownloadDone".
// JSON payload on success:  {success:true,  sessionId}
// JSON payload on failure:  {success:false, sessionId, error}
static void onDownloadDoneEvent(const OnDownloadDonePayload* evt, void* userData) {
    if (!evt || !userData) return;
    auto* impl = static_cast<StorageModuleImpl*>(userData);
    std::string cid = fromFfi(evt->cid);
    std::string error = fromFfi(evt->error);

    downloads.forget({impl, cid});
    emitSessionResult(impl, &StorageModuleImpl::storageDownloadDone,
                      error.empty() ? RET_OK : RET_ERR, cid, error, "",
                      "onDownloadDoneEvent");
}

// A listener registration answers with its id, and zero means it failed.
static bool registerTransferListeners(StorageCtx* ctx, StorageModuleImpl* impl) {
    if (storage_ctx_add_on_upload_progress_listener(ctx, onUploadProgressEvent, impl) == 0)
        return false;

    if (storage_ctx_add_on_download_chunk_listener(ctx, onDownloadChunkEvent, impl) == 0)
        return false;

    return storage_ctx_add_on_download_done_listener(ctx, onDownloadDoneEvent, impl) != 0;
}

// ---------------------------------------------------------------------------
// Concrete async context implementations
// ---------------------------------------------------------------------------

// Dispatches the typed event member pointer passed in `event` on completion,
// and moves the node's running flag when the command lands.
// JSON payload: {success, message}.
enum class NodeCommand { Start, Stop };

struct RunningEventCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    StorageEvent event;
    NodeCommand command;
    std::atomic<bool>* running;
    std::atomic<bool>* busy;

    RunningEventCtx(StorageModuleImpl* i, StorageEvent ev, NodeCommand c,
                    std::atomic<bool>* r, std::atomic<bool>* b)
        : impl(i), event(ev), command(c), running(r), busy(b) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        if (ret == RET_OK) {
            running->store(command == NodeCommand::Start);
        }

        busy->store(false);

        emitBasicResponse(impl, event, ret, fromMsg(msg, len), "RunningEventCtx");
    }
};

// Emits storageConnect.
// JSON payload: {success, message}.
struct ConnectCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;

    explicit ConnectCtx(StorageModuleImpl* i) : impl(i) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        emitBasicResponse(impl, &StorageModuleImpl::storageConnect, ret,
                          fromMsg(msg, len), "ConnectCtx");
    }
};

// Handles the file upload reply.  Progress comes from onUploadProgressEvent.
//
// Emits "storageUploadDone".
// JSON payload: {success, sessionId, cid} on success; {success:false, sessionId, error} on failure.
struct UploadFileCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    std::string sessionId;

    UploadFileCtx(StorageModuleImpl* i, std::string sid)
        : impl(i), sessionId(std::move(sid)) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        uploads.forget({impl, sessionId});
        emitSessionResult(impl, &StorageModuleImpl::storageUploadDone, ret,
                          sessionId, fromMsg(msg, len), "cid", "UploadFileCtx");
    }
};

// Handles a single manual chunk upload.
// Emits "storageUploadProgress" on completion.
// JSON payload on success:  {success:true,  sessionId, bytes}
// JSON payload on failure:  {success:false, sessionId, error}
//
// Note: we do NOT cancel the upload session on failure — a failed chunk does
// not corrupt the session, and the caller may choose to retry or abort.
struct UploadChunkCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    std::string sessionId;
    std::string chunk;

    UploadChunkCtx(StorageModuleImpl* i, std::string sid, std::string c)
        : impl(i), sessionId(std::move(sid)), chunk(std::move(c)) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        json j;
        j["success"] = (ret == RET_OK);
        j["sessionId"] = sessionId;
        if (ret == RET_OK) j["bytes"] = static_cast<int64_t>(chunk.size());
        else j["error"] = fromMsg(msg, len);
        emitJsonEvent(impl, &StorageModuleImpl::storageUploadProgress, j,
                      "UploadChunkCtx");
    }
};

// Handles a background manifest fetch.  The DHT lookup can take a
// long time so it uses async callbacks to avoid blocking.
//
// On completion emits "storageDownloadManifestDone".
// JSON payload on success:  {success:true,  cid, manifest:{…}}
// JSON payload on failure:  {success:false, cid, error}
struct FetchManifestCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    std::string cid;

    FetchManifestCtx(StorageModuleImpl* i, std::string c)
        : impl(i), cid(std::move(c)) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        json j;
        j["cid"] = cid;
        if (ret == RET_OK) {
            try {
                j["manifest"] = json::parse(fromMsg(msg, len));
                j["success"] = true;
            } catch (...) {
                j["success"] = false;
                j["error"] = "Failed to parse manifest.";
            }
        } else {
            j["success"] = false;
            j["error"] = fromMsg(msg, len);
        }
        emitJsonEvent(impl, &StorageModuleImpl::storageDownloadManifestDone, j,
                      "FetchManifestCtx");
    }
};

// Handles a background content removal.  The delete may touch the network
// and can take a while, so it uses async callbacks to avoid blocking.
//
// On completion emits "storageRemoveDone".
// JSON payload on success:  {success:true,  cid}
// JSON payload on failure:  {success:false, cid, error}
struct RemoveCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    std::string cid;

    RemoveCtx(StorageModuleImpl* i, std::string c)
        : impl(i), cid(std::move(c)) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        json j;
        j["cid"] = cid;
        j["success"] = (ret == RET_OK);
        if (ret != RET_OK) {
            j["error"] = fromMsg(msg, len);
        }
        emitJsonEvent(impl, &StorageModuleImpl::storageRemoveDone, j,
                      "RemoveCtx");
    }
};

// ---------------------------------------------------------------------------
// StorageModuleImpl
// ---------------------------------------------------------------------------

StorageModuleImpl::StorageModuleImpl() : storageCtx(nullptr) {
    fprintf(stderr, "StorageModuleImpl: Initializing...\n");
}

StorageModuleImpl::~StorageModuleImpl() {
    if (storageCtx) {
        fprintf(stderr,
                "StorageModuleImpl: Warning - storage context was not "
                "destroyed before plugin destruction\n");
        storageCtx = nullptr;
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
constexpr int configVersion = 3;

namespace {

std::string storageHome() {
    const char* home = std::getenv("HOME");
#ifdef _WIN32
    if (!home) home = std::getenv("USERPROFILE");
#endif
    if (!home || !*home) return {};

    return (fs::path(home) / ".logos_storage").string();
}

// The configuration saved in the storage home by init().
// An empty object when there is none.
json persistedConfig() {
    const std::string home = storageHome();

    if (home.empty()) {
        return json::object();
    }

    const fs::path path = fs::path(home) / "config.json";

    std::error_code ec;
    const bool exists = fs::exists(path, ec);

    if (ec) {
        throw std::runtime_error("cannot access " + path.string() + ": " + ec.message());
    }

    if (!exists) {
        return json::object();
    }

    std::ifstream file(path);

    if (!file) {
        throw std::runtime_error("cannot read " + path.string());
    }

    return json::parse(file);
}

void persistConfig(json config) {
    const std::string home = storageHome();

    if (home.empty()) {
        fprintf(stderr, "StorageModuleImpl::init: config not persisted, HOME is not set\n");
        return;
    }

    std::error_code ec;
    fs::create_directories(home, ec);

    const fs::path path = fs::path(home) / "config.json";
    std::ofstream file(path);

    file << config.dump();
    file.close();

    if (!file) {
        fprintf(stderr, "StorageModuleImpl::init: cannot write %s, config: %s\n",
                path.string().c_str(), config.dump().c_str());
    }
}

// Legacy bootstrap nodes used in old version.
// Those bootstrap should be replaced by network configuration.
const char* const legacyBootstrapNodes[] = {
    "spr:CiUIAhIhA-VlcoiRm02KyIzrcTP-ljFpzTljfBRRKTIvhMIwqBqWEgIDARpJCicAJQgCEiED5WVyiJGbTYrIjOtxM_6WMWnNOWN8FFEpMi-EwjCoGpYQs8n8wQYaCwoJBHTKubmRAnU6GgsKCQR0yrm5kQJ1OipHMEUCIQDwUNsfReB4ty7JFS5WVQ6n1fcko89qVAOfQEHixa03rgIgan2-uFNDT-r4s9TOkLe9YBkCbsRWYCHGGVJ25rLj0QE",
    "spr:CiUIAhIhApIj9p6zJDRbw2NoCo-tj98Y760YbppRiEpGIE1yGaMzEgIDARpJCicAJQgCEiECkiP2nrMkNFvDY2gKj62P3xjvrRhumlGISkYgTXIZozMQvcz8wQYaCwoJBAWhF3WRAnVEGgsKCQQFoRd1kQJ1RCpGMEQCIFZB84O_nzPNuViqEGRL1vJTjHBJ-i5ZDgFL5XZxm4HAAiB8rbLHkUdFfWdiOmlencYVn0noSMRHzn4lJYoShuVzlw",
    "spr:CiUIAhIhApqRgeWRPSXocTS9RFkQmwTZRG-Cdt7UR2N7POoz606ZEgIDARpJCicAJQgCEiECmpGB5ZE9JehxNL1EWRCbBNlEb4J23tRHY3s86jPrTpkQj8_8wQYaCwoJBAXfEfiRAnVOGgsKCQQF3xH4kQJ1TipGMEQCIGWJMsF57N1iIEQgTH7IrVOgEgv0J2P2v3jvQr5Cjy-RAiAy4aiZ8QtyDvCfl_K_w6SyZ9csFGkRNTpirq_M_QNgKw",
};

// Drop the bootstrap nodes this build no longer serves and keep the rest.
json withoutLegacyBootstrap(const json& bootstrap) {
    json kept = json::array();

    for (const auto& node : bootstrap) {
        if (!node.is_string()) {
            // Should not happen: bootstrap nodes should be strings.
            continue;
        }

        const std::string spr = node.get<std::string>();
        bool isLegacy = false;

        for (const char* legacy : legacyBootstrapNodes) {
            if (spr == legacy) {
                isLegacy = true;
                break;
            }
        }

        if (!isLegacy) {
            kept.push_back(node);
        }
    }

    return kept;
}

json mixConfiguration(const std::string& network) {
    try {
        const json config = json::parse(MIX_CONFIG_JSON);

        if (!config.is_object() || !config.contains(network)) {
            return json::object();
        }

        return config.at(network);
    } catch (const std::exception&) {
        return json::object();
    }
}

// After NAT Traversal, nat options were reduced to "auto" or "extip:<address>"
bool isLegacyNat(const json& nat) {
    if (!nat.is_string()) {
        return true;
    }

    const std::string value = nat.get<std::string>();
    return value != "auto" && value.rfind("extip:", 0) != 0;
}

// A node with its own bootstrap nodes, or with none, is not on a preset network.
bool hasCustomBootstrap(const json& obj) {
    if (obj.value("no-bootstrap-node", false)) {
        return true;
    }

    return obj.contains("bootstrap-node") && !obj["bootstrap-node"].empty();
}

json migrateV0toV1(json obj) {
    if (!obj.contains("bootstrap-node") || !obj["bootstrap-node"].is_array()) {
        return obj;
    }

    const json kept = withoutLegacyBootstrap(obj["bootstrap-node"]);

    if (kept.empty()) {
        obj.erase("bootstrap-node");
    } else {
        obj["bootstrap-node"] = kept;
    }

    return obj;
}

json migrateV1toV2(json obj) {
    if (!obj.contains("mix-enabled")) {
        // Don't enable Mix by default on a custom bootstrap network.
        obj["mix-enabled"] = !hasCustomBootstrap(obj);
    }

    if (!obj.contains("nat-schedule-interval")) {
        // Shorter than libstorage's own default, for quicker NAT diagnostics.
        obj["nat-schedule-interval"] = "60s";
    }

    if (obj.contains("nat") && isLegacyNat(obj["nat"])) {
        obj.erase("nat");
    }

    return obj;
}

json migrateV2toV3(json obj) {
    // Discovery moved from discv5 to the libp2p DHT: there is no UDP port left.
    obj.erase("disc-port");

    return obj;
}

json migrateConfigVersion(json obj) {
    const int version = obj.value("config-version", 0);

    if (version >= configVersion) {
        return obj;
    }

    switch (version) {
    case 0:
        obj = migrateV0toV1(obj);
        [[fallthrough]];
    case 1:
        obj = migrateV1toV2(obj);
        [[fallthrough]];
    case 2:
        obj = migrateV2toV3(obj);
    }

    obj["config-version"] = configVersion;

    return obj;
}

// Sync the Mix config from the network preset,
// unless the user has provided a custom bootstrap list.
//
// If a network is passed in parameter and mix-enabled is true,
// the Mix config is synced from the network preset.
json syncMixConfig(json obj) {
    if (!obj.value("mix-enabled", false)) {
        return obj;
    }

    if (hasCustomBootstrap(obj)) {
        return obj;
    }

    const std::string network = obj.value("network", std::string("logos.test"));
    const json mix = mixConfiguration(network);

    if (mix.empty()) {
        return obj;
    }

    obj["dht-mix-proxy"] = mix.value("dht-mix-proxy", json::array());
    obj["mix-pool-json"] = mix.value("mix-pool-json", "");

    return obj;
}

// Throws when the config holds a mistyped value or the storage home cannot be resolved.
json normalizeConfig(json config) {
    if (!config.is_object()) {
        throw std::runtime_error("expected a JSON object");
    }

    config = syncMixConfig(config);

    if (!config.contains("data-dir")) {
        // logos-storage-nim's own default differs per platform. One path keeps
        // every consumer on the same repository.
        const std::string home = storageHome();

        if (home.empty()) {
            throw std::runtime_error("cannot resolve the storage home: HOME is not set");
        }

        config["data-dir"] = (fs::path(home) / "data").string();
    }

    return config;
}

}

StdLogosResult StorageModuleImpl::loadConfigOrDefault() {
    try {
        return {true, normalizeConfig(migrateConfigVersion(persistedConfig())).dump(), ""};
    } catch (const std::exception& e) {
        return {false, {}, std::string("Invalid configuration: ") + e.what()};
    }
}

bool StorageModuleImpl::init(const std::string& cfg) {
    fprintf(stderr, "StorageModuleImpl::init called\n");

    if (storageCtx) {
        fprintf(stderr, "StorageModuleImpl::init: context already initialized\n");
        return false;
    }

    std::string config = cfg;
    json parsed;

    try {
        parsed = json::parse(cfg);
    } catch (const std::exception& e) {
        fprintf(stderr, "StorageModuleImpl::init: config left as-is, %s\n", e.what());
    }

    if (parsed.is_object()) {
        try {
            parsed = normalizeConfig(parsed);
        } catch (const std::exception& e) {
            fprintf(stderr, "StorageModuleImpl::init: invalid config, %s\n", e.what());
            return false;
        }

        if (!parsed.contains("config-version")) {
            parsed["config-version"] = configVersion;
        }

        json storageConfig = parsed;
        storageConfig.erase("config-version");
        config = storageConfig.dump();
    }

    auto* sctx = new SyncCtx();
    storage_ctx_create(config.c_str(), syncCreateCallback, sctx);
    SyncResult r = waitSync(sctx, CREATE_TIMEOUT_MS);

    if (!r.ok || !r.created) {
        fprintf(stderr, "StorageModuleImpl::init failed: %s\n",
                r.message.c_str());
        return false;
    }
    storageCtx = r.created;

    if (!registerTransferListeners(storageCtx, this)) {
        fprintf(stderr, "StorageModuleImpl::init: failed to register the "
                        "libstorage event listeners\n");
        storage_ctx_destroy(storageCtx);
        storageCtx = nullptr;
        return false;
    }

    if (parsed.is_object()) {
        persistConfig(parsed);
    }

    return true;
}

bool StorageModuleImpl::start() {
    fprintf(stderr, "StorageModuleImpl::start called\n");

    if (!storageCtx) {
        fprintf(stderr, "StorageModuleImpl::start: context not initialized\n");
        return false;
    }

    // Handle concurrent start requests.
    //
    // First call:
    //  - nodeBusy -> false
    //  - expected -> false
    //  - compare_exchange_strong returns true because nodeBusy switches to true
    //  - storage_start will be called
    //
    // Second call:
    //  - nodeBusy -> true
    //  - expected -> false
    //  - compare_exchange_strong returns false because nodeBusy is already true
    //  - storage_start will not be called
    //
    // Another way to look at it with 2 threads A and B:
    // A : start()
    // B : start()
    // A :   compare_exchange(false -> true)      -> success, true
    // B :   compare_exchange(false -> true)      -> fails, false
    // B :   return false
    // A :   nodeRunning.load() -> false
    // A :   storage_ctx_start(...)
    // A : callback response received
    // A :   nodeRunning = true, nodeBusy = false
    bool expected = false;
    if (!nodeBusy.compare_exchange_strong(expected, true)) {
        fprintf(stderr, "StorageModuleImpl::start: node is busy\n");
        return false;
    }

    if (nodeRunning.load()) {
        nodeBusy.store(false);
        emitBasicResponse(this, &StorageModuleImpl::storageStart, RET_OK, "",
                          "StorageModuleImpl::start");
        return true;
    }

    auto* ctx = new RunningEventCtx(this, &StorageModuleImpl::storageStart,
                                    NodeCommand::Start, &nodeRunning,
                                    &nodeBusy);

    // On error the callback has already run: it cleared nodeBusy and freed ctx.
    if (asyncSend(ctx, storage_ctx_start, storageCtx) != RET_OK) {
        return false;
    }

    return true;
}

StdLogosResult StorageModuleImpl::stop() {
    fprintf(stderr, "StorageModuleImpl::stop called\n");

    if (!storageCtx) {
        return {false, {}, "Storage context not initialized."};
    }

    bool expected = false;
    if (!nodeBusy.compare_exchange_strong(expected, true)) {
        return {false, {}, "Node is busy starting or stopping."};
    }

    auto* ctx = new RunningEventCtx(this, &StorageModuleImpl::storageStop,
                                    NodeCommand::Stop, &nodeRunning,
                                    &nodeBusy);

    // On error the callback has already run: it cleared nodeBusy and freed ctx.
    if (asyncSend(ctx, storage_ctx_stop, storageCtx) != RET_OK) {
        return {false, {}, "Failed to send stop command."};
    }

    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::destroy() {
    fprintf(stderr, "StorageModuleImpl::destroy called\n");

    if (!storageCtx) {
        return {false, {}, "Storage context not initialized."};
    }

    bool expected = false;
    if (!nodeBusy.compare_exchange_strong(expected, true)) {
        return {false, {}, "Node is busy starting or stopping."};
    }

    return destroyContext();
}

// Expects the caller to hold nodeBusy, and releases it.
StdLogosResult StorageModuleImpl::destroyContext() {
    // The binding frees the handle even when the teardown fails.
    int ret = storage_ctx_destroy(storageCtx);
    storageCtx = nullptr;
    uploads.forgetOwner(this);
    downloads.forgetOwner(this);
    nodeRunning.store(false);
    nodeBusy.store(false);

    if (ret != RET_OK) return {false, {}, "Failed to destroy storage context."};
    return {true, {}, ""};
}

LogosShutdown StorageModuleImpl::aboutToUnload() {
    fprintf(stderr, "StorageModuleImpl::aboutToUnload called\n");

    if (!storageCtx) {
        return LogosShutdown::Synchronous;
    }

    // Separate budgets so a late nodeBusy release still leaves time to stop.
    const int busyTimeoutMs = 2000;
    const int stopTimeoutMs = 2000;

    int waitedMs = 0;

    // Wait until nodeBusy is false, then set it to true.
    // If compare_exchange_strong returns false, it means nodeBusy is already true,
    // so we wait for a short time before retrying.
    // If compare_exchange_strong returns true, it means nodeBusy was successfully set to true
    // so we can proceed with the shutdown.
    bool expected = false;
    while (!nodeBusy.compare_exchange_strong(expected, true)) {

        // If it takes too long, we cannot do anything,
        // eventually the process will be killed by the OS.
        if (waitedMs >= busyTimeoutMs) {
            fprintf(stderr, "StorageModuleImpl::aboutToUnload: node still busy, skipping destroy\n");
            return LogosShutdown::Synchronous;
        }

        // Reset the expected value to false to retry the compare_exchange_strong operation.
        expected = false;

        // Wait a bit before retrying.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        waitedMs += 50;
    }

    if (nodeRunning.load()) {
        SyncResult r = syncCallNoArg(storageCtx, storage_ctx_stop, stopTimeoutMs);
        if (!r.ok) {
            fprintf(stderr, "StorageModuleImpl::aboutToUnload: stop failed, skipping destroy: %s\n",
                    r.message.c_str());
            return LogosShutdown::Synchronous;
        }
    }

    destroyContext();

    return LogosShutdown::Synchronous;
}

// ---------------------------------------------------------------------------
// Info
// ---------------------------------------------------------------------------

bool StorageModuleImpl::isRunning() {
    return nodeRunning.load();
}

StdLogosResult StorageModuleImpl::libstorageVersion() {
    // The buffer is thread-local and belongs to libstorage: copy, never free.
    const char* v = storage_version();
    if (!v) return {false, {}, "Failed to get version."};
    return {true, std::string(v), ""};
}

std::string StorageModuleImpl::moduleVersion() {
    return STORAGE_MODULE_VERSION;
}

StdLogosResult StorageModuleImpl::dataDir() {
    auto r = syncCallNoArg(storageCtx, storage_ctx_repo, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::network() {
    auto r = syncCallNoArg(storageCtx, storage_ctx_network, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::peerId() {
    auto r = syncCallNoArg(storageCtx, storage_ctx_peer_id, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::spr() {
    auto r = syncCallNoArg(storageCtx, storage_ctx_spr, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::debug() {
    auto r = syncCallNoArg(storageCtx, storage_ctx_debug, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    try {
        return {true, json::parse(r.message), ""};
    } catch (...) {
        return {false, {}, "Failed to parse debug info."};
    }
}

LogosMap StorageModuleImpl::collectMetrics() {
    auto emptyMetrics = [] { return json{{"metrics", json::array()}}; };

    auto r = syncCallNoArg(storageCtx, storage_ctx_get_metrics, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return emptyMetrics();
    try {
        json parsed = json::parse(r.message);
        if (!parsed.is_object()) return emptyMetrics();
        auto metrics = parsed.find("metrics");
        if (metrics == parsed.end() || !metrics->is_array()) return emptyMetrics();
        return parsed;
    } catch (...) {
        return emptyMetrics();
    }
}

StdLogosResult StorageModuleImpl::updateLogLevel(const std::string& logLevel) {
    auto r = syncCallString(storageCtx, storage_ctx_log_level, logLevel, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    return {true, {}, ""};
}

// ---------------------------------------------------------------------------
// Connect
// ---------------------------------------------------------------------------

StdLogosResult StorageModuleImpl::connect(const std::string& peerId,
                                           const std::vector<std::string>& peerAddresses) {
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    // The wrapper encodes before it returns, so a borrowed view can be local.
    std::vector<const char*> addrs;
    addrs.reserve(peerAddresses.size());
    for (const auto& a : peerAddresses) addrs.push_back(a.c_str());
    StorageSeq_Str addrList{addrs.data(), addrs.size()};

    auto* ctx = new ConnectCtx(this);
    if (asyncSend(ctx, storage_ctx_connect, storageCtx, peerId.c_str(),
                  &addrList) != RET_OK) {
        return {false, {}, "Failed to send connect command."};
    }
    return {true, {}, ""};
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

StdLogosResult StorageModuleImpl::uploadInit(const std::string& filename,
                                              int64_t chunkSize, bool advertise) {
    auto r = syncCallString(storageCtx, storage_ctx_upload_init, filename, DEFAULT_TIMEOUT_MS,
                            static_cast<uint64_t>(chunkSize), advertise);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::uploadUrl(const std::string& filePath,
                                             int64_t chunkSize, bool advertise) {
    fprintf(stderr, "StorageModuleImpl::uploadUrl called with path=%s\n",
            filePath.c_str());
    if (!storageCtx || chunkSize <= 0)
        return {false, {}, "Invalid arguments."};

    std::error_code ec;
    if (!fs::exists(filePath, ec) || !fs::is_regular_file(filePath, ec)) {
        fprintf(stderr, "StorageModuleImpl::uploadUrl: file not found or not regular: %s\n",
                filePath.c_str());
        return {false, {}, "File not found: " + filePath};
    }

    int64_t fileSize = static_cast<int64_t>(fs::file_size(filePath, ec));

    auto ir = syncCallString(storageCtx, storage_ctx_upload_init, filePath, DEFAULT_TIMEOUT_MS,
                             static_cast<uint64_t>(chunkSize), advertise);
    if (!ir.ok)
        return {false, {}, ir.message};
    std::string sessionId = ir.message;

    Progress p;
    p.total = fileSize;
    uploads.track({this, sessionId}, p);

    auto* ctx = new UploadFileCtx(this, sessionId);
    if (asyncSend(ctx, storage_ctx_upload_file, storageCtx,
                  sessionId.c_str()) != RET_OK) {
        uploadCancel(sessionId);
        return {false, {}, "Failed to start file upload."};
    }
    return {true, sessionId, ""};
}

StdLogosResult StorageModuleImpl::uploadChunk(const std::string& sessionId,
                                               const std::string& chunk) {
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    auto* ctx = new UploadChunkCtx(this, sessionId, chunk);
    NimFfiBytes data;
    data.data = reinterpret_cast<uint8_t*>(ctx->chunk.data());
    data.len = ctx->chunk.size();
    if (asyncSend(ctx, storage_ctx_upload_chunk, storageCtx,
                  ctx->sessionId.c_str(), &data) != RET_OK) {
        return {false, {}, "Failed to send chunk."};
    }
    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::uploadFinalize(const std::string& sessionId) {
    auto r = syncCallString(storageCtx, storage_ctx_upload_finalize, sessionId, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::uploadCancel(const std::string& sessionId) {
    auto r = syncCallString(storageCtx, storage_ctx_upload_cancel, sessionId, DEFAULT_TIMEOUT_MS);
    uploads.forget({this, sessionId});
    if (!r.ok) return {false, {}, r.message};
    return {true, {}, ""};
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

std::string StorageModuleImpl::downloadChunksInternal(const std::string& cid,
                                                       const std::string& filepath,
                                                       bool local,
                                                       int64_t chunkSize, bool isPrivate, bool advertise) {
    if (!storageCtx || chunkSize <= 0) return {};

    // For file-mode download, fetch the manifest to determine total size so
    // that progress events can be throttled to one per percentage point.
    int64_t totalBytes = 0;
    if (!filepath.empty()) {
        auto mr = syncCallString(storageCtx, storage_ctx_download_manifest, cid,
                                 FETCH_MANIFEST_TIMEOUT_MS, isPrivate, advertise);
        if (mr.ok) {
            try {
                json manifest = json::parse(mr.message);
                if (manifest.contains("datasetSize") && !manifest["datasetSize"].is_null()) {
                    const auto& ds = manifest["datasetSize"];
                    if (ds.is_number_integer()) totalBytes = ds.get<int64_t>();
                    else if (ds.is_number()) totalBytes = static_cast<int64_t>(ds.get<double>());
                    else if (ds.is_string()) totalBytes = std::stoll(ds.get_ref<const std::string&>());
                }
            } catch (const std::exception& e) {
                fprintf(stderr,
                        "StorageModuleImpl::downloadChunksInternal: failed to parse manifest for %s: %s\n",
                        cid.c_str(), e.what());
            } catch (...) {
                fprintf(stderr,
                        "StorageModuleImpl::downloadChunksInternal: failed to parse manifest for %s (unknown error)\n",
                        cid.c_str());
            }
        }
        if (totalBytes == 0) {
            fprintf(stderr,
                    "StorageModuleImpl::downloadChunksInternal: failed to get "
                    "manifest for %s\n",
                    cid.c_str());
            return {};
        }
    }

    auto r = syncCallString(storageCtx, storage_ctx_download_init, cid,
                            FETCH_MANIFEST_TIMEOUT_MS, static_cast<uint64_t>(chunkSize),
                            local, isPrivate, advertise);
    if (!r.ok) return {};

    Progress p;
    p.total = totalBytes;
    p.asChunks = filepath.empty();
    downloads.track({this, cid}, p);

    // onDownloadDoneEvent reports the outcome, so the reply needs no context.
    if (asyncSend(nullptr, storage_ctx_download_stream, storageCtx, cid.c_str(),
                  static_cast<uint64_t>(chunkSize), filepath.c_str()) != RET_OK) {
        downloadCancel(cid);
        return {};
    }
    return cid;
}

StdLogosResult StorageModuleImpl::downloadToUrl(const std::string& cid,
                                                 const std::string& filePath,
                                                 bool local, int64_t chunkSize, bool isPrivate, bool advertise) {
    std::string sessionId = downloadChunksInternal(cid, filePath, local, chunkSize, isPrivate, advertise);
    if (sessionId.empty())
        return {false, {}, "Failed to start download."};
    return {true, sessionId, ""};
}

StdLogosResult StorageModuleImpl::downloadChunks(const std::string& cid, bool local,
                                                  int64_t chunkSize, bool isPrivate, bool advertise) {
    std::string sessionId = downloadChunksInternal(cid, "", local, chunkSize, isPrivate, advertise);
    if (sessionId.empty())
        return {false, {}, "Failed to start chunk download."};
    return {true, sessionId, ""};
}

StdLogosResult StorageModuleImpl::downloadCancel(const std::string& sessionId) {
    auto r = syncCallString(storageCtx, storage_ctx_download_cancel, sessionId, DEFAULT_TIMEOUT_MS);
    downloads.forget({this, sessionId});
    if (!r.ok) return {false, {}, r.message};
    return {true, {}, ""};
}

// ---------------------------------------------------------------------------
// Data management
// ---------------------------------------------------------------------------

StdLogosResult StorageModuleImpl::exists(const std::string& cid) {
    auto r = syncCallString(storageCtx, storage_ctx_exists, cid, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message == "true", ""};
}

StdLogosResult StorageModuleImpl::fetch(const std::string& cid, bool isPrivate, bool advertise) {
    auto r = syncCallString(storageCtx, storage_ctx_fetch, cid, FETCH_MANIFEST_TIMEOUT_MS, isPrivate, advertise);
    if (!r.ok) return {false, {}, r.message};
    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::getAdvertise(const std::string& cid) {
    auto r = syncCallString(storageCtx, storage_ctx_get_advertise, cid, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message == "true", ""};
}

StdLogosResult StorageModuleImpl::setAdvertise(const std::string& cid, bool advertise) {
    auto r = syncCallString(storageCtx, storage_ctx_set_advertise, cid, DEFAULT_TIMEOUT_MS, advertise);
    if (!r.ok) return {false, {}, r.message};
    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::remove(const std::string& cid) {
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    auto* ctx = new RemoveCtx(this, cid);
    if (asyncSend(ctx, storage_ctx_delete, storageCtx, ctx->cid.c_str()) !=
        RET_OK) {
        return {false, {}, "Failed to send remove command."};
    }
    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::space() {
    auto r = syncCallNoArg(storageCtx, storage_ctx_space, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    try {
        return {true, json::parse(r.message), ""};
    } catch (...) {
        return {false, {}, "Failed to parse space info."};
    }
}

StdLogosResult StorageModuleImpl::manifests() {
    auto r = syncCallNoArg(storageCtx, storage_ctx_list, DEFAULT_TIMEOUT_MS);
    if (!r.ok) return {false, {}, r.message};
    try {
        json raw = json::parse(r.message);
        if (!raw.is_array())
            return {false, {}, "Failed to parse manifests."};
        json list = json::array();
        for (const auto& item : raw) {
            json entry;
            if (item.contains("cid"))      entry["cid"]         = item["cid"];
            if (item.contains("manifest")) {
                const auto& m = item["manifest"];
                if (m.contains("treeCid"))     entry["treeCid"]     = m["treeCid"];
                if (m.contains("datasetSize")) entry["datasetSize"] = m["datasetSize"];
                if (m.contains("blockSize"))   entry["blockSize"]   = m["blockSize"];
                if (m.contains("filename"))    entry["filename"]    = m["filename"];
                if (m.contains("mimetype"))    entry["mimetype"]    = m["mimetype"];
            }
            list.push_back(entry);
        }
        return {true, list, ""};
    } catch (...) {
        return {false, {}, "Failed to parse manifests."};
    }
}

StdLogosResult StorageModuleImpl::downloadManifest(const std::string& cid, bool isPrivate, bool advertise) {
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    auto* ctx = new FetchManifestCtx(this, cid);
    if (asyncSend(ctx, storage_ctx_download_manifest, storageCtx, ctx->cid.c_str(),
                  isPrivate, advertise) != RET_OK) {
        return {false, {}, "Failed to send download manifest command."};
    }
    return {true, {}, ""};
}

// ---------------------------------------------------------------------------
// importFiles (headless helper)
// ---------------------------------------------------------------------------

void StorageModuleImpl::importFiles(const std::string& path) {
    fprintf(stderr, "StorageModuleImpl::importFiles from path=%s\n",
            path.c_str());
    std::error_code ec;
    if (!fs::is_directory(path, ec)) {
        fprintf(stderr,
                "StorageModuleImpl::importFiles: not a directory: %s\n",
                path.c_str());
        return;
    }
    for (const auto& entry : fs::directory_iterator(path, ec)) {
        if (!entry.is_regular_file()) continue;
        std::string fp = entry.path().string();
        fprintf(stderr, "StorageModuleImpl::importFiles: uploading %s\n",
                fp.c_str());
        StdLogosResult result = uploadUrl(fp, DEFAULT_CHUNK_SIZE, true);
        if (!result.success) {
            fprintf(stderr,
                    "StorageModuleImpl::importFiles: failed to start upload "
                    "for %s\n",
                    fp.c_str());
        } else {
            std::string sid = result.value.is_string()
                                  ? result.value.get<std::string>()
                                  : std::string();
            fprintf(stderr,
                    "StorageModuleImpl::importFiles: upload started, "
                    "session=%s\n",
                    sid.c_str());
        }
    }
}
