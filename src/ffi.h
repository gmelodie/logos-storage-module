#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

#include <libstorage.h>

// Each storage_ctx_* reply fires once: on this thread when the send fails, else on the libstorage thread.

// A reply value belongs to the binding and dies with the callback, so copy it.
static std::string fromFfi(const char* s) {
    return s ? std::string(s) : std::string();
}

static std::string replyText(int ret, const char* const* reply, const char* errMsg) {
    if (ret == RET_OK) return reply ? fromFfi(*reply) : std::string();
    return errMsg ? std::string(errMsg) : std::string();
}

struct AsyncCallbackBase {
    virtual void handleResponse(int ret, const char* msg, size_t len) = 0;
    virtual ~AsyncCallbackBase() = default;
};

// Set by asyncSend while it is inside the wrapper; only an inline reply sees it.
static thread_local int* inlineReply = nullptr;

static void asyncCallback(int ret, const char* const* reply, const char* errMsg, void* userData) {
    if (inlineReply) *inlineReply = ret;
    if (!userData) return;
    auto* base = static_cast<AsyncCallbackBase*>(userData);
    std::string text = replyText(ret, reply, errMsg);
    base->handleResponse(ret, text.data(), text.size());
    delete base;
}

// Non-zero means the reply already fired and freed ctx. The wrapper itself returns 0 even then.
template <typename Fn, typename... Args>
static int asyncSend(AsyncCallbackBase* ctx, Fn fn, Args... args) {
    int replied = RET_OK;
    int* outer = inlineReply;
    inlineReply = &replied;
    int ret = fn(args..., asyncCallback, ctx);
    inlineReply = outer;
    return ret != RET_OK ? ret : replied;
}

struct SyncCtx {
    std::mutex mtx;
    std::condition_variable ready;
    int resultCode = -1;
    std::string resultMsg;
    bool received = false;
    // Set when the caller gives up first: the callback then owns the deletion.
    std::atomic<bool> abandoned{false};
    // Only init() reads this: the constructor delivers it by callback.
    StorageCtx* created = nullptr;

    SyncCtx() = default;
    SyncCtx(const SyncCtx&) = delete;
    SyncCtx& operator=(const SyncCtx&) = delete;
};

// Returns true when the caller timed out first, so this reply owns what it carries.
static bool signalSync(SyncCtx* ctx, int ret, const std::string& msg, StorageCtx* created) {
    bool abandoned;
    {
        std::unique_lock<std::mutex> lock(ctx->mtx);
        ctx->resultCode = ret;
        ctx->resultMsg = msg;
        ctx->created = created;
        ctx->received = true;
        ctx->ready.notify_all();
        // Read abandoned under the lock the timeout path also takes.
        abandoned = ctx->abandoned.load();
    }
    if (abandoned) {
        delete ctx;
    }
    return abandoned;
}

static void syncCallback(int ret, const char* const* reply, const char* errMsg, void* userData) {
    if (!userData) return;
    auto* ctx = static_cast<SyncCtx*>(userData);
    signalSync(ctx, ret, replyText(ret, reply, errMsg), nullptr);
}

static void syncCreateCallback(int ret, StorageCtx* created, const char* errMsg, void* userData) {
    if (!userData) return;
    auto* ctx = static_cast<SyncCtx*>(userData);
    std::string msg = (ret == RET_OK || !errMsg) ? std::string() : std::string(errMsg);

    if (signalSync(ctx, ret, msg, created) && created) {
        // init() gave up on this node; a destroy on the dispatch thread would wait on itself.
        std::thread(storage_ctx_destroy, created).detach();
    }
}

struct SyncResult {
    bool ok = false;
    std::string message;
    StorageCtx* created = nullptr;
};

static SyncResult waitSync(SyncCtx* ctx, int timeoutMs) {
    SyncResult r;
    bool shouldDelete;
    {
        std::unique_lock<std::mutex> lock(ctx->mtx);
        ctx->ready.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                         [ctx] { return ctx->received; });
        r.ok = ctx->received && ctx->resultCode == RET_OK;
        r.message = ctx->resultMsg;
        r.created = ctx->created;
        shouldDelete = ctx->received;
        if (!shouldDelete) {
            ctx->abandoned.store(true);
        }
    }
    if (shouldDelete) {
        delete ctx;
    }
    return r;
}

// A failed send has already fired the reply, so waitSync returns at once.
template <typename Function>
static SyncResult syncCallNoArg(StorageCtx* ctx, Function fn, int timeoutMs) {
    if (!ctx) return {false, "Storage context not initialized."};
    auto* sctx = new SyncCtx();
    fn(ctx, syncCallback, sctx);
    return waitSync(sctx, timeoutMs);
}

template <typename Function, typename... Args>
static SyncResult syncCallString(StorageCtx* ctx, Function fn,
                                 const std::string& arg, int timeoutMs, Args... args) {
    if (!ctx) return {false, "Storage context not initialized."};
    auto* sctx = new SyncCtx();
    fn(ctx, arg.c_str(), args..., syncCallback, sctx);
    return waitSync(sctx, timeoutMs);
}
