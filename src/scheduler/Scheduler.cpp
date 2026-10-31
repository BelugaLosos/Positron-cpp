#include "Scheduler.hpp"

Scheduler::Scheduler(size_t concurrncy_ratio) {
    _ctx_pool = make_unique<RawCtxPool>(concurrncy_ratio);
}

Scheduler::~Scheduler() = default;

io_context::executor_type Scheduler::schedule() {
    size_t idx = _schdule_counter.fetch_add(1, memory_order_relaxed) % _ctx_pool->getNumThreads();
    return _ctx_pool->getContext(idx)->get_executor();
}

void Scheduler::waitUntilSigterm() {
    _ctx_pool->waitForTermination();
}