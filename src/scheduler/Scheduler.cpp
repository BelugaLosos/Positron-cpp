#include "Scheduler.hpp"
#include <pthread.h>

int stickThisThreadToCore(int core_id) {
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();    
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

Scheduler::Scheduler(int concurrncy_ratio) {
    _num_threads = thread::hardware_concurrency();

    cout << "[SCHEDULER] Starting schduler on " << _num_threads << " cores. Concurrency ratio " << concurrncy_ratio << ". Boost version " << BOOST_VERSION << endl;
    
    _contextes.reserve(_num_threads);
    _guards.reserve(_num_threads);
    _threads_pool.reserve(_num_threads);

    for (int i = 0; i < _num_threads / concurrncy_ratio; i++) {
        unique_ptr<io_context> ctx_ptr = make_unique<io_context>();
        worker_guard_t guard = make_work_guard(*ctx_ptr);
        _contextes.emplace_back(move(ctx_ptr));
        _guards.emplace_back(guard);

        cout << "[SCHEDULER] Constructing io ctx " << i << endl;
    }

    int ctx_overflow_counter = 0;
    int ctx_index = 0;

    for (int i = 0; i < _num_threads; i++) {
        if (ctx_overflow_counter >= concurrncy_ratio) {
            ctx_overflow_counter = 0;
            ctx_index++;
        }

        thread th = thread([ctx_index, i, this] {
            stickThisThreadToCore(i);
            _contextes.at(ctx_index)->run();
        });

        _threads_pool.emplace_back(move(th));

        cout << "[SCHEDULER] Starting thread " << i << " with io ctx " << ctx_index << endl;

        ctx_overflow_counter++;
    }
}

Scheduler::~Scheduler() {
    _guards.clear();
    cout << "[SCHEDULER] Stoping... " << endl;

    for (int i = 0; i < _threads_pool.size(); i++) {
        cout << "[SCHEDULER] Stoping thread " << i << endl;

        if (_threads_pool[i].joinable()) {
            _threads_pool[i].join();
            cout << "[SCHEDULER] Stopped thread " << i << endl;
        }
    }
}

io_context::executor_type Scheduler::schedule() {
    int idx = _schdule_counter.fetch_add(1, memory_order_relaxed) % _num_threads;
    return _contextes[idx]->get_executor();
}

void Scheduler::waitUntilSigterm() {
    io_context io_ctx {};
    signal_set signals(io_ctx, SIGTERM, SIGINT);

    cout << "[SCHEDULER] Waiting for SIGTERM/SIGINT signals and keep running" << endl;

    future<int> signal_future = signals.async_wait(use_future);

    thread io_thread([&io_ctx]() { io_ctx.run(); });

    int sig = signal_future.get();
    cout << "[SCHEDULER] Captured signal: " << sig << ". Stopping..." << endl;

    if (io_thread.joinable()) {
        io_thread.join();
    }

    for (int i = 0; i < _contextes.size(); i++) {
        cout << "stopping ctx " << i << endl;
        _contextes[i]->stop();
    }
}