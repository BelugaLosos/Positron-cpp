#include <iostream>
#include <vector>
#include <memory>
#include <boost/asio.hpp>
#include <atomic>
#include <thread>
#include <future>
#include <pthread.h>
using namespace std;
using namespace boost::asio;

class Scheduler {
private:
    using worker_guard_t = executor_work_guard<io_context::executor_type>;

    vector<unique_ptr<io_context>> _contextes;
    vector<worker_guard_t> _guards;
    vector<thread> _threads_pool;
    atomic<int> _schdule_counter {0};
    int _num_threads;

public:
    Scheduler(int concurrncy_ratio);
    ~Scheduler();
    io_context::executor_type schedule();
    void waitUntilSigterm();
};
