#include <vector>
#include <memory>
#include <atomic>
#include "RawCtxPool.hpp"
using namespace std;
using namespace boost::asio;

class Scheduler {
private:
    unique_ptr<RawCtxPool> _ctx_pool;
    atomic<size_t> _schdule_counter {0};

public:
    Scheduler(size_t concurrncy_ratio);
    ~Scheduler();
    io_context::executor_type schedule();
    void waitUntilSigterm();
};
