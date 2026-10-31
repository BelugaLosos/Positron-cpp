#include <iostream>
#include <vector>
#include <memory>
#include <boost/asio.hpp>

using namespace std;
using namespace boost::asio;

class RawCtxPool {
private:
    using worker_guard_t = executor_work_guard<io_context::executor_type>;

    vector<shared_ptr<io_context>> _contextes;
    vector<worker_guard_t> _guards;
    vector<thread> _threads_pool;

    size_t _num_threads;

public:
    RawCtxPool(size_t concurrncy_ratio);
    ~RawCtxPool();
    shared_ptr<io_context> getContext(size_t idx);
    size_t getNumThreads();
    void waitForTermination();
};