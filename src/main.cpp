#include <iostream>
#include <memory>
#include <chrono>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include "scheduler/Scheduler.hpp"
using namespace std;

boost::asio::awaitable<void> backgroundTask() {
    auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor);

    int counter = 0;
    while (true) {
        timer.expires_after(std::chrono::seconds(2));
        
        co_await timer.async_wait(boost::asio::use_awaitable);

        counter++;
        cout << "[COROUTINE] background dummy task " << counter << " times executed" << endl;
    }
}

int main() {
    shared_ptr<Scheduler> scheduler = make_shared<Scheduler>(1);
    boost::asio::co_spawn(scheduler->schedule(), backgroundTask(), boost::asio::detached);
    scheduler->waitUntilSigterm();
    return 0;
}
