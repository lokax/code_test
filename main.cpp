#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <time.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define N 100000

enum class LockType { RLOCK = 0,
    WLOCK };

enum class LockStatus { LOCKED = 0,
    UNLOCKED };

std::vector<size_t> S(N, 1);

/*
struct Request {
    size_t worker_id;
    LockType type;
    LockStatus status;

    Request(size_t worker_id, LockType type, LockStatus status)
        : worker_id(worker_id)
        , type(type)
        , status(status)
    {
    }
};
*/

struct RequestQueue {
    std::mutex mtx;
    std::condition_variable cond;
    std::unordered_set<size_t> rlock_items;
    std::unordered_set<size_t> waiting_items;
    size_t wlock_item = 0; // 0为无效值

    bool TryLock(size_t worker_id, LockType type)
    {

        if (type == LockType::RLOCK) {
            return wlock_item == 0;
        } else {
            if (wlock_item != 0) {
                return false;
            }

            if (rlock_items.empty()) {
                return true;
            }

            // 可以进行锁提升
            if (rlock_items.size() == 1 && rlock_items.count(worker_id) != 0) {
                return true;
            }

            return false;
        }
    }
};

class LockTable {
public:
    bool RLock(size_t worker_id, size_t index)
    {
        std::unique_lock<std::mutex> table_latch(latch_);
        auto& request_queue = request_table_[index];
        std::unique_lock<std::mutex> qlock(request_queue.mtx);
        table_latch.unlock();

        // 已经存在其他worker对数据加了写锁
        if (!request_queue.TryLock(worker_id, LockType::RLOCK)) {
            // 将当前worker放入等待队列
            request_queue.waiting_items.insert(worker_id);
            bool timeout = !request_queue.cond.wait_for(qlock, std::chrono::milliseconds(300), [&]() {
                return request_queue.TryLock(worker_id, LockType::RLOCK);
            });
            request_queue.waiting_items.erase(worker_id);
            if (timeout) {
                std::cout << "RLOCK超时\n";
                return false;
            }
        }
        std::cout << "RLock: INSERT " << worker_id << " index " << index << "\n";
        request_queue.rlock_items.insert(worker_id);
        return true;
    }

    bool WLock(size_t worker_id, size_t index)
    {
        std::unique_lock<std::mutex> table_latch(latch_);
        auto& request_queue = request_table_[index];
        std::unique_lock<std::mutex> qlock(request_queue.mtx);
        table_latch.unlock();

        std::cout << "WLock: worker_id " << worker_id << " index " << index << "\n";
        if (!request_queue.TryLock(worker_id, LockType::WLOCK)) {
            request_queue.waiting_items.insert(worker_id);
            bool timeout = !request_queue.cond.wait_for(qlock, std::chrono::milliseconds(300), [&]() {
                return request_queue.TryLock(worker_id, LockType::WLOCK);
            });
            request_queue.waiting_items.erase(worker_id);
            if (timeout) {
                std::cout << "RLOCK 超时\n";
                return false;
            }
        }

        /*
        if (request_queue.rlock_items.size() != 0) {
            assert(request_queue.rlock_items.count(worker_id) != 0);
            request_queue.rlock_items.erase(worker_id);
        }
        */

        request_queue.wlock_item = worker_id;
        return true;
    }

    void RUnlock(size_t worker_id, size_t index)
    {
        std::unique_lock<std::mutex> table_latch(latch_);
        auto& request_queue = request_table_[index];
        std::unique_lock<std::mutex> qlock(request_queue.mtx);

        std::cout << "RUnlock: ERASE " << worker_id << " index " << index << "\n";
        assert(request_queue.rlock_items.count(worker_id) != 0);
        request_queue.rlock_items.erase(worker_id);

        size_t rlock_items_size = request_queue.rlock_items.size();
        bool has_waiting_item = !request_queue.waiting_items.empty();
        if (rlock_items_size == 0 && !has_waiting_item && request_queue.wlock_item == 0) {
            // 从哈希表中删除请求队列
            request_table_.erase(index);
            return;
        }

        table_latch.unlock();

        if (has_waiting_item) {
            // 唤醒其中一个请求写锁的Worker
            if (rlock_items_size == 0) {
                request_queue.cond.notify_one();
            } else if (rlock_items_size == 1) {
                // 其中一个Worker有机会将读锁提升为写锁，唤醒所有Worker
                size_t rlock_worker_id = *request_queue.rlock_items.begin();
                if (request_queue.waiting_items.count(rlock_worker_id) != 0) {
                    request_queue.cond.notify_all();
                }
            }
        }
    }

    void WUnlock(size_t worker_id, size_t index)
    {
        std::unique_lock<std::mutex> table_latch(latch_);
        auto& request_queue = request_table_[index];
        std::unique_lock<std::mutex> qlock(request_queue.mtx);

        request_queue.wlock_item = 0;

        bool has_waiting_item = !request_queue.waiting_items.empty();
        if (!has_waiting_item) {
            if (request_queue.rlock_items.empty()) {
                request_table_.erase(index);
            }
            return;
        }

        table_latch.unlock();

        request_queue.cond.notify_all();
    }

private:
    std::mutex latch_;
    // worder id --> RequestQueue
    std::unordered_map<size_t, RequestQueue> request_table_;
};

struct Worker {
    Worker(size_t worker_id, LockTable& table)
        : worker_id(worker_id)
        , lock_table(table)
    {
    }

    ~Worker()
    {
        if (thread.joinable()) {
            thread.join();
        }
    }

    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    void Run()
    {

        thread = std::thread([&]() {
            std::default_random_engine engin;
            engin.seed(time(nullptr));
            std::uniform_int_distribution<int> u(0, N);
            for (size_t idx = 0; idx < 10000; ++idx) {
                int i = u(engin);
                int j = u(engin);

                size_t sum = 0;
                bool failed = false;
                for (size_t k = 0; k < 3; ++k) {
                    // 加锁失败，释放之前的锁
                    if (!lock_table.RLock(worker_id, i)) {
                        ReleaseRLocks(i, k);
                        failed = true;
                        break;
                    }
                    sum += S[i];
                    i = (i + 1) % N;
                }

                if (failed) {
                    continue;
                }

                if (!lock_table.WLock(worker_id, j)) {
                    continue;
                }

                S[j] = sum;
                lock_table.WUnlock(worker_id, j);
                ReleaseRLocks(i, 3);
            }
            std::cout << "Worker: " << worker_id << "完成\n";
        });
    }

    void ReleaseRLocks(int i, size_t count)
    {
        i = (i - 1 + N) % N;
        for (size_t k = 0; k < count; ++k) {
            lock_table.RUnlock(worker_id, i);
            i = (i + N - 1) % N;
        }
    }

    size_t worker_id;
    std::thread thread;
    LockTable& lock_table;
};

int main()
{
    size_t M = 0;
    std::cin >> M;

    if (M < 2) {
        M = 2;
    }

    srand((unsigned)time(NULL));

    LockTable global_lock_table;
    std::vector<std::unique_ptr<Worker>> workers;

    for (size_t i = 1; i <= M; ++i) {
        auto worker = std::make_unique<Worker>(i, global_lock_table);
        worker->Run();
        workers.push_back(std::move(worker));
    }

    return 0;
}