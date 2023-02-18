#include <cassert>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <time.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define N 100000
#define M 16

#define INVALID_WORKER_ID 0

enum class LockType { RLOCK = 0,
    WLOCK };

enum class LockStatus { LOCKED = 0,
    UNLOCKED };

// 长度为N的数组，初始值为1
std::vector<size_t> S(N, 1);

struct RequestQueue {
    std::mutex mtx;
    std::condition_variable cond;
    std::unordered_set<size_t> rlock_items;
    std::unordered_set<size_t> waiting_items;
    size_t wlock_item = INVALID_WORKER_ID;

    bool TryLock(size_t worker_id, LockType type)
    {

        if (type == LockType::RLOCK) {
            // 如果不存在其他Worker加了写锁，则请求读锁成功
            return wlock_item == INVALID_WORKER_ID;
        } else {
            // 存在其他Worker加了写锁，则请求写锁失败，返回false
            if (wlock_item != 0) {
                return false;
            }

            // 不存在其他Worker加了读锁，则请求写锁成功，返回false
            if (rlock_items.empty()) {
                return true;
            }

            // 可以从读锁提升为写锁， 返回false
            if (rlock_items.size() == 1 && rlock_items.count(worker_id) != 0) {
                return true;
            }

            return false;
        }
    }
};

//! ShardRequestTable中维护多个锁对象和哈希表，以减少锁的冲突
class ShardRequestTable {
public:
    static constexpr size_t BUCKET_NUM = 16;

    ShardRequestTable()
    {
        mtxs.reserve(BUCKET_NUM);
        for (size_t i = 0; i < BUCKET_NUM; ++i) {
            auto mtx = std::make_unique<std::mutex>();
            mtxs.emplace_back(std::move(mtx));
        }
        request_tables_.resize(BUCKET_NUM);
    }

    ShardRequestTable(const ShardRequestTable&) = delete;
    ShardRequestTable& operator=(const ShardRequestTable&) = delete;

    RequestQueue& GetRequestQueueLocked(size_t index)
    {
        size_t bucket_num = index % BUCKET_NUM;
        return request_tables_[bucket_num][index];
    }

    void EraseRequestQueueLocked(size_t index)
    {
        size_t bucket_num = index % BUCKET_NUM;
        request_tables_[bucket_num].erase(index);
    }

    std::unique_lock<std::mutex> AcquireShardLock(size_t index)
    {
        size_t bucket_num = index % BUCKET_NUM;
        return std::unique_lock<std::mutex>(*mtxs[bucket_num]);
    }

private:
    std::vector<std::unique_ptr<std::mutex>> mtxs;
    //! worder id --> RequestQueue
    std::vector<std::unordered_map<size_t, RequestQueue>> request_tables_;
};

class LockTable {
public:
    bool RLock(size_t worker_id, size_t index)
    {
        // 请求分段锁
        auto lock = request_table_.AcquireShardLock(index);
        auto& request_queue = request_table_.GetRequestQueueLocked(index);
        std::unique_lock<std::mutex> qlock(request_queue.mtx);
        lock.unlock();

        // 存在其他Worker对数据加了写锁，则加锁失败
        if (!request_queue.TryLock(worker_id, LockType::RLOCK)) {
            // 将当前Worker放入等待队列
            request_queue.waiting_items.insert(worker_id);
            bool timeout = !request_queue.cond.wait_for(qlock, std::chrono::milliseconds(300), [&]() {
                return request_queue.TryLock(worker_id, LockType::RLOCK);
            });
            request_queue.waiting_items.erase(worker_id);
            // 等待超时，有可能是因为发生死锁，返回false表明加锁失败
            if (timeout) {
                return false;
            }
        }
        request_queue.rlock_items.insert(worker_id);
        return true;
    }

    bool WLock(size_t worker_id, size_t index)
    {
        auto lock = request_table_.AcquireShardLock(index);
        auto& request_queue = request_table_.GetRequestQueueLocked(index);
        std::unique_lock<std::mutex> qlock(request_queue.mtx);
        lock.unlock();

        if (!request_queue.TryLock(worker_id, LockType::WLOCK)) {
            request_queue.waiting_items.insert(worker_id);
            bool timeout = !request_queue.cond.wait_for(qlock, std::chrono::milliseconds(300), [&]() {
                return request_queue.TryLock(worker_id, LockType::WLOCK);
            });

            request_queue.waiting_items.erase(worker_id);
            if (timeout) {
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
        auto lock = request_table_.AcquireShardLock(index);
        auto& request_queue = request_table_.GetRequestQueueLocked(index);
        std::unique_lock<std::mutex> qlock(request_queue.mtx);

        assert(request_queue.rlock_items.count(worker_id) != 0);
        request_queue.rlock_items.erase(worker_id);

        size_t rlock_items_size = request_queue.rlock_items.size();
        bool has_waiting_item = !request_queue.waiting_items.empty();
        if (rlock_items_size == 0 && !has_waiting_item && request_queue.wlock_item == INVALID_WORKER_ID) {
            qlock.unlock();
            request_table_.EraseRequestQueueLocked(index);
            return;
        }

        lock.unlock();

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
        auto lock = request_table_.AcquireShardLock(index);
        auto& request_queue = request_table_.GetRequestQueueLocked(index);
        std::unique_lock<std::mutex> qlock(request_queue.mtx);

        assert(request_queue.wlock_item == worker_id);
        request_queue.wlock_item = INVALID_WORKER_ID;

        bool has_waiting_item = !request_queue.waiting_items.empty();
        if (!has_waiting_item) {
            // 如果之前从读锁提升为写锁，那么rlock_items的size为1
            if (request_queue.rlock_items.empty()) {
                qlock.unlock();
                request_table_.EraseRequestQueueLocked(index);
            }
            return;
        }

        lock.unlock();

        request_queue.cond.notify_all();
    }

private:
    ShardRequestTable request_table_;
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
            // 0 <= i, j < N
            std::uniform_int_distribution<int> u(0, N - 1);
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
        });
    }

    void ReleaseRLocks(int i, size_t count)
    {
        i = (i + N - 1) % N;
        for (size_t k = 0; k < count; ++k) {
            assert(i < N && i >= 0);
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
    LockTable lock_table;
    std::vector<std::unique_ptr<Worker>> workers;

    // 创建16个Worker
    for (size_t i = 1; i <= M; ++i) {
        auto worker = std::make_unique<Worker>(i, lock_table);
        worker->Run();
        workers.push_back(std::move(worker));
    }

    return 0;
}