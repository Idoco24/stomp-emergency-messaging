#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>

template <typename T>
class SafeQueue {
public:
    SafeQueue() : queue(), mtx(), cv() {} // Explicitly initialize members
    ~SafeQueue() = default;

    void enqueue(T value) {
        std::lock_guard<std::mutex> lock(mtx);
        queue.push(value);
        cv.notify_one();
    }

    T dequeue() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this] { return !queue.empty(); });
        T value = queue.front();
        queue.pop();
        return value;
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mtx);
        return queue.empty();
    }

private:
    mutable std::mutex mtx;
    std::queue<T> queue;
    std::condition_variable cv;
};