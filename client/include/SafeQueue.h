#ifndef SAFEQUEUE_H
#define SAFEQUEUE_H

#include <queue>
#include <mutex>
#include <condition_variable>

template <typename T>
class SafeQueue {
public:
    SafeQueue() = default;
    ~SafeQueue() = default;

    // Enqueues a value into the queue
    void enqueue(T value) {
        std::lock_guard<std::mutex> lock(mtx); // Lock once
        queue.push(value);
        cv.notify_one(); // Notify waiting threads
    }

    // Dequeues a value from the queue (blocking if empty)
    T dequeue() {
        std::unique_lock<std::mutex> lock(mtx); // Lock once
        cv.wait(lock, [this] { return !queue.empty(); }); // Wait until not empty
        T value = queue.front();
        queue.pop();
        return value;
    }

    // Checks if the queue is empty
    bool isEmpty() {
        std::lock_guard<std::mutex> lock(mtx); // Lock for thread-safe access
        return queue.empty();
    }

private:
    std::queue<T> queue;
    std::mutex mtx;
    std::condition_variable cv;
};

#endif // SAFEQUEUE_H