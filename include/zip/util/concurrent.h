#pragma once

#include <atomic>
#include <list>
#include <optional>
#include <concepts>

#include <zip/util/util.h>

namespace zip::util {

/**
 * A single producer-single consumer concurrent queue.
 *
 * We make it lockless by using `std::list::iterator` semantics
 * to always keep one element in the queue to keep the iterators
 * alive.
 */
template <std::movable T>
class spsc_queue {

public:

    /**
     * Enqueue an element.
     *
     * @param element the element to enqueue
     */
    inline void enqueue(T&& element) {
        // add the element and increment the counter
        queue_.emplace_back(std::move(element));
        num_enqueued_.fetch_add(1, std::memory_order_seq_cst);
    }

    /**
     * Try to dequeue an element from the queue.
     *
     * @return a pointer to the element
     */
    inline std::optional<T> dequeue() {
        // check if an element is available, and obtain
        // it from the queue
        if (num_polled_ < num_enqueued_.load(std::memory_order_seq_cst)) {
            if (num_polled_++ == 0) {
                iterator_ = queue_.begin();
            } else {
                iterator_ = queue_.erase(iterator_);
            }
            return std::move(*iterator_);
        }
        return std::nullopt;
    }

private:

    /** list to store the elements */
    std::list<T> queue_;

    /** counter for number of elements added */
    std::atomic<uint64_t> num_enqueued_ = 0;

    /** number of elements polled from the queue */
    uint64_t num_polled_ = 0;

    /** iterator to the last accessed element */
    decltype(queue_)::iterator iterator_;

};

/**
 * A multiple producer-single consumer concurrent queue.
 *
 * We make it lockless by using `std::list::iterator` semantics
 * to always keep one element in the queue to keep the iterators
 * alive.
 */
template <typename T>
class mpsc_queue {

public:

    /**
     * Enqueue an element.
     *
     * @param element the element to enqueue
     */
    inline void enqueue(T&& element) {
        // grab the tail of the queue
        auto tail = tail_.fetch_add(1, std::memory_order_relaxed);

        // wait until it's our turn to append to the list
        while (num_enqueued_.load(std::memory_order_acquire) != tail) zip::util::relax();

        // add the element and increment the counter
        queue_.emplace_back(element);
        num_enqueued_.store(tail + 1, std::memory_order_release);
    }

    /**
     * Try to dequeue an element from the queue.
     *
     * @return a pointer to the element
     */
    inline std::optional<T> dequeue() {
        // check if an element is available, and obtain
        // it from the queue
        if (num_polled_ < num_enqueued_.load(std::memory_order_acquire)) {
            if (num_polled_++ == 0) {
                iterator_ = queue_.begin();
            } else {
                iterator_ = queue_.erase(iterator_);
            }
            return std::move(*iterator_);
        }
        return {};
    }

private:

    /** list to store the elements */
    std::list<T> queue_;

    /** counter to track the tail of insertion */
    std::atomic<uint64_t> tail_ = 0;

    /** counter for number of elements added */
    std::atomic<uint64_t> num_enqueued_ = 0;

    /** number of elements polled from the queue */
    uint64_t num_polled_ = 0;

    /** iterator to the last accessed element */
    decltype(queue_)::iterator iterator_;

};

} // namespace zip::util
