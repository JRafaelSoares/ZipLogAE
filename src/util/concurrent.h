#pragma once

#include <atomic>
#include <list>
#include <type_traits>

namespace zip::util {

/** Forward declaration of the bag reader. */
template <typename T>
class bag_reader;

/**
 * A single producer-multiple consumer concurrent bag.
 *
 * It is easy to make it lockless by never removing
 * elements.
 */
template <typename T>
class bag {

public:

    /**
     * Add an element to the bag.
     *
     * @param element the element to add
     */
    inline void add(T element) {
        // add to the list and increment the counter
        list_.emplace_back(std::move(element));
        num_elements_.fetch_add(1, std::memory_order_release);
    }

private:

    /** Make reader friend of this class. */
    friend bag_reader<T>;

    /** list to store the elements */
    std::list<T> list_;

    /** counter for number of elements */
    std::atomic<unsigned long> num_elements_ = 0;

};

/**
 * Used to access elements in the bag.
 */
template <typename T>
class bag_reader {

public:

    /** Initialise a bag reader from a bag. */
    bag_reader(bag<T>& bag): list_(bag.list_), num_elements_(bag.num_elements_) {}

    /**
     * Get a new element from the bag.
     *
     * @return pointer to the element
     */
    inline T* get() {
        // check if a new element has been added and
        // iterate to that element and return it
        if (num_polled_ < num_elements_.load(std::memory_order_acquire)) {
            if (num_polled_++ == 0) {
                iterator_ = list_.begin();
            } else {
                iterator_++;
            }
            return &*iterator_;
        }
        return nullptr;
    }

private:

    /** reference to the list */
    std::list<T>& list_;

    /** reference to the counter */
    std::atomic<unsigned long>& num_elements_;

    /** number of elements polled from the bag */
    unsigned long num_polled_ = 0;

    /** iterator the last accessed element */
    std::remove_reference_t<decltype(list_)>::iterator iterator_;

};

/**
 * A single producer-single consumer concurrent queue.
 *
 * We make it lockless by using `std::list::iterator` semantics
 * to always keep one element in the queue to keep the iterators
 * alive.
 */
template <typename T>
class queue {

public:

    /**
     * Enqueue an element.
     *
     * @param element the element to enqueue
     */
    inline void enqueue(T element) {
        // add the element and increment the counter
        queue_.emplace_back(std::move(element));
        num_enqueued_.fetch_add(1, std::memory_order_release);
    }

    /**
     * Try to dequeue an element from the queue.
     *
     * @return a pointer to the element
     */
    inline T* dequeue() {
        // check if an element is available, and obtain
        // it from the queue
        if (num_polled_ < num_enqueued_.load(std::memory_order_acquire)) {
            if (num_polled_++ == 0) {
                iterator_ = queue_.begin();
            } else {
                iterator_ = queue_.erase(iterator_);
            }
            return &*iterator_;
        }
        return nullptr;
    }

private:

    /** list to store the elements */
    std::list<T> queue_;

    /** counter for number of elements added */
    std::atomic<unsigned long> num_enqueued_ = 0;

    /** number of elements polled from the queue */
    unsigned long num_polled_ = 0;

    /** iterator to the last accessed element */
    decltype(queue_)::iterator iterator_;

};



} // namespace zip::util
