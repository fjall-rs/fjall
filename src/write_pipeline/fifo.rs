// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::mem::MaybeUninit;

// Wrapper for loom-specific "with" API
#[cfg(not(feature = "__internal_loom"))]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(feature = "__internal_loom"))]
impl<T> UnsafeCell<T> {
    pub(crate) fn new(data: T) -> Self {
        Self(std::cell::UnsafeCell::new(data))
    }

    pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }

    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

#[cfg(not(feature = "__internal_loom"))]
use std::{
    sync::atomic::AtomicUsize,
    sync::atomic::Ordering::{Acquire, Relaxed, Release},
};

#[cfg(feature = "__internal_loom")]
use loom::{
    cell::UnsafeCell,
    sync::atomic::AtomicUsize,
    sync::atomic::Ordering::{Acquire, Relaxed, Release},
};

/// Slot in bounded queue
struct Slot<T> {
    /// Actual user payload
    data: UnsafeCell<MaybeUninit<T>>,

    seqno: AtomicUsize,
}

#[expect(unsafe_code)]
unsafe impl<T: Send> Sync for Queue<T> {}
#[expect(unsafe_code)]
unsafe impl<T: Send> Send for Queue<T> {}

/// A bounded, FIFO MPSC queue
///
/// Based on the Vyukov queue.
pub struct Queue<T> {
    /// Bounded list of slots
    slots: Box<[Slot<T>]>,

    /// Monotonic head index
    head: AtomicUsize,

    /// Monotonic tail index
    tail: AtomicUsize,

    // See https://www.snellman.net/blog/archive/2016-12-13-ring-buffers/
    // or https://stackoverflow.com/questions/10527581/why-must-a-ring-buffer-size-be-a-power-of-2
    mask: usize,
}

impl<T> Queue<T> {
    pub fn with_capacity(n: usize) -> Self {
        assert!(
            n >= 2 && n.is_power_of_two(),
            "capacity must be a power of two and >= 2"
        );

        Self {
            #[warn(clippy::uninit_vec)]
            slots: {
                let mut v = Vec::<Slot<T>>::with_capacity(n);

                for i in 0..n {
                    v.push(Slot {
                        data: UnsafeCell::new(MaybeUninit::uninit()),
                        seqno: AtomicUsize::new(i),
                    });
                }

                v.into_boxed_slice()
            },
            head: AtomicUsize::default(),
            tail: AtomicUsize::default(),
            mask: n - 1,
        }
    }

    #[inline(always)]
    fn get_index(&self, ticket: usize) -> usize {
        ticket & self.mask
    }

    pub fn try_push(&self, item: T) -> Option<()> {
        use std::cmp::Ordering;

        let mut tail = self.tail.load(Relaxed);

        loop {
            #[expect(clippy::indexing_slicing)]
            let slot = &self.slots[self.get_index(tail)];
            let seqno = slot.seqno.load(Acquire);

            // Vyukov's algorithm compares the sequence-number difference as a
            // signed value: a negative difference means the queue is full.
            let diff = seqno.wrapping_sub(tail) as isize;

            match diff.cmp(&0) {
                Ordering::Equal => {
                    match self.tail.compare_exchange_weak(
                        tail,
                        tail.wrapping_add(1),
                        Relaxed,
                        Relaxed,
                    ) {
                        Ok(_) => {
                            // SAFETY: We won the CAS, and are setting a value
                            #[expect(unsafe_code)]
                            unsafe {
                                slot.data.with_mut(|d| {
                                    *d = MaybeUninit::new(item);
                                });
                            }
                            slot.seqno.store(tail.wrapping_add(1), Release);
                            return Some(());
                        }
                        Err(actual_tail) => tail = actual_tail,
                    }
                }
                Ordering::Less => return None,
                Ordering::Greater => tail = self.tail.load(Relaxed),
            }
        }
    }

    // We have a specific invariant in fjall: there is only a single consumer
    pub fn try_pop(&self) -> Option<T> {
        use std::cmp::Ordering;

        let mut head = self.head.load(Relaxed);

        loop {
            #[expect(clippy::indexing_slicing)]
            let slot = &self.slots[self.get_index(head)];
            let seqno = slot.seqno.load(Acquire);

            let diff = seqno.wrapping_sub(head.wrapping_add(1)) as isize;

            match diff.cmp(&0) {
                Ordering::Equal => {
                    match self.head.compare_exchange_weak(
                        head,
                        head.wrapping_add(1),
                        Relaxed,
                        Relaxed,
                    ) {
                        Ok(_) => {
                            // SAFETY: We set published=true after pushing a value
                            // We set published=false after popping a value
                            #[expect(unsafe_code)]
                            let value = unsafe { slot.data.with(|d| d).read().assume_init() };

                            slot.seqno
                                .store(head.wrapping_add(self.mask).wrapping_add(1), Release);

                            return Some(value);
                        }
                        Err(actual_head) => head = actual_head,
                    }
                }
                Ordering::Less => return None,
                Ordering::Greater => head = self.head.load(Relaxed),
            }
        }
    }

    // We have a special invariant in fjall: we only have a single consumer
    pub fn peek(&self) -> Option<&T> {
        use std::cmp::Ordering;

        let head = self.head.load(Acquire);

        #[expect(clippy::indexing_slicing)]
        let slot = &self.slots[self.get_index(head)];
        let seqno = slot.seqno.load(Acquire);

        let diff = seqno.wrapping_sub(head + 1);

        match diff.cmp(&0) {
            Ordering::Equal => {
                // SAFETY: Uhhh
                #[expect(unsafe_code)]
                let ptr = {
                    let ptr = slot.data.with(|d| d).cast();
                    unsafe { &*ptr }
                };

                Some(ptr)
            }
            Ordering::Less | Ordering::Greater => None,
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        // At drop time, we have &mut self, so no concurrent access should exist.
        let head = self.head.load(Acquire);
        let tail = self.tail.load(Acquire);

        for idx in head..tail {
            #[expect(clippy::indexing_slicing)]
            let slot = &self.slots[self.get_index(idx)];

            // A slot is published for enqueue position `idx` when its sequence
            // number is advanced to the next position.
            if slot.seqno.load(Acquire) == idx.wrapping_add(1) {
                #[expect(unsafe_code)]
                unsafe {
                    std::ptr::drop_in_place((*slot.data.with_mut(|d| d)).as_mut_ptr());
                }
            }
        }
    }
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self::with_capacity(1_024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mpsc_queue_1() {
        let list = Queue::<usize>::with_capacity(4);

        assert!(list.try_push(2).is_some());
        assert!(list.try_push(4).is_some());
        assert!(list.try_push(8).is_some());
        assert!(list.try_push(12).is_some());
        assert!(list.try_push(1).is_none());

        assert_eq!(Some(2), list.try_pop());
        assert_eq!(Some(4), list.try_pop());
        assert_eq!(Some(8), list.try_pop());
        assert_eq!(Some(12), list.try_pop());
        assert_eq!(None, list.try_pop());
    }

    #[test]
    fn mpsc_queue_2() {
        let list = Queue::<usize>::with_capacity(4);
        assert!(list.try_push(2).is_some());
        assert!(list.try_push(4).is_some());
        assert!(list.try_push(8).is_some());
        assert!(list.try_push(12).is_some());
        assert!(list.try_push(1).is_none());

        assert_eq!(Some(2), list.try_pop());
        assert!(list.try_push(14).is_some());
        assert!(list.try_push(1).is_none());

        assert_eq!(Some(4), list.try_pop());
        assert!(list.try_push(17).is_some());
        assert!(list.try_push(1).is_none());
    }

    #[test]
    fn mpsc_queue_3() {
        let list = Queue::<usize>::with_capacity(4);
        assert_eq!(None, list.peek());

        assert!(list.try_push(2).is_some());
        assert_eq!(Some(&2), list.peek());
        assert_eq!(Some(&2), list.peek());
        assert_eq!(Some(&2), list.peek());

        assert!(list.try_push(4).is_some());
        assert_eq!(Some(&2), list.peek());
        assert_eq!(Some(&2), list.peek());
        assert_eq!(Some(&2), list.peek());
        assert_eq!(Some(2), list.try_pop());
        assert_eq!(Some(&4), list.peek());
        assert_eq!(Some(&4), list.peek());
        assert_eq!(Some(&4), list.peek());
        assert_eq!(Some(4), list.try_pop());
        assert_eq!(None, list.try_pop());

        assert_eq!(None, list.try_pop());
        assert_eq!(None, list.try_pop());
        assert_eq!(None, list.try_pop());
    }

    #[test]
    #[should_panic]
    fn mpsc_queue_with_capacity_zero_panics() {
        let _ = Queue::<u8>::with_capacity(0);
    }

    #[test]
    #[should_panic]
    fn mpsc_queue_with_capacity_non_power_of_two_panics() {
        let _ = Queue::<u8>::with_capacity(3);
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn mpsc_queue_drops_elements_on_drop() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        struct FreeTracker(Arc<AtomicUsize>);

        impl Drop for FreeTracker {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicUsize::new(0));
        {
            let q = Queue::with_capacity(4);
            q.try_push(FreeTracker(dropped.clone())).unwrap();
            q.try_push(FreeTracker(dropped.clone())).unwrap();
            q.try_push(FreeTracker(dropped.clone())).unwrap();
            // q drops here
        }
        assert_eq!(3, dropped.load(Ordering::SeqCst));
    }

    #[cfg(feature = "__internal_loom")]
    #[test]
    fn loom_two_producers_single_consumer() {
        loom::model(|| {
            use loom::{sync::Arc, thread};

            let queue = Arc::new(Queue::with_capacity(2));

            let producer_1 = {
                let queue = Arc::clone(&queue);
                thread::spawn(move || {
                    while queue.try_push(1).is_none() {
                        thread::yield_now();
                    }
                })
            };
            let producer_2 = {
                let queue = Arc::clone(&queue);
                thread::spawn(move || {
                    while queue.try_push(2).is_none() {
                        thread::yield_now();
                    }
                })
            };

            let mut received = Vec::new();

            while received.len() < 2 {
                if let Some(value) = queue.try_pop() {
                    received.push(value);
                } else {
                    thread::yield_now();
                }
            }

            producer_1.join().unwrap();
            producer_2.join().unwrap();

            received.sort_unstable();
            assert_eq!(received, [1, 2]);
            assert_eq!(queue.try_pop(), None);
        });
    }
}
