use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{cell::UnsafeCell, sync::atomic::AtomicUsize};

/// Slot in bounded queue
struct Slot<T> {
    /// Actual user payload
    data: UnsafeCell<MaybeUninit<T>>,

    /// Avoid race condition where tail is incremented but not yet written to slot
    published: AtomicBool,
}

unsafe impl<T> Sync for Queue<T> {}
unsafe impl<T> Send for Queue<T> {}

/// A bounded, FIFO MPSC queue
///
/// Consumers must be synchronized externally for performance reasons.
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
            n > 0 && n.is_power_of_two(),
            "capacity must be a power of two and > 0"
        );

        Self {
            #[warn(clippy::uninit_vec)]
            slots: {
                let mut v = Vec::<Slot<T>>::with_capacity(n);

                for _ in 0..n {
                    v.push(Slot {
                        data: UnsafeCell::new(MaybeUninit::uninit()),
                        published: AtomicBool::default(),
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
        let mut tail = self.tail.load(Ordering::Acquire);

        loop {
            let head = self.head.load(Ordering::Acquire);

            // If the gap between head and tail is greater or equal to our capacity...
            //
            // Tail is never < head
            if (tail - head) >= self.slots.len() {
                // We are full
                return None;
            }

            match self.tail.compare_exchange_weak(
                tail,
                tail + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // We got the slot
                    let idx = self.get_index(tail);
                    let slot = &self.slots[idx];

                    // SAFETY: We won the CAS, and are setting a value
                    unsafe {
                        *slot.data.get() = MaybeUninit::new(item);
                    }

                    slot.published.store(true, Ordering::Release);

                    return Some(());
                }
                Err(newer_tail) => {
                    // Spin with new tail
                    tail = newer_tail;
                }
            }
        }
    }

    // We have a specific invariant in fjall: there is only a single consumer
    pub fn try_pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Acquire);

        let idx = self.get_index(head);
        let slot = &self.slots[idx];

        if !slot.published.load(Ordering::Acquire) {
            return None;
        }

        // SAFETY: We set published=true after pushing a value
        // We set published=false after popping a value
        let value = unsafe { self.slots[idx].data.get().read().assume_init() };

        slot.published.store(false, Ordering::Release);

        // There is only a single consumer, so this is fine
        self.head.store(head + 1, Ordering::Release);

        Some(value)
    }

    pub fn peek(&self) -> Option<&T> {
        let head = self.head.load(Ordering::Acquire);

        let idx = self.get_index(head);
        let slot = &self.slots[idx];

        if !slot.published.load(Ordering::Acquire) {
            return None;
        }

        // SAFETY: Uhhh
        let ptr = slot.data.get().cast();
        let ptr = unsafe { &*(ptr as *const T) };
        Some(ptr)
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        // At drop time, we have &mut self, so no concurrent access should exist.
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        let mut ticket = head;

        while ticket < tail {
            let idx = self.get_index(ticket);
            let slot = &self.slots[idx];

            if slot.published.load(Ordering::Acquire) {
                unsafe {
                    std::ptr::drop_in_place((*slot.data.get()).as_mut_ptr());
                }
            }

            ticket += 1;
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
    fn with_capacity_zero_panics() {
        let _ = Queue::<u8>::with_capacity(0);
    }

    #[test]
    #[should_panic]
    fn with_capacity_non_power_of_two_panics() {
        let _ = Queue::<u8>::with_capacity(3);
    }

    #[test]
    fn drops_elements_on_drop() {
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
}
