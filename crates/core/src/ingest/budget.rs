//! Byte budget for file data held by ingest (memory budget spec).
//!
//! A worker reserves a file's size before reading it. The [`Permit`] it
//! gets back releases those bytes when dropped, wherever that happens: in
//! the worker, or in storage after the units are appended. Requests are
//! granted first come, first served; one larger than the whole budget is
//! granted when nothing else is held, so peak held bytes are at most the
//! larger of the limit and the largest request (plus re-read growth).

use std::collections::BTreeSet;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

/// How often a waiting request re-checks its `stop` condition.
const POLL: Duration = Duration::from_millis(50);

/// The budget used when physical memory cannot be read: 4 GiB.
const FALLBACK_LIMIT: u64 = 4 << 30;

pub(crate) struct Budget {
    limit: u64,
    state: Mutex<State>,
    cv: Condvar,
}

#[derive(Default)]
struct State {
    held: u64,
    peak: u64,
    next_ticket: u64,
    serving: u64,
    /// Tickets whose requests stopped before being served.
    abandoned: BTreeSet<u64>,
}

impl State {
    fn advance(&mut self) {
        self.serving += 1;
        while self.abandoned.remove(&self.serving) {
            self.serving += 1;
        }
    }

    fn add(&mut self, bytes: u64) {
        self.held = self.held.saturating_add(bytes);
        self.peak = self.peak.max(self.held);
    }
}

impl Budget {
    pub(crate) fn new(limit: u64) -> Arc<Budget> {
        Arc::new(Budget {
            limit,
            state: Mutex::new(State::default()),
            cv: Condvar::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Wait for `bytes` in arrival order. `None` once `stop()` is true.
    pub(crate) fn acquire(self: &Arc<Self>, bytes: u64, stop: &dyn Fn() -> bool) -> Option<Permit> {
        let mut s = self.lock();
        let ticket = s.next_ticket;
        s.next_ticket += 1;
        loop {
            if stop() {
                if s.serving == ticket {
                    s.advance();
                } else {
                    s.abandoned.insert(ticket);
                }
                drop(s);
                self.cv.notify_all();
                return None;
            }
            let fits = s.held == 0 || s.held.saturating_add(bytes) <= self.limit;
            if s.serving == ticket && fits {
                s.advance();
                s.add(bytes);
                drop(s);
                self.cv.notify_all();
                return Some(Permit {
                    budget: Arc::clone(self),
                    bytes,
                });
            }
            s = self
                .cv
                .wait_timeout(s, POLL)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
    }

    /// The most bytes held at once so far.
    pub(crate) fn peak(&self) -> u64 {
        self.lock().peak
    }

    #[cfg(test)]
    fn held(&self) -> u64 {
        self.lock().held
    }

    #[cfg(test)]
    fn tickets_issued(&self) -> u64 {
        self.lock().next_ticket
    }
}

/// Bytes reserved from a [`Budget`], released on drop.
pub(crate) struct Permit {
    budget: Arc<Budget>,
    bytes: u64,
}

impl Permit {
    /// Raise the reservation to `bytes` without waiting (a file that grew
    /// while being read). Never shrinks it.
    pub(crate) fn grow_to(&mut self, bytes: u64) {
        if bytes > self.bytes {
            self.budget.lock().add(bytes - self.bytes);
            self.bytes = bytes;
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut s = self.budget.lock();
        s.held = s.held.saturating_sub(self.bytes);
        drop(s);
        self.budget.cv.notify_all();
    }
}

/// A quarter of physical memory, or 4 GiB if it cannot be read.
pub(crate) fn default_limit() -> u64 {
    physical_memory()
        .map(|m| m / 4)
        .filter(|&l| l > 0)
        .unwrap_or(FALLBACK_LIMIT)
}

#[cfg(target_os = "macos")]
pub(crate) fn physical_memory() -> Option<u64> {
    let mut bytes: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: `hw.memsize` is a u64; `bytes` and `len` describe a buffer of
    // exactly that size, and no new value is set.
    let rc = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&mut bytes as *mut u64).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (rc == 0 && len == std::mem::size_of::<u64>() && bytes > 0).then_some(bytes)
}

#[cfg(target_os = "linux")]
pub(crate) fn physical_memory() -> Option<u64> {
    // SAFETY: sysconf takes no pointers.
    let (pages, size) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_PAGESIZE),
        )
    };
    if pages > 0 && size > 0 {
        (pages as u64).checked_mul(size as u64)
    } else {
        None
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(crate) fn physical_memory() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::thread;
    use std::time::Duration;

    const NEVER: &dyn Fn() -> bool = &|| false;

    #[test]
    fn grants_within_limit() {
        let b = Budget::new(100);
        let _a = b.acquire(60, NEVER).unwrap();
        let _c = b.acquire(40, NEVER).unwrap();
        assert_eq!(b.held(), 100);
    }

    #[test]
    fn waits_until_release() {
        let b = Budget::new(100);
        let first = b.acquire(60, NEVER).unwrap();
        let (tx, rx) = mpsc::channel();
        let b2 = Arc::clone(&b);
        let t = thread::spawn(move || {
            let p = b2.acquire(50, NEVER).unwrap();
            tx.send(()).unwrap();
            drop(p);
        });
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        drop(first);
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        t.join().unwrap();
        assert_eq!(b.peak(), 60);
        assert_eq!(b.held(), 0);
    }

    #[test]
    fn first_come_first_served() {
        let b = Budget::new(100);
        let big = b.acquire(90, NEVER).unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let spawn = |name: &'static str, bytes: u64| {
            let (b, order) = (Arc::clone(&b), Arc::clone(&order));
            thread::spawn(move || {
                let p = b.acquire(bytes, NEVER).unwrap();
                order.lock().unwrap().push(name);
                drop(p);
            })
        };
        let a = spawn("A", 50);
        wait_for_queue(&b, 2); // ticket 0 is `big`
        let c = spawn("B", 5);
        wait_for_queue(&b, 3);
        thread::sleep(Duration::from_millis(100));
        assert!(order.lock().unwrap().is_empty(), "B jumped the queue");
        drop(big);
        a.join().unwrap();
        c.join().unwrap();
        assert_eq!(*order.lock().unwrap(), vec!["A", "B"]);
    }

    #[test]
    fn oversize_runs_alone() {
        let b = Budget::new(100);
        let small = b.acquire(10, NEVER).unwrap();
        let (tx, rx) = mpsc::channel();
        let b2 = Arc::clone(&b);
        let t = thread::spawn(move || {
            let p = b2.acquire(250, NEVER).unwrap();
            tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(200));
            drop(p);
        });
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        drop(small);
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(b.peak(), 250);
        let (tx1, rx1) = mpsc::channel();
        let b3 = Arc::clone(&b);
        let t1 = thread::spawn(move || {
            let p = b3.acquire(1, NEVER).unwrap();
            tx1.send(b3.held()).unwrap();
            drop(p);
        });
        // While the oversize permit is held, nothing else is granted.
        assert!(rx1.recv_timeout(Duration::from_millis(50)).is_err());
        t.join().unwrap();
        assert_eq!(rx1.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
        t1.join().unwrap();
    }

    #[test]
    fn zero_byte_request_does_not_wait_for_bytes() {
        let b = Budget::new(100);
        let _full = b.acquire(100, NEVER).unwrap();
        let (tx, rx) = mpsc::channel();
        let b2 = Arc::clone(&b);
        thread::spawn(move || {
            let p = b2.acquire(0, NEVER);
            tx.send(p.is_some()).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
    }

    #[test]
    fn grow_to_never_waits() {
        let b = Budget::new(100);
        let mut p = b.acquire(100, NEVER).unwrap();
        p.grow_to(150);
        assert_eq!(b.held(), 150);
        p.grow_to(120); // never shrinks
        assert_eq!(b.held(), 150);
        drop(p);
        assert_eq!(b.held(), 0);
        assert_eq!(b.peak(), 150);
    }

    #[test]
    fn stop_wakes_a_waiter() {
        let b = Budget::new(100);
        let _full = b.acquire(100, NEVER).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel();
        let (b2, stop2) = (Arc::clone(&b), Arc::clone(&stop));
        thread::spawn(move || {
            let p = b2.acquire(10, &|| stop2.load(Ordering::SeqCst));
            tx.send(p.is_none()).unwrap();
        });
        thread::sleep(Duration::from_millis(50));
        stop.store(true, Ordering::SeqCst);
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
    }

    #[test]
    fn physical_memory_is_detected() {
        let n = physical_memory().expect("physical memory");
        assert!(n >= 256 << 20, "{n}");
        assert_eq!(default_limit(), n / 4);
    }

    /// Wait until `n` requests have taken a ticket.
    fn wait_for_queue(b: &Budget, n: u64) {
        for _ in 0..500 {
            if b.tickets_issued() >= n {
                return;
            }
            thread::sleep(Duration::from_millis(2));
        }
        panic!("queue never reached {n}");
    }
}
