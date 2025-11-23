// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This implementation is derived from the `oneshot` crate [1], with significant simplifications
// since mea needs not support synchronized receiving functions.
//
// [1] https://github.com/faern/oneshot/blob/25274e99/src/lib.rs

//! A one-shot channel is used for sending a single message between
//! asynchronous tasks. The [`channel`] function is used to create a
//! [`Sender`] and [`Receiver`] handle pair that form the channel.
//!
//! The `Sender` handle is used by the producer to send the value.
//! The `Receiver` handle is used by the consumer to receive the value.
//!
//! Each handle can be used on separate tasks.
//!
//! Since the `send` method is not async, it can be used anywhere. This includes
//! sending between two runtimes, and using it from non-async code.
//!
//! # Examples
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use mea::oneshot;
//!
//! let (tx, rx) = oneshot::channel();
//!
//! tokio::spawn(async move {
//!     if let Err(_) = tx.send(3) {
//!         println!("the receiver dropped");
//!     }
//! });
//!
//! match rx.await {
//!     Ok(v) => println!("got = {:?}", v),
//!     Err(_) => println!("the sender dropped"),
//! }
//! # }
//! ```
//!
//! If the sender is dropped without sending, the receiver will fail with
//! [`RecvError`]:
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use mea::oneshot;
//!
//! let (tx, rx) = oneshot::channel::<u32>();
//!
//! tokio::spawn(async move {
//!     drop(tx);
//! });
//!
//! match rx.await {
//!     Ok(_) => panic!("This doesn't happen"),
//!     Err(_) => println!("the sender dropped"),
//! }
//! # }
//! ```

use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::future::IntoFuture;
use std::hint;
use std::mem;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

#[cfg(test)]
mod tests;

/// Creates a new oneshot channel and returns the two endpoints, [`Sender`] and [`Receiver`].
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let channel_ptr = NonNull::from(Box::leak(Box::new(Channel::new())));
    (Sender { channel_ptr }, Receiver { channel_ptr })
}

/// Sends a value to the associated [`Receiver`].
#[derive(Debug)]
pub struct Sender<T> {
    channel_ptr: NonNull<Channel<T>>,
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Sync> Sync for Sender<T> {}

#[inline(always)]
fn sender_wake_up_receiver<T>(channel: &Channel<T>, state: u8) {
    // ORDERING: Synchronizes with writing waker to memory, and prevents the
    // taking of the waker from being ordered before this operation.
    fence(Ordering::Acquire);

    // Take the waker, but critically do not awake it. If we awake it now, the
    // receiving thread could still observe the AWAKING state and re-await, meaning
    // that after we change to the MESSAGE state, it would remain waiting indefinitely
    // or until a spurious wakeup.
    //
    // SAFETY: at this point we are in the AWAKING state, and the receiving thread
    // does not access the waker while in this state, nor does it free the channel
    // allocation in this state.
    let waker = unsafe { channel.take_waker() };

    // ORDERING: this ordering serves two-fold: it synchronizes with the acquire load
    // in the receiving thread, ensuring that both our read of the waker and write of
    // the message happen-before the taking of the message and freeing of the channel.
    // Furthermore, we need acquire ordering to ensure awaking the receiver
    // happens after the channel state is updated.
    channel.state.swap(state, Ordering::AcqRel);

    // Note: it is possible that between the store above and this statement that
    // the receiving thread is spuriously awakened, takes the message, and frees
    // the channel allocation. However, we took ownership of the channel out of
    // that allocation, and freeing the channel does not drop the waker since the
    // waker is wrapped in MaybeUninit. Therefore, this data is valid regardless of
    // whether the receiver has completed by this point.
    waker.wake();
}

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning an error contains the message if it
    /// could not be sent.
    pub fn send(self, message: T) -> Result<(), SendError<T>> {
        let channel_ptr = self.channel_ptr;

        // Do not run the Drop implementation if send was called, any cleanup happens below.
        mem::forget(self);

        // SAFETY: The channel exists on the heap for the entire duration of this method, and we
        // only ever acquire shared references to it. Note that if the receiver disconnects it
        // does not free the channel.
        let channel = unsafe { channel_ptr.as_ref() };

        // Write the message into the channel on the heap.
        //
        // SAFETY: The receiver only ever accesses this memory location if we are in the MESSAGE
        // state, and since we are responsible for setting that state, we can guarantee that we have
        // exclusive access to this memory location to perform this write.
        unsafe { channel.write_message(message) };

        // Update the state to signal there is a message on the channel:
        //
        // * EMPTY + 1 = MESSAGE
        // * RECEIVING + 1 = AWAKING
        // * DISCONNECTED + 1 = EMPTY (invalid), however this state is never observed
        //
        // ORDERING: we use release ordering to ensure writing the message is visible to the
        // receiving thread. The EMPTY and DISCONNECTED branches do not observe any shared state,
        // and thus we do not need an acquire ordering. The RECEIVING branch manages synchronization
        // independent of this operation.
        match channel.state.fetch_add(1, Ordering::Release) {
            // The receiver is alive and has not started waiting. Send done.
            EMPTY => Ok(()),
            // The receiver is waiting. Wake it up so it can return the message.
            RECEIVING => {
                sender_wake_up_receiver(channel, MESSAGE);
                Ok(())
            }
            // The receiver was already dropped. The error is responsible for freeing the channel.
            //
            // SAFETY: since the receiver disconnected it will no longer access `channel_ptr`, so
            // we can transfer exclusive ownership of the channel's resources to the error.
            // Moreover, since we just placed the message in the channel, the channel contains a
            // valid message.
            DISCONNECTED => Err(SendError { channel_ptr }),
            state => unreachable!("unexpected channel state: {}", state),
        }
    }

    /// Returns true if the associated [`Receiver`] has been dropped.
    ///
    /// If true is returned, a future call to send is guaranteed to return an error.
    pub fn is_closed(&self) -> bool {
        // SAFETY: The channel exists on the heap for the entire duration of this method, and we
        // only ever acquire shared references to it. Note that if the receiver disconnects it
        // does not free the channel.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: We *chose* a Relaxed ordering here as it sufficient to enforce the method's
        // contract: "if true is returned, a future call to send is guaranteed to return an error."
        //
        // Once true has been observed, it will remain true. However, if false is observed,
        // the receiver might have just disconnected but this thread has not observed it yet.
        matches!(channel.state.load(Ordering::Relaxed), DISCONNECTED)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // SAFETY: The receiver only ever frees the channel if we are in the MESSAGE or
        // DISCONNECTED states.
        //
        // * If we are in the MESSAGE state, then we called mem::forget(self), so we should
        // not be in this function call.
        // * If we are in the DISCONNECTED state, then the receiver either received a MESSAGE
        // so this statement is unreachable, or was dropped and observed that our side was still
        // alive, and thus didn't free the channel.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // Update the channel state to disconnected:
        //
        // * EMPTY ^ 001 = DISCONNECTED
        // * RECEIVING ^ 001 = AWAKING
        // * DISCONNECTED ^ 001 = EMPTY (invalid), but this state is never observed
        //
        // ORDERING: we need not release ordering here since there are no modifications we
        // need to make visible to other thread, and the Err(RECEIVING) branch handles
        // synchronization independent of this fetch_xor
        match channel.state.fetch_xor(0b001, Ordering::Relaxed) {
            // The receiver has not started waiting, nor is it dropped.
            EMPTY => {}
            // The receiver is waiting. Wake it up so it can detect that the channel disconnected.
            RECEIVING => sender_wake_up_receiver(channel, DISCONNECTED),
            // The receiver was already dropped. We are responsible for freeing the channel.
            DISCONNECTED => {
                // SAFETY: when the receiver switches the state to DISCONNECTED they have received
                // the message or will no longer be trying to receive the message, and have
                // observed that the sender is still alive, meaning that we are responsible for
                // freeing the channel allocation.
                unsafe { dealloc(self.channel_ptr) };
            }
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

/// Receives a value from the associated [`Sender`].
#[derive(Debug)]
pub struct Receiver<T> {
    channel_ptr: NonNull<Channel<T>>,
}

unsafe impl<T: Send> Send for Receiver<T> {}

impl<T> IntoFuture for Receiver<T> {
    type Output = Result<T, RecvError>;

    type IntoFuture = Recv<T>;

    fn into_future(self) -> Self::IntoFuture {
        let Receiver { channel_ptr } = self;
        // Do not run our Drop implementation, since the receiver lives on as the new future.
        mem::forget(self);
        Recv { channel_ptr }
    }
}

impl<T> Receiver<T> {
    /// Returns true if the associated [`Sender`] was dropped before sending a message. Or if
    /// the message has already been received.
    ///
    /// If `true` is returned, all future calls to receive the message are guaranteed to return
    /// [`RecvError`]. And future calls to this method is guaranteed to also return `true`.
    pub fn is_closed(&self) -> bool {
        // SAFETY: the existence of the `self` parameter serves as a certificate that the receiver
        // is still alive, meaning that even if the sender was dropped then it would have observed
        // the fact that we are still alive and left the responsibility of deallocating the
        // channel to us, so `self.channel` is valid
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: We *chose* a Relaxed ordering here as it is sufficient to
        // enforce the method's contract.
        //
        // Once true has been observed, it will remain true. However, if false is observed,
        // the sender might have just disconnected but this thread has not observed it yet.
        matches!(channel.state.load(Ordering::Relaxed), DISCONNECTED)
    }

    /// Returns true if there is a message in the channel, ready to be received.
    ///
    /// If `true` is returned, the next call to receive the message is guaranteed to return
    /// the message immediately.
    pub fn has_message(&self) -> bool {
        // SAFETY: the existence of the `self` parameter serves as a certificate that the receiver
        // is still alive, meaning that even if the sender was dropped then it would have observed
        // the fact that we are still alive and left the responsibility of deallocating the
        // channel to us, so `self.channel` is valid
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: An acquire ordering is used to guarantee no subsequent loads is reordered
        // before this one. This upholds the contract that if true is returned, the next call to
        // receive the message is guaranteed to also observe the `MESSAGE` state and return the
        // message immediately.
        matches!(channel.state.load(Ordering::Acquire), MESSAGE)
    }

    /// Checks if there is a message in the channel without blocking. Returns:
    ///
    /// * `Ok(Some(message))` if there was a message in the channel.
    /// * `Ok(None)` if the [`Sender`] is alive, but has not yet sent a message.
    /// * `Err(RecvError)` if the [`Sender`] was dropped before sending anything or if the message
    ///   has already been extracted by a previous `try_recv` call.
    ///
    /// If a message is returned, the channel is disconnected and any subsequent receive operation
    /// using this receiver will return an [`RecvError`].
    pub fn try_recv(&self) -> Result<Option<T>, RecvError> {
        // SAFETY: The channel will not be freed while this method is still running.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: we use acquire ordering to synchronize with the store of the message.
        match channel.state.load(Ordering::Acquire) {
            EMPTY => Ok(None),
            DISCONNECTED => Err(RecvError(())),
            MESSAGE => {
                // It is okay to break up the load and store since once we are in the MESSAGE state,
                // the sender no longer modifies the state
                //
                // ORDERING: at this point the sender has done its job and is no longer active, so
                // we need not make any side effects visible to it.
                channel.state.store(DISCONNECTED, Ordering::Relaxed);

                // SAFETY: we are in the MESSAGE state so the message is present
                Ok(Some(unsafe { channel.take_message() }))
            }
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // SAFETY: since the receiving side is still alive the sender would have observed that and
        // left deallocating the channel allocation to us.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // Set the channel state to disconnected and read what state the receiver was in.
        match channel.state.swap(DISCONNECTED, Ordering::Acquire) {
            // The sender has not sent anything, nor is it dropped.
            EMPTY => {}
            // The sender already sent something. We must drop it, and free the channel.
            MESSAGE => {
                unsafe { channel.drop_message() };
                unsafe { dealloc(self.channel_ptr) };
            }
            // The sender was already dropped. We are responsible for freeing the channel.
            DISCONNECTED => {
                unsafe { dealloc(self.channel_ptr) };
            }
            // NOTE: the receiver, unless transformed into a future, will never see the
            // RECEIVING or AWAKING states, so we can ignore them here.
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

/// A future that completes when the message is sent from the associated [`Sender`], or the
/// [`Sender`] is dropped before sending a message.
#[derive(Debug)]
pub struct Recv<T> {
    channel_ptr: NonNull<Channel<T>>,
}

unsafe impl<T: Send> Send for Recv<T> {}

fn recv_awaken<T>(channel: &Channel<T>) -> Poll<Result<T, RecvError>> {
    loop {
        hint::spin_loop();

        // ORDERING: The load above has already synchronized with writing message.
        match channel.state.load(Ordering::Relaxed) {
            AWAKING => {}
            DISCONNECTED => break Poll::Ready(Err(RecvError(()))),
            MESSAGE => {
                // ORDERING: the sender has been dropped, so this update only
                // needs to be visible to us.
                channel.state.store(DISCONNECTED, Ordering::Relaxed);
                // SAFETY: We observed the MESSAGE state.
                break Poll::Ready(Ok(unsafe { channel.take_message() }));
            }
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

impl<T> Future for Recv<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the existence of the `self` parameter serves as a certificate that the receiver
        // is still alive, meaning that even if the sender was dropped then it would have observed
        // the fact that we are still alive and left the responsibility of deallocating the
        // channel to us, so `self.channel` is valid
        let channel = unsafe { self.channel_ptr.as_ref() };

        // ORDERING: we use acquire ordering to synchronize with the store of the message.
        match channel.state.load(Ordering::Acquire) {
            // The sender is alive but has not sent anything yet.
            EMPTY => {
                let waker = cx.waker().clone();
                // SAFETY: We can not be in the forbidden states, and no waker in the channel.
                unsafe { channel.write_waker(waker) }
            }
            // The sender sent the message.
            MESSAGE => {
                // ORDERING: the sender has been dropped so this update only needs to be
                // visible to us.
                channel.state.store(DISCONNECTED, Ordering::Relaxed);
                Poll::Ready(Ok(unsafe { channel.take_message() }))
            }
            // We were polled again while waiting for the sender. Replace the waker with the new
            // one.
            RECEIVING => {
                // ORDERING: We use relaxed ordering on both success and failure since we have not
                // written anything above that must be released, and the individual match arms
                // handle any additional synchronization.
                match channel.state.compare_exchange(
                    RECEIVING,
                    EMPTY,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    // We successfully changed the state back to EMPTY.
                    //
                    // This is the most likely branch to be taken, which is why we do not use any
                    // memory barriers in the compare_exchange above.
                    Ok(_) => {
                        let waker = cx.waker().clone();

                        // SAFETY: We wrote the waker in a previous call to poll. We do not need
                        // a memory barrier since the previous write here was by ourselves.
                        unsafe { channel.drop_waker() };

                        // SAFETY: We can not be in the forbidden states, and no waker in the
                        // channel.
                        unsafe { channel.write_waker(waker) }
                    }
                    // The sender sent the message while we prepared to replace the waker.
                    // We take the message and mark the channel disconnected.
                    // The sender has already taken the waker.
                    Err(MESSAGE) => {
                        // ORDERING: Synchronize with writing message. This branch is
                        // unlikely to be taken.
                        channel.state.swap(DISCONNECTED, Ordering::Acquire);

                        // SAFETY: The state tells us the sender has initialized the message.
                        Poll::Ready(Ok(unsafe { channel.take_message() }))
                    }
                    // The sender is currently waking us up.
                    Err(AWAKING) => recv_awaken(channel),
                    // The sender was dropped before sending anything while we prepared to park.
                    // The sender has taken the waker already.
                    Err(DISCONNECTED) => Poll::Ready(Err(RecvError(()))),
                    Err(state) => unreachable!("unexpected channel state: {}", state),
                }
            }
            // The sender has observed the RECEIVING state and is currently reading the waker from
            // a previous poll. We need to loop here until we observe the MESSAGE or DISCONNECTED
            // state. We busy loop here since we know the sender is done very soon.
            AWAKING => recv_awaken(channel),
            // The sender was dropped before sending anything.
            DISCONNECTED => Poll::Ready(Err(RecvError(()))),
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

impl<T> Drop for Recv<T> {
    fn drop(&mut self) {
        // SAFETY: since the receiving side is still alive the sender would have observed that and
        // left deallocating the channel allocation to us.
        let channel = unsafe { self.channel_ptr.as_ref() };

        // Set the channel state to disconnected and read what state the receiver was in.
        match channel.state.swap(DISCONNECTED, Ordering::Acquire) {
            // The sender has not sent anything, nor is it dropped.
            EMPTY => {}
            // The sender already sent something. We must drop it, and free the channel.
            MESSAGE => {
                unsafe { channel.drop_message() };
                unsafe { dealloc(self.channel_ptr) };
            }
            // The receiver has been polled. We must drop the waker.
            RECEIVING => {
                unsafe { channel.drop_waker() };
            }
            // The sender was already dropped. We are responsible for freeing the channel.
            DISCONNECTED => {
                // SAFETY: see safety comment at top of function.
                unsafe { dealloc(self.channel_ptr) };
            }
            // This receiver was previously polled, so the channel was in the RECEIVING state.
            // But the sender has observed the RECEIVING state and is currently reading the waker
            // to wake us up. We need to loop here until we observe the MESSAGE or DISCONNECTED
            // state. We busy loop here since we know the sender is done very soon.
            AWAKING => {
                loop {
                    hint::spin_loop();

                    // ORDERING: The swap above has already synchronized with writing message.
                    match channel.state.load(Ordering::Relaxed) {
                        AWAKING => {}
                        DISCONNECTED => break,
                        MESSAGE => {
                            // SAFETY: we are in the message state so the message is initialized.
                            unsafe { channel.drop_message() };
                            break;
                        }
                        state => unreachable!("unexpected channel state: {}", state),
                    }
                }
                unsafe { dealloc(self.channel_ptr) };
            }
            state => unreachable!("unexpected channel state: {}", state),
        }
    }
}

/// Internal channel data structure.
///
/// The [`channel`] method allocates and puts one instance of this struct on the heap for each
/// oneshot channel instance. The struct holds:
///
/// * The current state of the channel.
/// * The message in the channel. This memory is uninitialized until the message is sent.
/// * The waker instance for the task that is currently receiving on this channel. This memory is
///   uninitialized until the receiver starts receiving.
struct Channel<T> {
    state: AtomicU8,
    message: UnsafeCell<MaybeUninit<T>>,
    waker: UnsafeCell<MaybeUninit<Waker>>,
}

impl<T> Channel<T> {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(EMPTY),
            message: UnsafeCell::new(MaybeUninit::uninit()),
            waker: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    #[inline(always)]
    unsafe fn message(&self) -> &MaybeUninit<T> {
        unsafe { &*self.message.get() }
    }

    #[inline(always)]
    unsafe fn write_message(&self, message: T) {
        unsafe {
            let slot = &mut *self.message.get();
            slot.as_mut_ptr().write(message);
        }
    }

    #[inline(always)]
    unsafe fn drop_message(&self) {
        unsafe {
            let slot = &mut *self.message.get();
            slot.assume_init_drop();
        }
    }

    #[inline(always)]
    unsafe fn take_message(&self) -> T {
        unsafe { ptr::read(self.message.get()).assume_init() }
    }

    /// # Safety
    ///
    /// * The `waker` field must not have a waker stored when calling this method.
    /// * The `state` must not be in the RECEIVING state when calling this method.
    unsafe fn write_waker(&self, waker: Waker) -> Poll<Result<T, RecvError>> {
        unsafe {
            // Write the waker instance to the channel.
            //
            // SAFETY: we are not yet in the RECEIVING state, meaning that the sender will not
            // try to access the waker until it sees the state set to RECEIVING below.
            let slot = &mut *self.waker.get();
            slot.as_mut_ptr().write(waker);

            // ORDERING: we use release ordering on success so the sender can synchronize with
            // our write of the waker. We use relaxed ordering on failure since the sender does
            // not need to synchronize with our write and the individual match arms handle any
            // additional synchronization
            match self.state.compare_exchange(
                EMPTY,
                RECEIVING,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                // We stored our waker, now we return and let the sender wake us up.
                Ok(_) => Poll::Pending,
                // The sender sent the message while we prepared to await.
                // We take the message and mark the channel disconnected.
                Err(MESSAGE) => {
                    // ORDERING: Synchronize with writing message. This branch is unlikely to be
                    // taken, so it is likely more efficient to use a fence here
                    // instead of AcqRel ordering on the compare_exchange
                    // operation.
                    fence(Ordering::Acquire);

                    // SAFETY: we started in the EMPTY state and the sender switched us to the
                    // MESSAGE state. This means that it did not take the waker, so we're
                    // responsible for dropping it.
                    self.drop_waker();

                    // ORDERING: sender does not exist, so this update only needs to be visible to
                    // us.
                    self.state.store(DISCONNECTED, Ordering::Relaxed);

                    // SAFETY: The MESSAGE state tells us there is a correctly initialized message.
                    Poll::Ready(Ok(self.take_message()))
                }
                // The sender was dropped before sending anything while we prepared to await.
                Err(DISCONNECTED) => {
                    // SAFETY: we started in the EMPTY state and the sender switched us to the
                    // DISCONNECTED state. This means that it did not take the waker, so we are
                    // responsible for dropping it.
                    self.drop_waker();
                    Poll::Ready(Err(RecvError(())))
                }
                Err(state) => unreachable!("unexpected channel state: {}", state),
            }
        }
    }

    #[inline(always)]
    unsafe fn drop_waker(&self) {
        unsafe {
            let slot = &mut *self.waker.get();
            slot.assume_init_drop();
        }
    }

    #[inline(always)]
    unsafe fn take_waker(&self) -> Waker {
        unsafe { ptr::read(self.waker.get()).assume_init() }
    }
}

unsafe fn dealloc<T>(channel: NonNull<Channel<T>>) {
    unsafe { drop(Box::from_raw(channel.as_ptr())) }
}

/// An error returned when trying to send on a closed channel. Returned from
/// [`Sender::send`] if the corresponding [`Receiver`] has already been dropped.
///
/// The message that could not be sent can be retrieved again with [`SendError::into_inner`].
pub struct SendError<T> {
    channel_ptr: NonNull<Channel<T>>,
}

unsafe impl<T: Send> Send for SendError<T> {}
unsafe impl<T: Sync> Sync for SendError<T> {}

impl<T> SendError<T> {
    /// Get a reference to the message that failed to be sent.
    pub fn as_inner(&self) -> &T {
        unsafe { self.channel_ptr.as_ref().message().assume_init_ref() }
    }

    /// Consumes the error and returns the message that failed to be sent.
    pub fn into_inner(self) -> T {
        let channel_ptr = self.channel_ptr;

        // Do not run destructor if we consumed ourselves. Freeing happens below.
        mem::forget(self);

        // SAFETY: we have ownership of the channel
        let channel: &Channel<T> = unsafe { channel_ptr.as_ref() };

        // SAFETY: we know that the message is initialized according to the safety requirements of
        // `new`
        let message = unsafe { channel.take_message() };

        // SAFETY: we own the channel
        unsafe { dealloc(channel_ptr) };

        message
    }
}

impl<T> Drop for SendError<T> {
    fn drop(&mut self) {
        // SAFETY: we have ownership of the channel and require that the message is initialized
        // upon construction
        unsafe {
            self.channel_ptr.as_ref().drop_message();
            dealloc(self.channel_ptr);
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "sending on a closed channel".fmt(f)
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SendError<{}>(..)", stringify!(T))
    }
}

impl<T> std::error::Error for SendError<T> {}

/// An error returned when receiving the message.
///
/// The receiving operation can only fail if the corresponding [`Sender`] was dropped
/// before sending any message, or if a message has already been received on the channel.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RecvError(());

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "receiving on a closed channel".fmt(f)
    }
}

impl std::error::Error for RecvError {}

/// The initial channel state. Active while both endpoints are still alive, no message has been
/// sent, and the receiver is not receiving.
const EMPTY: u8 = 0b011;
/// A message has been sent to the channel, but the receiver has not yet read it.
const MESSAGE: u8 = 0b100;
/// No message has yet been sent on the channel, but the receiver future ([`Recv`]) is currently
/// receiving.
const RECEIVING: u8 = 0b000;
/// A message is sending to the channel, or the channel is closing. The receiver future ([`Recv`])
/// is currently being awakened.
const AWAKING: u8 = 0b001;
/// The channel has been closed. This means that either the sender or receiver has been dropped,
/// or the message sent to the channel has already been received.
const DISCONNECTED: u8 = 0b010;
