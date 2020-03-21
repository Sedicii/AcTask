use async_trait::async_trait;
use futures::StreamExt;
use tokio::net::{TcpListener, TcpStream};

pub use async_std::sync::{channel as mpmc_channel, Receiver as MPMCReceiver, Sender as MPMCSender};
pub use futures::channel::mpsc::{channel as mpsc_channel, Receiver as MPSCReceiver, Sender as MPSCSender};
pub use futures::channel::oneshot::{
    channel as one_shot_channel, Receiver as OneShotReceiver, Sender as OneShotSender,
};

use crate::errors::SendError;
use crate::task::{Handle, Message, AcTask, TaskSender, HandleWithResponse };
use std::marker::PhantomData;


#[async_trait]
pub trait Sender<M>: Send {
    type Return = ();

    async fn send(&mut self, data: M) -> Result<(), SendError>;
}

#[async_trait]
pub trait Receiver<M> {
    async fn recv(&mut self) -> Option<M>;
}

#[async_trait]
impl<M: Message, T: AcTask + Handle<M>> Sender<M> for TaskSender<T> {
    #[inline]
    async fn send(&mut self, data: M) -> Result<(), SendError> {
        self.send(data).await
    }
}
struct TaskSenderWithReturn<M: Message, R: Message, T: AcTask + HandleWithResponse<M, R>> {
    task_sender: TaskSender<T>,
    _m: PhantomData<M>,
    _r: PhantomData<R>,
}

#[async_trait]
impl<M: Message, R: Message, T: AcTask + HandleWithResponse<M, R>> Sender<M> for TaskSenderWithReturn<M, R, T> {
    type Return = R;

    #[inline]
    async fn send(&mut self, data: M)  -> Result<(), SendError>  {
        self.task_sender.send_and_receive(data).await.map(|_| ())
    }
}

#[async_trait]
impl<T: Send> Receiver<T> for MPSCReceiver<T> {
    #[inline]
    async fn recv(&mut self) -> Option<T> {
        self.next().await
    }
}

#[async_trait]
impl<T: Send> Receiver<T> for MPMCReceiver<T> {
    #[inline]
    async fn recv(&mut self) -> Option<T> {
        self.next().await
    }
}

#[async_trait]
impl<M: Message> Sender<M> for MPMCSender<M> {
    #[inline]
    async fn send(&mut self, data: M) -> Result<(), SendError> {
        self.send(data).await.map_err(|e| SendError::Err(Box::new(e)))
    }
}

#[async_trait]
impl<M: Message> Sender<M> for MPSCSender<M> {
    #[inline]
    async fn send(&mut self, data: M) -> Result<(), SendError> {
        self.try_send(data).map_err(|e| SendError::Err(Box::new(e)))
    }
}

#[async_trait]
impl<M: Send + 'static> Receiver<M> for OneShotReceiver<M> {
    #[inline]
    async fn recv(&mut self) -> Option<M> {
        self.await.ok()
    }
}

#[async_trait]
impl Receiver<TcpStream> for TcpListener {
    async fn recv(&mut self) -> Option<TcpStream> {
        self.accept().await.map(|(c, _)| c).ok()
    }
}
