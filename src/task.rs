use async_trait::async_trait;
use futures::future::Shared;
use futures::{FutureExt};
use std::time::Duration;
use tokio::time::delay_for;
use tracing::warn;

use crate::channel::{mpsc_channel, one_shot_channel, MPSCReceiver, MPSCSender, OneShotReceiver, OneShotSender};
use crate::errors::{SendError, StartError, StopError};
use crate::rt;
use crate::util::type_name_short;
use std::marker::PhantomData;

pub trait Identifiable: Sized {
    fn id(&self) -> String {
        format!("{}<{:p}>", type_name_short::<Self>(), self)
    }
}
impl<T: Sized> Identifiable for T {}

pub trait Message: Identifiable + Send + 'static {}

impl<T: Send + 'static> Message for T {}

#[derive(Clone, PartialEq, Debug)]
pub enum TaskStatus {
    Init,
    Started,
    Stopped,
    Errored,
}

#[derive(Clone)]
pub struct TaskStatusHandler {
    start: Shared<OneShotReceiver<()>>,
    stop: Shared<OneShotReceiver<()>>,
    error: Shared<OneShotReceiver<()>>,
}

pub struct TaskStatusReporter {
    pub(crate) start: OneShotSender<()>,
    pub(crate) stop: OneShotSender<()>,
    pub(crate) error: OneShotSender<()>,
}

fn new_task_status_pair() -> (TaskStatusHandler, TaskStatusReporter) {
    let (start_sender, start_receiver) = one_shot_channel();
    let (stop_sender, stop_receiver) = one_shot_channel();
    let (error_sender, error_receiver) = one_shot_channel();
    let start = start_receiver.shared();
    let stop = stop_receiver.shared();
    let error = error_receiver.shared();

    (
        TaskStatusHandler { start, stop, error },
        TaskStatusReporter { start: start_sender, stop: stop_sender, error: error_sender },
    )
}

impl TaskStatusHandler {
    pub fn status(&self) -> TaskStatus {
        let start = self.start.peek();
        let stop = self.stop.peek();
        let error = self.error.peek();
        match (start, stop, error) {
            (_, _, Some(Err(_))) => TaskStatus::Stopped,
            (_, _, Some(Ok(_))) => TaskStatus::Errored,
            (_, Some(_), None) => TaskStatus::Stopped,
            (Some(_), None, None) => TaskStatus::Started,
            _ => TaskStatus::Init,
        }
    }

    pub fn wait_started(&self) -> Shared<OneShotReceiver<()>> {
        self.start.clone()
    }

    pub fn wait_stopped(&self) -> Shared<OneShotReceiver<()>> {
        self.stop.clone()
    }

    pub fn wait_errored(&self) -> Shared<OneShotReceiver<()>> {
        self.error.clone()
    }
}

struct EnvelopeWithResponse<M: Message, R: Message, T: AcTask + HandleWithResponse<M, R>> {
    message: M,
    reply_to: OneShotSender<R>,
    _p: PhantomData<T>,
}

struct Envelope<M: Message, T: AcTask + Handle<M>> {
    message: M,
    _p: PhantomData<T>,
}


#[async_trait]
pub trait EnvelopeDispatcher<T: AcTask>: Send {
    async fn dispatch_envelope(self: Box<Self>, task: &mut T);
}

#[async_trait]
impl<M: Message, T: AcTask + Handle<M>> EnvelopeDispatcher<T> for Envelope<M, T> {
    async fn dispatch_envelope(self: Box<Self>, task: &mut T) {
        let Envelope::<M, T> { message, .. } = *self;
       T::handle(task, message).await;
    }
}

#[async_trait]
impl<M: Message, R: Message, T: AcTask + HandleWithResponse<M, R>> EnvelopeDispatcher<T> for EnvelopeWithResponse<M, R, T> {
    async fn dispatch_envelope(self: Box<Self>, task: &mut T) {
        let EnvelopeWithResponse::<M, R, T> { message, reply_to, .. } = *self;
        let msg_id = message.id();
        let result = T::handle(task, message).await;
        reply_to.send(result).map_err(|_| eprintln!("Error replying message {} from Task : {}", msg_id, task.id()));
    }
}
pub struct TaskSender<T: AcTask> {
    pub sender: MPSCSender<Box<dyn EnvelopeDispatcher<T>>>,
    pub cancel: MPSCSender<()>,
    pub status: TaskStatusHandler,
}

pub struct TaskReceiver<T: AcTask> {
    pub(crate) receiver: MPSCReceiver<Box<dyn EnvelopeDispatcher<T>>>,
    pub(crate) cancel: MPSCReceiver<()>,
    pub(crate) status: TaskStatusReporter,
}

pub fn task_channel<T: AcTask>(size: usize) -> (TaskSender<T>, TaskReceiver<T>) {
    let (sender, receiver) = mpsc_channel(size);
    let (cancel_sender, cancel_receiver) = mpsc_channel(100);
    let (handler, reporter) = new_task_status_pair();

    (
        TaskSender::<T> { sender, cancel: cancel_sender, status: handler },
        TaskReceiver::<T> { receiver, cancel: cancel_receiver, status: reporter },
    )
}

impl<T: AcTask> Clone for TaskSender<T> {
    fn clone(&self) -> Self {
        let sender = self.sender.clone();
        let cancel = self.cancel.clone();
        let status = self.status.clone();
        TaskSender { sender, cancel, status }
    }
}

impl<T: AcTask> TaskSender<T> {
    async fn _send_with_response<M: Message, R: Message>(
        self_sender: &mut MPSCSender<Box<dyn EnvelopeDispatcher<T>>>,
        msg: M,
    ) -> Result<OneShotReceiver<R>, SendError>
    where
        T: HandleWithResponse<M, R>,
    {
        let (sender, receiver) = one_shot_channel::<R>();
        let message = EnvelopeWithResponse::<M, R, T> { message: msg, reply_to: sender, _p: PhantomData };

        let message = Box::new(message);
        let result = self_sender.try_send(message);
        match result {
            Ok(_) => Ok(receiver),
            Err(e) => Err(SendError::Err(Box::new(e))),
        }
    }

    async fn _send<M: Message>(
        self_sender: &mut MPSCSender<Box<dyn EnvelopeDispatcher<T>>>,
        msg: M,
    ) -> Result<(), SendError>
        where
            T: Handle<M>,
    {
        let message = Envelope::<M, T> { message: msg, _p: PhantomData };

        let message: Box<dyn EnvelopeDispatcher<T>> = Box::new(message);
        self_sender.try_send(message).map_err(|e| SendError::Err(Box::new(e)))
    }

    pub async fn send<M: Message>(&mut self, msg: M) -> Result<(), SendError>
    where
        T: Handle<M>,
    {
        Self::_send(&mut self.sender, msg).await.map(|_| ())
    }

    pub async fn send_and_receive<M: Message, R: Message>(
        &mut self,
        msg: M,
    ) -> Result<OneShotReceiver<R>, SendError>
    where
        T: HandleWithResponse<M, R>,
    {
        Self::_send_with_response(&mut self.sender, msg).await
    }

    pub fn send_delayed<M: Message>(&mut self, duration: Duration, msg: M)
    where
        T: Handle<M>,
    {
        let mut sender = self.sender.clone();
        rt::spawn_raw(async move {
            delay_for(duration).await;
            let _ = Self::_send(&mut sender, msg).await.map_err(|e| warn!("Error Sending message: {}", e));
        });
    }

    pub fn send_and_receive_delayed<M: Message, R: Message>(&mut self, duration: Duration, msg: M)
        where
            T: HandleWithResponse<M, R>,
    {
        let mut sender = self.sender.clone();
        rt::spawn_raw(async move {
            delay_for(duration).await;
            let _ = Self::_send_with_response(&mut sender, msg).await.map_err(|e| warn!("Error Sending message: {}", e));
        });
    }

    pub fn stop(&mut self) -> Result<(), SendError> {

        self.cancel.try_send(()).map_err(|_| SendError::Empty)
    }
}

#[async_trait]
pub trait Handle<M: Message>: AcTask {
    async fn handle(&mut self, msg: M);
}

#[async_trait]
pub trait HandleWithResponse<M: Message, R: Message>: AcTask {
    async fn handle(&mut self, msg: M) -> R;
}

#[async_trait]
pub trait AcTask: Identifiable + Send + Sized + 'static {
    #[allow(unused_variables)]
    async fn start(&mut self, sender: TaskSender<Self>) -> Result<(), StartError> {
        Ok(())
    }
    async fn stop(&mut self) -> Result<(), StopError> {
        Ok(())
    }
}

pub trait ParallelAcTask: AcTask + Clone {}
