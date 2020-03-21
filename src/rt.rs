use futures::{pin_mut, select, FutureExt};
use std::any::type_name;
use std::future::Future;
use tokio::task::JoinHandle;
use tracing::{debug, info, info_span, warn};
use tracing_futures::Instrument;

use crate::errors::{TaskError, SendError};
use crate::channel::{MPSCReceiver, OneShotSender, Receiver, Sender};
use crate::task::{
    task_channel, EnvelopeDispatcher, Message, ParallelAcTask, AcTask, TaskReceiver, TaskSender, TaskStatusReporter,
};
use crate::util::type_name_short;

#[allow(clippy::unnecessary_mut_passed)]
async fn run_task<T: AcTask>(
    task: &mut T,
    mut receiver: MPSCReceiver<Box<dyn EnvelopeDispatcher<T>>>,
    sender: TaskSender<T>,
    start: OneShotSender<()>,
    stop: OneShotSender<()>,
    mut cancel: MPSCReceiver<()>,
) -> Result<(), TaskError> {

    let status1 = sender.status.clone();
    let status2 = sender.status.clone();
    let status3 = sender.status.clone();

    // needed to be able to peek the shared future
    tokio::spawn(async move {
        let _ = status1.wait_started().await.map_err(|_| warn!("Error Waiting started Signal"));
    });
    tokio::spawn(async move {
        let _ = status2.wait_stopped().await;
    });
    tokio::spawn(async move {
        let _ = status3.wait_errored().await;
    });

    debug!("Starting Task");

    task.start(sender).await.map_err(|e| {
        warn!("Error starting Task: {}", e);
        TaskError::StartError(e)
    })?;

    let _ = start.send(()).map_err(|_| warn!("Error Sending Start Signal"));
    debug!("Task Started");

    let cancel_fut = cancel.recv().fuse();
    pin_mut!(cancel_fut);
    loop {
        let recv_fut = receiver.recv().fuse();
        pin_mut!(recv_fut);

        let result = select! {
            msg = recv_fut => msg,
            cancel = cancel_fut => None,
        };

        if let Some(msg) = result {
            debug!("Received message");
            msg.dispatch_envelope(task).await;
        } else {
            break;
        }
    }

    drop(receiver);

    let _ = stop.send(()).map_err(|_| warn!("Error Sending Stop Signal"));

    debug!("Stopping Task");
    task.stop().await.map_err(|e| {
        warn!("Error stopping Task: {}", e);
        TaskError::StopError(e)
    })?;
    debug!("Task Stopped");

    Ok(())
}

#[allow(clippy::unnecessary_mut_passed)]
async fn run_task_parallel<T: ParallelAcTask>(
    task: &mut T,
    mut receiver: MPSCReceiver<Box<dyn EnvelopeDispatcher<T>>>,
    sender: TaskSender<T>,
    start: OneShotSender<()>,
    stop: OneShotSender<()>,
    mut cancel: MPSCReceiver<()>,
) -> Result<(), TaskError> {

    let status = sender.status.clone();
    let status1 = sender.status.clone();
    let status2 = sender.status.clone();

    // needed to be able to peek the shared future
    tokio::spawn(async move {
        let _ = status.wait_started().await.map_err(|_| warn!("Error Waiting Starting Signal"));
    });
    tokio::spawn(async move {
        let _ = status1.wait_stopped().await.map_err(|_| warn!("Error Waiting Stopping Signal"));
    });
    tokio::spawn(async move {
        let _ = status2.wait_errored().await.map_err(|_| warn!("Error Waiting Error Signal"));
    });

    debug!("Starting Task");

    task.start(sender).await.map_err(|e| {
        warn!("Error starting Task: {}", e);
        TaskError::StartError(e)
    })?;

    let _ = start.send(()).map_err(|_| warn!("Error Sending Start Signal"));
    debug!("Task Started");

    let cancel_fut = cancel.recv().fuse();
    pin_mut!(cancel_fut);
    loop {
        let recv_fut = receiver.recv().fuse();
        pin_mut!(recv_fut);

        let result = select! {
            msg = recv_fut => msg,
            cancel = cancel_fut => None,
        };

        if let Some(msg) = result {
            debug!("Received message");
            let inner_task = task.clone();
            // TODO handle panics in parallel tasks / log / instrument
            tokio::spawn(async move {
                let mut inner_task = inner_task;
                msg.dispatch_envelope(&mut inner_task).await;
            });
        } else {
            break;
        }
    }

    drop(receiver);

    let _ = stop.send(()).map_err(|_| warn!("Error Sending Stop Signal"));

    debug!("Stopping Task");
    task.stop().await.map_err(|e| {
        warn!("Error stopping Task: {}", e);
        TaskError::StopError(e)
    })?;
    debug!("Task Stopped");

    Ok(())
}

pub fn async_create_spawn<T, F, R>(channel_size: usize, builder: F) -> Result<TaskSender<T>, TaskError>
where
    T: AcTask,
    F: FnOnce() -> R + Send + 'static,
    R: Future<Output = Result<T, TaskError>> + Send,
{
    let (sender, receiver) = task_channel(channel_size);

    let inner_sender = sender.clone();
    tokio::spawn(async move {
        let TaskReceiver { receiver, cancel, status: TaskStatusReporter { start, stop, error } } = receiver;

        let result: Result<(), TaskError> = try {
            let task_name = type_name::<T>();
            let mut task = builder().instrument(info_span!("Create Task", id = task_name)).await.map_err(|e| {
                warn!("Error creating Task {}: {}", task_name, e);
                e
            })?;

            let task_name = task.id();
            let task_name: &str = task_name.as_ref();
            run_task(&mut task, receiver, inner_sender, start, stop, cancel)
                .instrument(info_span!("Task", id = task_name))
                .await?;
        };

        if result.is_err() {
            let _ = error.send(()).map_err(|_| warn!("Error Sending Stop Signal"));
        }
    });
    Ok(sender)
}

pub fn create_spawn<T: AcTask, F: FnOnce() -> T>(channel_size: usize, builder: F) -> TaskSender<T> {
    let (sender, receiver) = task_channel(channel_size);
    let task = builder();
    spawn(receiver, sender.clone(), task);
    sender
}

pub fn spawn<T: AcTask>(receiver: TaskReceiver<T>, sender: TaskSender<T>, mut task: T) {
    let task_name = task.id();
    let task_name: &str = task_name.as_ref();

    tokio::spawn(
        async move {
            let TaskReceiver { receiver, cancel, status: TaskStatusReporter { start, stop, error } } = receiver;

            run_task(&mut task, receiver, sender, start, stop, cancel)
                .await
                .map_err(|_| {
                    let _ = error.send(()).map_err(|_| warn!("Error Sending Stop Signal"));
                })
                .unwrap();
        }
        .instrument(info_span!("Task", id = task_name)),
    );
}

pub fn spawn_parallel<T: ParallelAcTask>(receiver: TaskReceiver<T>, sender: TaskSender<T>, mut task: T) {
    let task_name = task.id();
    let task_name: &str = task_name.as_ref();

    tokio::spawn(
        async move {
            let TaskReceiver { receiver, cancel, status: TaskStatusReporter { start, stop, error } } = receiver;

            run_task_parallel(&mut task, receiver, sender, start, stop, cancel)
                .await
                .map_err(|_| {
                    let _ = error.send(()).map_err(|_| warn!("Error Sending Stop Signal"));
                })
                .unwrap();
        }
        .instrument(info_span!("Task", id = task_name)),
    );
}

pub fn spawn_connect<I, R, S>(receiver: R, sender: S)
where
    I: Message,
    R: Receiver<I> + Send + 'static,
    S: Sender<I> + Send + 'static,
{
    spawn_connect_transform(receiver, sender, |a| a);
}

pub fn spawn_connect_transform<I, R, S, O>(mut receiver: R, mut sender: S, transformer: fn(I) -> O)
where
    I: Message,
    O: Message,
    R: Receiver<I> + Send + 'static,
    S: Sender<O> + Send + 'static,
{
    let spawn_connect_id = connect_id(&receiver, &sender);
    let spawn_connect_id: &str = spawn_connect_id.as_ref();

    tokio::spawn(
        async move {
            let result: Result<(), SendError> = try {
                info!("Start");

                while let Some(msg) = receiver.recv().await {
                    debug!("Received message");

                    let msg = transformer(msg);
                    sender.send(msg).await.map_err(|e| {
                        warn!("Error forwarding Queue Message to Sender: {}", e);
                        e
                    })?;
                }

                info!("Stop");
            };
            if let Err(e) = result {
                warn!("Stopped task because of Error: {}", e)
                // TODO errored.store(Arc::new(true));
            }
        }
        .instrument(info_span!("Spawn Connect", id = spawn_connect_id)),
    );
}

fn connect_id<R, S, I, O>(receiver: &R, sender: &S) -> String
where
    I: Message,
    O: Message,
    R: Receiver<I> + Send + 'static,
    S: Sender<O> + Send + 'static,
{
    let r_name = type_name_short::<R>();
    let s_name = type_name_short::<S>();
    let i_name = type_name_short::<I>();
    let o_name = type_name_short::<O>();

    format!("{}<{:p}> -> (|{}| -> {}) -> {}<{:p}>", r_name, receiver, i_name, o_name, s_name, sender)
}

pub fn spawn_raw<T>(task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(task)
}
