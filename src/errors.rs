use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum SendError {
    #[error("Send Error")]
    Empty,
    #[error("Send Error")]
    Err(Box<dyn std::error::Error>),
}

#[derive(ThisError, Debug)]
pub enum TaskError {
    #[error("Send Error")]
    StartError(StartError),
    #[error("Send Error")]
    StopError(StopError),
}

#[derive(ThisError, Debug)]
#[error("Start Error")]
pub struct StartError;

#[derive(ThisError, Debug)]
#[error("Stop Error")]
pub struct StopError;