use async_trait::async_trait;
use std::fmt::Debug;

use crate::{rt, Handle, AcTask};
use crate::task::HandleWithResponse;

#[derive(Debug)]
struct Task1;
struct Msg1;
struct Msg2(u32);
struct Msg3<T>(T);
struct Msg4<T>(T);

impl AcTask for Task1 {}

#[async_trait]
impl Handle<Msg1> for Task1 {
    async fn handle(&mut self, _: Msg1) {
        println!("received msg1");
    }
}

#[async_trait]
impl HandleWithResponse<Msg3<u8>, u8> for Task1 {
    async fn handle(&mut self, msg: Msg3<u8>) -> u8 {
        println!("received msg1");
        msg.0
    }
}

#[async_trait]
impl HandleWithResponse<Msg3<u32>, u32> for Task1 {
    async fn handle(&mut self, msg: Msg3<u32>) -> u32 {
        println!("received msg1");
        msg.0
    }
}

#[async_trait]
impl<T: Debug + Send + 'static> Handle<Msg4<T>> for Task1 {
    async fn handle(&mut self, msg: Msg4<T>) {
        println!("received msg {:?}", msg.0);
    }
}

#[async_trait]
impl HandleWithResponse<Msg2, u32> for Task1 {
    async fn handle(&mut self, msg: Msg2) -> u32 {
        println!("received msg1");
        msg.0
    }
}

#[tokio::test]
async fn test() {
    let mut sender = rt::create_spawn(1000, || Task1 {});

    sender.send(Msg1 {}).await.unwrap();
    sender.send(Msg1 {}).await.unwrap();

    let id = 1;
    let result = sender.send_and_receive(Msg2(id)).await.unwrap().await.unwrap();
    assert_eq!(result, id);

    let id = 1u8;
    let result = sender.send_and_receive(Msg3(id)).await.unwrap().await.unwrap();
    assert_eq!(result, id);

    let id = 1u32;
    let result = sender.send_and_receive(Msg3(id)).await.unwrap().await.unwrap();
    assert_eq!(result, id);

    let id = 1u32;
    let result = sender.send(Msg4(id)).await.unwrap();
    assert_eq!(result, ());
}
