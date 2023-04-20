use std::sync::Arc;
use std::thread::Thread;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::Mutex;
use my_raft::core::{CandidateState, FollowerState, LeadershipState, Peer, RaftNode};
use my_raft::request::{RequestVoteRequest, Rpc};
// use request::RequestVoteRequest;

#[tokio::main]
async fn main(){
    println!("xx");

   async{
       test2();
   };
   //
    test1().await;
    let mut y: Vec<Peer> = Vec::new();
    y.push(Peer { addr: String::from("a1") });
    y.push(Peer { addr: String::from("b2") });
    y.push(Peer { addr: String::from("c3") });
    let mut node = RaftNode::new(String::from("127.0.0.1:81"), LeadershipState::Follower(FollowerState::new(String::new(),false)), y, 10);
    let wrap_node=Arc::new(Mutex::new(node));


    let handle = tokio::spawn(async move {
        RaftNode::heart_timer(wrap_node,4,|node|{
            println!("定时器触发");
        }).await;
    });




    // select!{
    //    _=test1()=>{
    //         println!("完成");
    //     },
    //     res2=async{
    //         test2();
    //
    //
    //      }=>res2,
    //
    // }
    //
    //
    // std::thread::sleep(Duration::from_secs(10));



}
#[test]

pub fn test33(){
    let mut a=2;
    {
        a=a+1;
    }
    println!("{}",a);
}

async fn test2(){
    std::thread::sleep(Duration::from_secs(10));
    println!("执行test2");
    loop{
        std::thread::sleep(Duration::from_secs(3));
    }
}



async fn test1(){
    println!("执行test1");
    let mut y: Vec<Peer> = Vec::new();
    y.push(Peer { addr: String::from("a1") });
    y.push(Peer { addr: String::from("b2") });
    y.push(Peer { addr: String::from("c3") });
    let mut node = RaftNode::new(String::from("127.0.0.1:81"), LeadershipState::Follower(FollowerState::new(String::new(),false)), y, 10);
    let a=Arc::new(Mutex::new(node));
    let q = RaftNode::start(a).await;


}
// async fn test(){
//     println!("执行test1");
//     let mut y: Vec<Peer> = Vec::new();
//     y.push(Peer { addr: String::from("a1") });
//     y.push(Peer { addr: String::from("b2") });
//     y.push(Peer { addr: String::from("c3") });
//     let mut node = RaftNode::new(String::from("127.0.0.1:81"), LeadershipState::Follower(FollowerState::new(String::new(),false)), y, 10);
//
//     let q = node.start().await;
//
//
// }
