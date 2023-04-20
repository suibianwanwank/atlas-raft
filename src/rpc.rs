use std::io::Write;
use std::io::prelude::*;
use std::net::TcpStream;
use std::time::{Duration, Instant};
// use log;
use crate::request::{AppendEntriesRequest, AppendEntriesResponse, Rpc};
use core;
use crate::core::{RaftNode};


struct RpcClient {

}

impl RpcServer {
    // fn send(req:Request,timeout:u32,temp:T)->Response<T> {
    //     //优化 改为异步
    //
    //     //rpc 发送消息
    // }
   pub fn new(config:RaftNode)->RpcServer{
        RpcServer{config}
    }
   //构造

    /*
        定期发送心跳，接受信号
     */
    pub fn run(&self)->std::io::Result<()>{
        println!("执行run方法");
        let mut stream=TcpStream::connect("127.0.0.1:8080").expect("连接端口错误");
        let mut start=Instant::now();
        let mut i=0;
        loop {
            //定时，五秒执行一次
            if start.elapsed()>=Duration::from_secs(5){
                // 发送心跳


            }

        }

    }


}


pub struct RpcServer{
    config:RaftNode,
}
impl RpcClient{

    // const :u16,
    //1.判断是什么数据格式，例如心跳包，选举，
    fn receive(req: Rpc){
        //判断是哪一种消息
    }
}

