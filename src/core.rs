use std::cmp::max;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::iter::Map;
use std::net::{TcpListener, TcpStream};
use std::ops::DerefMut;
use tokio::sync:: Mutex;
use std::sync::Arc;
use std::task::ready;
use std::time::Duration;
use std::vec;
use log::{Log, log};
use serde_json::map;
use tokio::io::AsyncReadExt;



use tokio::net::windows::named_pipe::PipeMode::Message;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::time::interval;

use crate::request::{AppendEntriesRequest, AppendEntriesResponse, RaftMessageDestination, RequestVoteRequest, RequestVoteResponse, Response, Rpc, SendableRaftMessage};
use crate::request::RaftMessageDestination::Broadcast;
// use crate::request::Rpc;
use crate::request::Rpc::{VoteRequest, VoteResponse};
use crate::rpc::RpcServer;

mod core{

}
// #[derive(Clone)]
pub struct RaftNode {
    // pub node_id: u32,
    term:u32,
    //自己的port
    pub addr_port: String,

    state: LeadershipState,

    pub receive_channel_tx: Arc<Mutex<mpsc::Sender<Rpc>>>,

    receive_channel_rx: Arc<Mutex<mpsc::Receiver<Rpc>>>,

    send_channel_tx: Arc<Mutex<mpsc::Sender<Rpc>>>,

    send_channel_rx: Arc<Mutex<mpsc::Receiver<Rpc>>>,

    //其他节点的信息
    peer_addrs: Vec<Peer>,

    spin_time: u64,

    pre_log_index: u32,

    pre_log_term: u32,

    commit_index:u32,

    commit_term:u32,

    // connect_map:HashMap<String,TcpStream>,
    // connect_map:Arc<Mutex<HashMap<String,TcpStream>>>,
    connect_map:Arc<Mutex<HashMap<String,Arc<Mutex<TcpStream>>>>>,

    log:Vec<RaftLog>,

    log_record:HashMap<u32,u32>,


    //预留快照
}

struct RaftLog{
    term:u32,
    entry:Vec<u8>,
}

impl RaftLog {
    pub fn new(term:u32,entry:Vec<u8>)->Self{
        RaftLog{
            term,
            entry,
        }
    }
}
#[derive(Clone)]
pub enum LeadershipState {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}



//who is the leader
#[derive(Clone)]
pub struct FollowerState {
    //
    leader_addr: String,

    has_vote:bool,
    //一个follow
}

impl FollowerState {
    pub fn new(leader_addr:String,has_vote:bool)->Self{
        FollowerState{
            leader_addr,
            has_vote,
        }
    }
}

#[derive(Clone)]
pub struct CandidateState {
    pub vote_num: u32,
    //剩余
    pub remain_map:HashMap<String,bool>,

}

#[derive(Clone)]
pub struct LeaderState {

    log_commit_map:HashMap<u32,u32>,
    log_append_map:HashMap<String,u32>,
    term_id:u32,
    heart_time:u32,

}







impl RaftNode {
    pub fn new(port: String, state: LeadershipState, peers: Vec<Peer>, spin_time: u64) -> Self {
        let (tx, mut rx): (mpsc::Sender<Rpc>, mpsc::Receiver<Rpc>) = mpsc::channel(100);
        let (tx2, mut rx2): (mpsc::Sender<Rpc>, mpsc::Receiver<Rpc>) = mpsc::channel(100);
        let id = 1;
        let map=HashMap::new();
        let map=Arc::new(Mutex::new(map));
        // let map=Arc::new(Mutex::new(map));

        let tx=Arc::new(Mutex::new(tx));
        let tx2=Arc::new(Mutex::new(tx2));

        let rx=Arc::new(Mutex::new(rx));
        let rx2=Arc::new(Mutex::new(rx2));

        let log_record:HashMap<u32,u32>=HashMap::new();
        let log:Vec<RaftLog> =Vec::new();


        RaftNode {
            // node_id: id,
            term:0,
            connect_map:map,
            addr_port: port,
            state,
            receive_channel_tx: tx,
            receive_channel_rx: rx,
            send_channel_tx: tx2,
            send_channel_rx: rx2,
            peer_addrs: peers,
            spin_time,
            pre_log_term: 0,
            pre_log_index: 0,
            commit_term:0,
            commit_index:0,
            log,
            log_record,
        }
    }



    pub async fn start(self1 : Arc<Mutex<Self>>){
        //启动程序，开始自旋(得用异步处理)，首先把自己作为follow
        // let self_clone=self1.clone();
        let self2=self1.clone();
        tokio::spawn(async move{
            let temp_receive_channel_tx;
            let send_channel_rx;
            let connect_map;
            {
                let temp=self1.lock().await;
                temp_receive_channel_tx=temp.receive_channel_tx.clone();
                send_channel_rx=temp.send_channel_rx.clone();
                connect_map=temp.connect_map.clone();

            };
            RaftNode::listen_message(temp_receive_channel_tx, send_channel_rx,connect_map ).await;

        });

        //启动程序
        let p=RaftNode::spin(self2).await;




    }


    async fn spin(safe_self : Arc<Mutex<Self>>) {
        println!("开始执行spin");
        loop{
            let state;
            {
                let temp=safe_self.lock().await;
                state = temp.state.clone();
            }

        match state {
            LeadershipState::Follower(state) => {
                println!("作为Follower");
                let receiver;
                let mut accept;
                {
                     receiver= safe_self.lock().await.receive_channel_rx.clone();
                    accept = receiver.lock().await;
                }
                println!("抢到第一个锁");

                println!("time:{}",safe_self.lock().await.spin_time);
                tokio::select! {

                    _=tokio::time::sleep(Duration::from_secs(safe_self.lock().await.spin_time))=>{
                        //超时
                        println!("超时");
                        //超时，认为leader已经寄了，开始自旋，认为自己是candidate，发送信号

                        {
                           let mut temp=safe_self.lock().await;

                            temp.term=temp.term+1;

                            temp.state=LeadershipState::Candidate(CandidateState{
                            remain_map:{
                                let mut map:HashMap<String,bool>=HashMap::new();
                                for k in &temp.peer_addrs{
                                    map.insert(k.addr.clone(),true);
                                }
                                map
                            },
                            vote_num:1,

                            });
                        }



                        println!("设置自己为candidate");
                        //等待选举

                        // return;
                    },
                    msg=accept.recv()=>{
                        if let Some(msg1)= msg{
                            let mut temp_self=safe_self.lock().await;


                            match msg1{
                                Rpc::VoteRequest(req)=>{

                                    let resp;
                                    //判断对方能否成为自己的leader()

                                    /*

                                        1.Candidate的Term必须大于等于Follower的Term。
                                        2.Candidate需要提供的日志条目必须和Follower的日志条目相同或者更新。可以通过比较Follower的日志和Candidate的日志来判断是否满足这个条件。
                                            在Raft协议中，通常是比较最后一条日志的索引和任期号是否相同。
                                            如果Follower的最后一条日志的Term大于Candidate的Term，那么Follower会拒绝投票；
                                            如果Follower的最后一条日志的Term等于Candidate的Term，但索引值小于Candidate的索引值，那么Follower会拒绝投票。
                                     */



                                    if !state.has_vote && req.term>=temp_self.term && req.pre_log_term>temp_self.pre_log_term && (req.pre_log_term==temp_self.pre_log_term ||req.pre_log_index<temp_self.pre_log_index )
                                    {
                                         resp=RequestVoteResponse::refuse(temp_self.pre_log_term,temp_self.addr_port.clone());
                                    }else{
                                        resp=RequestVoteResponse::ok(temp_self.pre_log_term,temp_self.addr_port.clone())
                                    }

                                    temp_self.send_message(SendableRaftMessage::new(Rpc::VoteResponse(resp),RaftMessageDestination::To(Peer::new(req.candidate_id))));




                                    //处理投票业务
                                    println!("请求是投票");
                                },
                                Rpc::AppendRequest(req)=>{
                                    println!("请求是日志");

                                    /*
                                            检查请求的Term是否比当前节点的Term大。如果请求的Term小于当前节点的Term，说明Leader的日志已经过时，
                                        Follower会拒绝请求并将当前节点的Term发送给Leader。

                                            检查请求中PrevLogIndex和PrevLogTerm是否与当前节点的日志匹配。如果不匹配，
                                        说明Leader的日志和当前节点的日志存在冲突，Follower会拒绝请求并返回自己的最后一条日志的索引值和Term给Leader，
                                        以便Leader能够找到日志冲突的位置进行处理。

                                            如果请求中包含新的日志条目，那么需要将新的日志条目追加到当前节点的日志中。
                                        如果已经存在相同索引位置的日志条目，那么需要比较它们的Term。
                                        如果已经存在的日志条目的Term与新的日志条目的Term不同，那么需要删除已经存在的日志条目以及之后的所有日志条目，
                                        然后将新的日志条目追加到当前节点的日志中。

                                            更新当前节点的commitIndex。如果Leader的commitIndex大于当前节点的commitIndex，
                                        那么需要将当前节点的commitIndex更新为Leader的commitIndex和当前节点的最后一条日志索引的较小值。

                                            向Leader发送成功的响应。如果Follower成功处理了请求，
                                        那么会向Leader发送一个包含自己的最后一条日志的索引和Term的成功响应，
                                        以便Leader能够更新自己的nextIndex和matchIndex。
                                     */

                                    let resp;

                                    //1.如果请求中的term<当前节点的term,leader的日志已经过时，拒绝
                                    if req.term < temp_self.term || req.pre_log_index < temp_self.commit_index {
                                        resp=AppendEntriesResponse::refuse(temp_self.term,-1,-1,req.term,req.pre_log_index+1,temp_self.addr_port.clone());
                                        temp_self.send_message(SendableRaftMessage::new(Rpc::AppendResponse(resp),RaftMessageDestination::To(Peer::new(req.leader_addr))));
                                        //发送
                                        return;
                                    }
                                    //什么时候需要删除提交之后的所有数据呢，当你期望的index不是那个的时候，意味着term有问题
                                    //那就无法判断是否应该删除之前的所有commit对象
                                    else if req.term>temp_self.term{
                                        //只要你之前不是任他为leader，现在转为任他为leader了，直接删除提交之前的，也意味着后面只要一直往前查找知道pre_log_存在
                                        //删除对应的索引
                                        temp_self.term=req.term;
                                        let temp_index=temp_self.commit_index.clone()+1;
                                        temp_self.log.truncate((temp_index).try_into().unwrap());
                                        temp_self.pre_log_term=temp_self.commit_term;
                                        temp_self.pre_log_index=temp_self.commit_index;
                                        //接下来告诉对方自己的最新的，请求对方发送数据来完成同步
                                        resp=AppendEntriesResponse::refuse(temp_self.term,temp_self.pre_log_term.try_into().unwrap(),temp_self.pre_log_index.try_into().unwrap(),req.term,req.pre_log_index+1,temp_self.addr_port.clone());
                                        return ;
                                    }



                                    //如果是小了，那就要告诉对方自己需要索引的位置，来达到状态一致性
                                    if  req.pre_log_index != temp_self.pre_log_index || req.pre_log_term != temp_self.pre_log_term  {
                                        //如果不一致。因为在这个分支中，可以确定的是最近提交的term等于Leader上一个的term，
                                        //所以只需要在这个term中向前寻找，判断是否有一个满足的条件，即加入

                                        if   req.pre_log_index > temp_self.pre_log_index {
                                            //让他发下一个得了
                                            resp=AppendEntriesResponse::refuse(temp_self.term,temp_self.pre_log_term.try_into().unwrap(),temp_self.pre_log_index.try_into().unwrap(),req.term,req.pre_log_index+1,temp_self.addr_port.clone());
                                            temp_self.send_message(SendableRaftMessage::new(Rpc::AppendResponse(resp),RaftMessageDestination::To(Peer::new(req.leader_addr))));
                                        //发送
                                            return ;

                                        }

                                        if req.pre_log_index <= temp_self.pre_log_index{
                                            //应当清空这个任期所有的数据
                                            //直接清空到commit
                                            let temp_index=temp_self.commit_index+1;
                                            temp_self.log.truncate((temp_index).try_into().unwrap());
                                            temp_self.pre_log_term=temp_self.commit_term;
                                            temp_self.pre_log_index=temp_self.commit_index;




                                            //然后同样返回
                                             resp=AppendEntriesResponse::refuse(temp_self.term,temp_self.pre_log_term.try_into().unwrap(),temp_self.pre_log_index.try_into().unwrap(),req.term,req.pre_log_index+1,temp_self.addr_port.clone());
                                            temp_self.send_message(SendableRaftMessage::new(Rpc::AppendResponse(resp),RaftMessageDestination::To(Peer::new(req.leader_addr))));


                                        }

                                    }

                                    else{
                                        //根据commit来提交
                                        temp_self.commit_term=req.term;
                                        temp_self.commit_index=req.leader_commit;
                                        temp_self.log.push(RaftLog::new(req.term,req.entries));
                                        resp=AppendEntriesResponse::ok(temp_self.pre_log_term,req.term,req.pre_log_index,temp_self.addr_port.clone());
                                        temp_self.send_message(SendableRaftMessage::new(Rpc::AppendResponse(resp),RaftMessageDestination::To(Peer::new(req.leader_addr))));
                                        //
                                    }






                                    //处理追加日志业务
                                }
                                _=>{
                                    println!("出问题");
                                }



                            }

                        }
                    },


                }
            },
            LeadershipState::Candidate(mut candidate) => {


                println!("作为candidate");

                //TODO 遇到问题，需要传入的闭包是一个可被重复使用的，但是用着只读的方式传入没有这样的效果
                let view_map=candidate.remain_map.clone();
                let callback =move |this: &mut Self| {

                    // let view_map2=view_map.clone();
                    for (k,v) in &view_map{
                        let req = RequestVoteRequest {
                            term: this.pre_log_term,
                            candidate_id: this.addr_port.clone(),
                            pre_log_index: this.pre_log_index,
                            pre_log_term: this.pre_log_term,
                        };
                         let msg = SendableRaftMessage::new(Rpc::VoteRequest(req), RaftMessageDestination::Broadcast);
                        this.send_message(msg);
                    }

                    //等待响应
                    //发送



                    println!("再次发送请求申请");
                    // break;
                };
                // self.heart_timer(3, callback).await;

                let mut clone_self=safe_self.clone();
                let handle = tokio::spawn(async move{
                    RaftNode::heart_timer(clone_self,4,callback).await;
                });

                //阻塞的接收数据，不应该是这样的

                let mut temp_channel;
                {
                    //先拿到safe_self锁权的使用
                    let temp=safe_self.lock().await;
                    //拿到锁后，获取里面的变量receive_channel_rx

                    temp_channel=temp.receive_channel_rx.clone();
                    // temp_channel=temp.receive_channel_rx.lock().await;

                }




                let mut wait=temp_channel.lock().await;

                // let arc = safe_self.lock().await.receive_channel_rx.clone();
                // let mut mutex = arc.lock().await;

                let mut msg = wait.recv().await;

                if let Some(msg1) = msg {

                    let mut temp_self=safe_self.lock().await;
                    match msg1 {

                        Rpc::VoteResponse(response) => {
                            //收到别人的回复,观察是否同意投你


                            if response.vote_granted {
                                candidate.vote_num += 1;
                                candidate.remain_map.remove(&response.addr);
                                //如果收到的票数
                                let mut size=temp_self.peer_addrs.len() as u32;

                                if candidate.vote_num > (size + 1) / 2 {
                                    //变为leader
                                    println!("{}变为leader", temp_self.addr_port);
                                    temp_self.state = LeadershipState::Leader(LeaderState {
                                        log_append_map:HashMap::new(),
                                        log_commit_map: HashMap::new(),
                                        heart_time: 3,
                                        term_id: temp_self.pre_log_index,
                                    });
                                    handle.abort();
                                    return ;

                                }
                            }

                            if response.term > temp_self.term {
                                temp_self.term=temp_self.term+1;
                                FollowerState::new(String::new(),false);
                                handle.abort();
                            }
                        },
                        Rpc::VoteRequest(req) => {
                            //收到别人的投票了，返回拒绝
                            let resp=RequestVoteResponse::refuse(temp_self.term,temp_self.addr_port.clone());
                            let r = Rpc::VoteResponse(resp);
                            temp_self.send_message(SendableRaftMessage::new(r, RaftMessageDestination::To(Peer::new(req.candidate_id))));
                        },
                        //收到来自新leader的心跳
                        Rpc::AppendRequest(req) => {
                            //如果心跳的任期号>=自己的任期号，同意它成为leader

                            if req.term >= temp_self.pre_log_term {
                                temp_self.state = LeadershipState::Follower(FollowerState {
                                    //在candidate状态，切换
                                    has_vote: true,
                                    leader_addr: req.leader_addr.clone(),
                                });
                                //关闭定时器
                                handle.abort();
                                temp_self.pre_log_term = req.term;


                                let resp = RequestVoteResponse::ok(temp_self.pre_log_term,temp_self.addr_port.clone());
                                let r = Rpc::VoteResponse(resp);
                                let msg = SendableRaftMessage::new(r, RaftMessageDestination::To(Peer::new(req.leader_addr.clone())));
                            } else {

                                let resp = RequestVoteResponse::refuse(temp_self.pre_log_term,temp_self.addr_port.clone());
                                let r = Rpc::VoteResponse(resp);
                                let msg = SendableRaftMessage::new(r, RaftMessageDestination::To(Peer::new(req.leader_addr.clone())));
                            }
                        },
                        _ => {}
                    }
                }
            }
            //todo 心跳包包含内容是这个节点最新需要的日志
            LeadershipState::Leader(mut leader) => {


                let safe_self2=safe_self.clone();
                //todo todo todo
                //如果闭包参数是mutex的问题是，闭包不是异步的
                //如果参数是self的话问题是,参数传递不进去
                let handle = tokio::spawn(async move {
                    RaftNode::heart_timer(safe_self2,leader.heart_time as u64,|this:&mut Self | {
                        //发送心跳包
                        let req;
                        let msg;
                        {
                            req= AppendEntriesRequest::new(leader.term_id, String::from(&this.addr_port), this.pre_log_index, this.pre_log_term, Vec::new(), this.commit_index);
                            msg= SendableRaftMessage::new(Rpc::AppendRequest(req), Broadcast);
                        }


                        this.send_message(msg);
                    }).await;
                });



                    loop {
                        let mut msg;
                        {

                            let mut temp_self=safe_self.lock().await;
                            msg=temp_self.receive_channel_rx.lock().await.recv().await;
                        }

                        if let Some(msg1) = msg {
                            let mut temp_self=safe_self.lock().await;
                            //几种可能，当自己为leader的时候，可能收到追加日志的响应，也可能收到别人想要成为的申请
                            match msg1 {
                                //如果是VoteRequest
                                Rpc::VoteRequest(req) => {
                                    // let mut resp = RequestVoteResponse {
                                    //     term: max(safe_self.lock().await.pre_log_term, req.pre_log_term),
                                    //     vote_granted: false,
                                    // };
                                    let mut resp=RequestVoteResponse::refuse(max(temp_self.pre_log_term, req.pre_log_term),temp_self.addr_port.clone());


                                    //判断他是否有成为leader的条件
                                    if req.term > temp_self.term &&
                                        (req.pre_log_term >temp_self.pre_log_term || req.pre_log_term==temp_self.pre_log_term && req.pre_log_index==temp_self.pre_log_index)
                                        {
                                        resp.vote_granted = true;
                                        temp_self.state = LeadershipState::Follower(FollowerState {
                                            has_vote:true,
                                            leader_addr: req.candidate_id.clone(),
                                        });
                                        break;
                                        //leader的下一个状态一定是follow
                                    }

                                    let r = Rpc::VoteResponse(resp);
                                    let send_msg = SendableRaftMessage::new(r, RaftMessageDestination::To(Peer::new(req.candidate_id.clone())));
                                },
                                Rpc::AppendResponse(resp) => {
                                    if resp.success {


                                        //1.map+1
                                        let p = leader.log_commit_map.get(&resp.req_index);
                                        match p {
                                            None => {
                                                 let p = leader.log_commit_map.get(&resp.req_index);
                                            },
                                            Some(p) => {
                                                println!("map:k-{},v-{}", resp.term, p + 1);
                                                leader.log_commit_map.insert(resp.term, p + 1);
                                            }
                                        }


                                    }
                                    else{
                                        //首先判断
                                        if resp.term>temp_self.term{
                                            temp_self.state = LeadershipState::Follower(FollowerState {
                                                has_vote:false,
                                                leader_addr: String::new(),
                                            });
                                            temp_self.term=resp.term;
                                            //切换状态
                                            break;
                                        }

                                        leader.log_append_map.insert(resp.addr, resp.conflict_index as u32);


                                    }
                                }
                                _ => {}
                                //
                            }
                        }
                    }




            }
        }
    }
    }

    //注意，这个方法里的回调不应当再使用线程了
    pub async fn heart_timer<F>(wrap_self:Arc<Mutex<Self>>,time:u64,invoke:F)
        where F:Fn(& mut Self){


        let mut inter =interval(Duration::from_secs(time));
        loop{
            inter.tick().await;
            {
                let mut temp= wrap_self.lock().await;
                let temp2=temp.deref_mut();
                invoke(temp2);
            }




        }
        // let s=tokio::time::sleep().await;

        // invoke(self);


    }


    // async fn apply_be_leader(&mut self) {
    //     //想要成为候选人，向所有节点发送广播
    //
    //
    //     let req = RequestVoteRequest {
    //         term: self.pre_log_term + 1,
    //         candidate_id: self.addr_port.clone(),
    //         pre_log_index: self.pre_log_index,
    //         pre_log_term: self.pre_log_term,
    //
    //     };
    //     let msg = SendableRaftMessage::new(Rpc::VoteRequest(req), Broadcast);
    //
    //     self.send_message(msg).await;
    // }


    //根据sendableRaftMessage去发送数据
    async fn send_message(&mut self, request: SendableRaftMessage) {

        //用到的只有map和peer addr吗
        // let map;
        //
        // {
        //     let temp_self=wrap_self.lock().await;
        //     map=temp_self.connect_map.clone();
        //     // peer_addr=temp_self.peer_addrs;
        // }
        let map=self.connect_map.lock().await;
        match request.dest {
            RaftMessageDestination::Broadcast => {
                //广播
                for peer in &self.peer_addrs {
                    //发送数据
                    // peer

                    let mut tcpStream=map.get(peer.addr.as_str()).unwrap();
                    let msg=serde_json::to_vec(&request.message).unwrap();
                    let tes=tcpStream.lock().await.write(&msg);
                    println!("向peer{}发送数据", peer.addr);
                }
            },
            RaftMessageDestination::To(peer) => {
                //单点发送
                //发送数据

                let mut wrap_tcpStream=map.get(peer.addr.as_str()).unwrap();
                let msg=serde_json::to_vec(&request.message).unwrap();
                let res=wrap_tcpStream.lock().await.write(&msg);
                println!("向peer{}发送数据", peer.addr);

            }
        }
    }

    async fn listen_message(receive_channel_tx:Arc<Mutex<mpsc::Sender<Rpc>>>,
                            send_channel_rx: Arc<Mutex<mpsc::Receiver<Rpc>>>,
                            connect_map:Arc<Mutex<HashMap<String,Arc<Mutex<TcpStream>>>>>) {

        //执行监听任务
        println!("执行监听任务");
        let mut guard=connect_map.lock().await;
        for (k, v) in guard.iter(){

            let tx_copy=receive_channel_tx.clone();
            let stream_copy=Arc::clone(v);
            tokio::spawn(async move{
                let mut buf = [0; 1024];
                loop{
                    match stream_copy.lock().await.read(&mut buf) {
                        Ok(0)=>{
                            println!("读入的数据个数为0");
                            //尝试重新连接？
                            break;
                        },
                        Ok(n)=>{
                            let data=&buf[..n];
                            //把数据用protobuf转为普通函数
                            let msg:Rpc=serde_json::from_slice(&data).expect("文件出错");
                            tx_copy.lock().await.send(msg);
                            println!("读取到数据了，发送数据到队列");
                        }
                        Err(e)=>{
                            //
                            println!("连接异常");
                        }
                    }
                }
            });


        }


        //再开几个线程去监听队列，去发送



    }

    //再开几个线程去监听队列，去发送



    }









pub struct Peer {
    pub addr: String,
}

impl Peer {
    pub fn  new(addr:String)->Self{
        Peer{
            addr,
        }
    }
}



