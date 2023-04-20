
use serde::{Serialize,Deserialize};
use crate::core::Peer;
use crate::request;

pub struct SendableRaftMessage{
    //todo 修改为正确的类型
    pub message: Rpc,

    pub dest: RaftMessageDestination,
}


impl SendableRaftMessage{
    pub fn new(req: Rpc, des: RaftMessageDestination) ->Self{
        SendableRaftMessage{
            message: req,
            dest: des,
        }
    }
}




pub enum RaftMessageDestination {
    /// The associated message should be sent to all known peers.
    Broadcast,
    /// The associated message should be sent to one particular peer.
    To(Peer),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVoteRequest{
    pub term:u32,  //当前任期号
    pub candidate_id:String ,//自己的id
    pub pre_log_index:u32 ,// 自己的最后一个日志号
    pub pre_log_term:u32,// 自己最后一个日志的任期
}
#[derive(Serialize, Deserialize, Debug)]
pub enum Rpc {

    VoteRequest(RequestVoteRequest),


    VoteResponse(RequestVoteResponse),


    AppendRequest(AppendEntriesRequest),

    /// A response to an [`AppendRequest`] allowing or denying an append to the Raft node's log.

    AppendResponse(AppendEntriesResponse),
}


pub struct Response<Data>{
    data:Data,
    url:String,
}



#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVoteResponse{
    pub addr:String,
    pub term:u32,
    pub vote_granted:bool,
}
impl RequestVoteResponse{
    pub fn ok(term:u32,addr:String)->Self{
        RequestVoteResponse{
            addr,
            term,
            vote_granted:true,
        }
    }

    pub fn refuse(term:u32,addr:String)->Self{
        RequestVoteResponse{
            addr,
            term,
            vote_granted:false,
        }
    }
}

/*
    追加日志请求
 */
#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesRequest{
    pub term: u32,

    pub leader_addr : String,

    pub pre_log_index: u32,

    pub pre_log_term: u32,

    pub entries: Vec<u8>,

    pub leader_commit: u32,
}

impl AppendEntriesRequest {
    pub fn new(term:u32,leader_addr:String,pre_log_index:u32,pre_log_term:u32,entries:Vec<u8>,leader_commit:u32)->Self{
        AppendEntriesRequest{
            term,
            leader_addr,
            pre_log_index,
            pre_log_term,
            entries,
            leader_commit,
        }
    }
}

/*
    追加日志响应
 */
#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesResponse{



    pub term:u32,

    pub success:bool,

    pub addr:String,

    pub req_term:u32,

    pub req_index:u32,

    pub conflict_term:i32,

    pub conflict_index:i32,

}

impl AppendEntriesResponse{
    pub fn ok(term:u32,req_term:u32,req_index:u32,addr:String)->Self{
        AppendEntriesResponse{
            addr,
            term,
            req_term,
            req_index,
            success:true,
            conflict_term:-1,
            conflict_index:-1,
        }
    }

    pub fn refuse(term:u32,conflict_term:i32,conflict_index:i32,req_term:u32,req_index:u32,addr:String)->Self{
        AppendEntriesResponse{
            addr,
            term,
            req_term,
            req_index,
            success:false,
            conflict_term,
            conflict_index,
        }
    }
}




