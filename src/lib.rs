

/*

    设计思路：做大可插拔的raft框架，即可嵌入任何 数据库对数据进行包裹，只需要实现对应的接口即可
            以raft的方式完成

 */

pub mod core;
pub mod rpc;
pub mod request;

