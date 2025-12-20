use std::net::{IpAddr, Ipv4Addr};

use tokio::sync::watch;

use fibril_storage::{LogId, Topic};

#[async_trait::async_trait]
pub trait Coordination: Send + Sync {
    async fn is_leader(&self, topic: Topic, partition: LogId) -> bool;
    #[deprecated]
    async fn await_leadership(&self, topic: Topic, partition: LogId);
    fn node_id(&self) -> &str;
    async fn watch_leadership(&self, topic: Topic, partition: LogId) -> LeadershipStream;
    async fn leader_for(&self, topic: Topic, partition: LogId) -> Option<NodeInfo>;

    // stub: no-op for single node now
}

pub type LeadershipStream = watch::Receiver<LeadershipEvent>;

pub enum LeadershipEvent {
    Gained,
    Lost,
}

pub struct NodeInfo {
    pub node_id: String,
    pub address: IpAddr, // tcp / http
}

#[derive(Debug, Clone)]
pub struct NoopCoordination;

#[async_trait::async_trait]
impl Coordination for NoopCoordination {
    async fn is_leader(&self, _: Topic, _: LogId) -> bool {
        true
    }

    async fn await_leadership(&self, _: Topic, _: LogId) {
        // no-op
    }

    fn node_id(&self) -> &str {
        "local"
    }

    async fn leader_for(&self, _: Topic, _: LogId) -> Option<NodeInfo> {
        Some(NodeInfo {
            node_id: self.node_id().to_string(),
            address: IpAddr::V4(Ipv4Addr::from_octets([127, 0, 0, 1])),
        })
    }

    async fn watch_leadership(&self, _: Topic, _: LogId) -> watch::Receiver<LeadershipEvent> {
        let (tx, rx) = watch::channel(LeadershipEvent::Gained);
        drop(tx); // never changes
        rx
    }
}
