use std::net::{IpAddr, Ipv4Addr};

use tokio::sync::watch;

use fibril_storage::{Partition, Topic};

#[async_trait::async_trait]
pub trait Coordination: Send + Sync {
    async fn is_leader(&self, topic: Topic, partition: Partition) -> bool;
    #[deprecated]
    async fn await_leadership(&self, topic: Topic, partition: Partition);
    fn node_id(&self) -> &str;
    async fn watch_leadership(&self, topic: Topic, partition: Partition) -> LeadershipStream;
    async fn leader_for(&self, topic: Topic, partition: Partition) -> Option<NodeInfo>;

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
    async fn is_leader(&self, _: Topic, _: Partition) -> bool {
        true
    }

    async fn await_leadership(&self, _: Topic, _: Partition) {
        // no-op
    }

    fn node_id(&self) -> &str {
        "local"
    }

    async fn leader_for(&self, _: Topic, _: Partition) -> Option<NodeInfo> {
        Some(NodeInfo {
            node_id: self.node_id().to_string(),
            address: IpAddr::V4(Ipv4Addr::from_octets([127, 0, 0, 1])),
        })
    }

    async fn watch_leadership(&self, _: Topic, _: Partition) -> watch::Receiver<LeadershipEvent> {
        let (tx, rx) = watch::channel(LeadershipEvent::Gained);
        drop(tx); // never changes
        rx
    }
}
