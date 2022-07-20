pub type Topic = String;
pub type Partition = i32;
pub type Offset = i64;

#[derive(Eq, PartialEq, Hash)]
pub struct TopicManagementKey {
    pub topic: Topic,
    pub partition: Partition,
}

macro_rules! key {
    ($msg: expr) => {
        $crate::kafka::key::TopicManagementKey {
            topic: $msg.topic().to_string(),
            partition: $msg.partition(),
        }
    };
}

pub(crate) use key;
