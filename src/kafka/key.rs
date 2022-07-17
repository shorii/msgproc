#[derive(Eq, PartialEq, Hash)]
pub struct TopicManagementKey {
    pub topic: String,
    pub partition: i32,
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
