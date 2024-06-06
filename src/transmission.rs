use crate::data::RedisStreamItem;

#[derive(Clone, Debug)]
pub enum Transmission {
    Xadd(XAddTransmission),
    #[allow(dead_code)]
    Unknown,
}

#[derive(Clone, Debug)]
pub struct XAddTransmission {
    pub key: String,
    pub ms_time: u128,
    pub sequence_number: usize,
    pub data: Vec<RedisStreamItem>,
}
