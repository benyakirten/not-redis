pub fn wrong_type() -> anyhow::Error {
    anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value")
}

pub fn not_an_integer() -> anyhow::Error {
    anyhow::anyhow!("ERR value is not an integer or out of range")
}
