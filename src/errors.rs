pub fn wrong_type() -> anyhow::Error {
    anyhow::anyhow!("WRONGTYPE Operation against a key holding the wrong kind of value")
}
