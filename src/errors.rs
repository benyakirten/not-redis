pub fn wrong_type_str<'a>() -> &'a str {
    "WRONGTYPE Operation against a key holding the wrong kind of value"
}

pub fn wrong_type() -> anyhow::Error {
    anyhow::anyhow!(wrong_type_str())
}

pub fn not_an_integer() -> anyhow::Error {
    anyhow::anyhow!("ERR value is not an integer or out of range")
}
