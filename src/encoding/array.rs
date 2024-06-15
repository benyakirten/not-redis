use crate::data;

fn encode_array_length(size: usize) -> String {
    format!("*{}\r\n", size)
}

fn encode_array_item(item: &str) -> String {
    format!("${}\r\n{}\r\n", item.len(), item)
}

pub fn encode_array(input: &[&str]) -> String {
    let mut result = encode_array_length(input.len());

    for item in input {
        result.push_str(&encode_array_item(item));
    }

    result
}

pub fn encode_stream(stream: &[&data::InnerRedisStream]) -> String {
    let mut output = encode_array_length(stream.len());

    for inner in stream.iter() {
        let stream_id = inner.stream_id();
        output.push_str(&encode_array_length(2));
        output.push_str(&encode_array_item(&stream_id));

        let mut stream_items: Vec<&str> = vec![];
        for item in inner.items.iter() {
            stream_items.push(item.key.as_str());
            stream_items.push(item.value.as_str());
        }

        let encoded = encode_array(stream_items.as_slice());
        output.push_str(&encoded)
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data;

    #[test]
    fn test_encode_stream() {
        let mut items_1 = vec![];
        items_1.push(data::RedisStreamItem::new(
            "foo".to_string(),
            "bar".to_string(),
        ));
        items_1.push(data::RedisStreamItem::new(
            "bat".to_string(),
            "baz".to_string(),
        ));
        let inner_1: data::InnerRedisStream = data::InnerRedisStream {
            items: items_1,
            ms_time: 100,
            sequence_number: 200,
        };

        let mut items_2 = vec![];
        items_2.push(data::RedisStreamItem::new(
            "one".to_string(),
            "two".to_string(),
        ));
        let inner_2: data::InnerRedisStream = data::InnerRedisStream {
            items: items_2,
            ms_time: 2000,
            sequence_number: 200,
        };

        let mut items_3 = vec![];
        items_3.push(data::RedisStreamItem::new(
            "three".to_string(),
            "four".to_string(),
        ));
        items_3.push(data::RedisStreamItem::new(
            "five".to_string(),
            "six".to_string(),
        ));
        items_3.push(data::RedisStreamItem::new(
            "seven".to_string(),
            "eight".to_string(),
        ));
        items_3.push(data::RedisStreamItem::new(
            "nine".to_string(),
            "ten".to_string(),
        ));
        let inner_3: data::InnerRedisStream = data::InnerRedisStream {
            items: items_3,
            ms_time: 2000,
            sequence_number: 30000,
        };

        let stream = vec![&inner_1, &inner_2, &inner_3];
        let got = encode_stream(stream.as_slice());

        let got_items: Vec<&str> = got.split("\r\n").collect();
        let want_items: Vec<&str> = vec![
            "*3",
            "*2",
            "$7",
            "100-200",
            "*4",
            "$3",
            "foo",
            "$3",
            "bar",
            "$3",
            "bat",
            "$3",
            "baz",
            "*2",
            "$8",
            "2000-200",
            "*2",
            "$3",
            "one",
            "$3",
            "two",
            "*2",
            "$10",
            "2000-30000",
            "*8",
            "$5",
            "three",
            "$4",
            "four",
            "$4",
            "five",
            "$3",
            "six",
            "$5",
            "seven",
            "$5",
            "eight",
            "$4",
            "nine",
            "$3",
            "ten",
            "",
        ];

        assert_eq!(got_items, want_items);
    }
}

pub fn encode_streams(read_streams: Vec<data::ReadStreamItem>) -> String {
    let mut output = encode_array_length(read_streams.len());

    for item in read_streams.iter() {
        output.push_str(&encode_array_length(2));
        output.push_str(&encode_array_item(&item.key));

        let stream_encode = encode_stream(&item.streams);
        output.push_str(&stream_encode);
    }

    output
}
