![Tests](https://github.com/benyakirten/not-redis/actions/workflows/test.yml/badge.svg)
![Security Audit](https://github.com/benyakirten/not-redis/actions/workflows/audit.yml/badge.svg)

## What Am I Looking At?

The repo started based off the Codecrafters' ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis). After completing it, I have decided to continue it and build some additional parts.

## Why

I wanted to get more experience with Tokio and concurrency. I want to try out various concurrency models such as lockless (like [SkipDB](https://github.com/al8n/skipdb)) and such.

## How do I run this

```sh
$ cargo run
```

You can dockerize it if you want. I don't know why when you have a lot better solutions like actual Redis or [KeyDB](https://github.com/Snapchat/KeyDB).
