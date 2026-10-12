# vetis-ws

[![Crates.io downloads](https://img.shields.io/crates/d/vetis-ws)](https://crates.io/crates/vetis-ws) [![crates.io](https://img.shields.io/crates/v/vetis-ws?style=flat-square)](https://crates.io/crates/vetis-ws) [![Build Status](https://github.com/vetis-server/vetis-ws/actions/workflows/rust.yml/badge.svg?event=push)](https://github.com/vetis-server/vetis-ws/actions/workflows/rust.yml) ![Crates.io MSRV](https://img.shields.io/crates/msrv/vetis-ws) [![Documentation](https://docs.rs/vetis-ws/badge.svg)](https://docs.rs/vetis-ws/latest/vetis-ws) [![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/vetis-server/vetis-ws/blob/main/LICENSE.md)  [![codecov](https://codecov.io/gh/vetis-server/vetis-ws/graph/badge.svg?token=T0HSBAPVSI)](https://codecov.io/gh/vetis-server/vetis-ws)

vetis-ws adds websockets support to your vetis server!

## Features

- Protocol version 13
- WAMP (planned)
- STOMP (planned)
- MQTT (planned)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
vetis-ws = "0.1.0"
```

## Configuration

Below you can find instruction on how to configure many logging options availble
on vetis.

### stdout

#### Using API

```rust,ignore

```

#### vetis.yaml

There's no plans to provide this kind of configuration method.

#### Configuration Options

- **log_level**: Log level: TRACE, WARN, DEBUG, INFO, ERROR and FATAL

### Runtime Features

Choose the appropriate runtime feature for your project:

- `runtime-tokio` (default): Use with Tokio runtime

## License

Licensed under either of

- Apache License, Version 2.0
  (LICENSE-APACHE or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license
  (LICENSE-MIT or <https://opensource.org/licenses/MIT>)

at your option.

## Author

Rogerio Pereira Araujo <rogerio.araujo@gmail.com>
