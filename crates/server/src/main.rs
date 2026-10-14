//! Thin binary: parse arguments, hand control to [`server::run`].

use clap::Parser;

#[tokio::main]
async fn main() {
    server::run(server::Args::parse()).await;
}
