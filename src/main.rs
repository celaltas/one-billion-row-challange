#[tokio::main]
async fn main() {
    if let Err(e) = one_billion_row_challange::run().await {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
