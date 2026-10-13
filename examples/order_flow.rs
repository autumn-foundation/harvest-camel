//! Runnable order flow. Not implemented yet.

use serde_json::{Value, json};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What one run of the flow did.
#[derive(Debug)]
pub struct Outcome {
    /// The order id used for this run.
    pub order_id: String,
    /// The workflow result.
    pub result: Value,
    /// How many times the inventory route was called.
    pub inventory_calls: usize,
}

/// Run the flow against the Postgres database at `url`.
pub async fn run(_url: &str) -> Result<Outcome, BoxError> {
    Err("not implemented".into())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::var("DATABASE_URL").map_err(|_| "set DATABASE_URL to a Postgres URL")?;
    let outcome = run(&url).await?;
    println!("{}", serde_json::to_string_pretty(&outcome.result)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB_ENV: &str = "HARVEST_CAMEL_TEST_DATABASE_URL";

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn redelivered_order_is_reserved_once_and_charged() {
        let Ok(url) = std::env::var(DB_ENV) else {
            eprintln!("skipping: set {DB_ENV} to a Postgres URL to run the example");
            return;
        };
        let outcome = run(&url).await.expect("the order flow completes");
        assert_eq!(
            outcome.result,
            json!({
                "order_id": outcome.order_id,
                "reservation": {"sku": "sku-1", "reserved": 2},
                "payment": {"approved": true, "amount": 42},
            }),
        );
        assert_eq!(
            outcome.inventory_calls, 1,
            "the order is sent twice, but stock is reserved once"
        );
    }
}
