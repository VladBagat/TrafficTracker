use std::time::Duration;
use axum::http::StatusCode;
use log::info;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use axum::{routing::get, Router};
use std::net::SocketAddr;
use tokio::net::TcpListener;

// The produce function stays exactly the same
async fn produce(brokers: &str, topic_name: &str) -> anyhow::Result<()> {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let futures = (0..5)
        .map(|i| async move {
            let delivery_status = producer
                .send(
                    FutureRecord::to(topic_name)
                        .payload(&format!("Message {}", i))
                        .key(&format!("Key {}", i))
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header_key",
                            value: Some("header_value"),
                        })),
                    Duration::from_secs(0),
                )
                .await;

            info!("Delivery status for message {} received", i);
            delivery_status
        })
        .collect::<Vec<_>>();

    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init(); 

    let app = Router::new()
        .route("/send-test-messages", get(send_test_messages));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await?;

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn send_test_messages() -> StatusCode {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".to_string());
    let topic = "test";

    info!("Connecting to {}", brokers);

    if let Err(e) = produce(&brokers, topic).await {
        eprintln!("Kafka error: {:?}", e);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::OK
}