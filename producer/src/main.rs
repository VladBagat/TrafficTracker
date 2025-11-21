use std::thread::spawn;
use std::time::Duration;
use axum::extract::State;
use axum::http::StatusCode;
use log::info;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use axum::{routing::get, Router};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use std::net::SocketAddr;
use tokio::net::TcpListener;

enum Commands {
    GetStatus {
        respond_to: oneshot::Sender<EngineSnapshot>
    },
    SetSpeed {
        interval: f32
    },
    TestProduce
}

struct EngineActor {
    reciever: mpsc::Receiver<Commands>,
    interval: f32
}

struct EngineSnapshot {
    interval: f32
}

#[derive(Clone)]
struct AxumState {
    engine_sender: Sender<Commands>
}


impl EngineActor {
    async fn run(mut self) {
        while let Some(msg) = self.reciever.recv().await {
            match msg {
                Commands::SetSpeed { interval } => {
                    info!("Changed interval to {}", interval);
                    self.interval = interval;
                },
                Commands::GetStatus { respond_to } => {
                    info!("Processed status request");
                    let _ = respond_to.send(EngineSnapshot{ interval: self.interval} );
                },
                Commands::TestProduce => {
                    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".to_string());
                    let topic = "test";
                    info!("Connecting to {}", brokers);
                    let _ = produce(&brokers, topic).await;
                }
            }
        }
    }
}

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

    let (tx, rx) = mpsc::channel(128);
    let actor = EngineActor{ reciever: rx, interval: 0f32 };
    tokio::spawn(actor.run());

    let state = AxumState {
        engine_sender: tx
    };

    let app = Router::new()
        .route("/send-test-messages", get(send_test_messages))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await?;

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn send_test_messages(State(state):State<AxumState>) -> StatusCode {
    let _ = state.engine_sender.send(Commands::TestProduce).await;
    StatusCode::ACCEPTED
}