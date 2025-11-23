use std::time::Duration;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use log::info;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use axum::Router;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures::stream::{SplitSink, SplitStream, StreamExt};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};

enum Commands {
    GetStatus {
        respond_to: oneshot::Sender<EngineState>
    },
    Pause {
        respond_to: oneshot::Sender<bool>
    },
    Resume {
        respond_to: oneshot::Sender<bool>
    }
}

struct EngineActor {
    command_reciever: mpsc::Receiver<Commands>,
    ws_write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ws_read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    state: EngineState
}

#[derive(Clone, Debug)]
struct EngineState {
    paused: bool,
}

#[derive(Clone)]
struct AxumState {
    engine_sender: Sender<Commands>
}


impl EngineActor {
    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(Ok(msg)) = self.ws_read.next() => {
                    if self.state.paused { continue; }
                    self.process_message(msg).await;
                }
                Some(msg) = self.command_reciever.recv() => {
                    match msg {
                        Commands::GetStatus { respond_to } => {
                            info!("Processed status request");
                            let _ = respond_to.send(self.state.clone());
                        },
                        Commands::Pause { respond_to } => {
                            if self.state.paused {
                                let _ = respond_to.send(false);
                            } else {
                                self.state.paused = true;
                                let _ = respond_to.send(true);
                            }
                        },
                        Commands::Resume { respond_to } => {
                            if !self.state.paused {
                                let _ = respond_to.send(false);
                            } else {
                                self.state.paused = false;
                                let _ = respond_to.send(true);
                            }
                        }
                        /*Commands::TestProduce => {
                            let brokers = std::env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".to_string());
                            let topic = "test";
                            info!("Connecting to {}", brokers);
                            let _ = produce(&brokers, topic).await;
                        }*/
                    }
                }
            }
        }
    }
    async fn process_message(&mut self, msg: Message) {
        info!("{}", msg.to_text().unwrap());
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init(); 
    info!("Producer started");

    let (tx, rx) = mpsc::channel(128);

    let force_order_url = "wss://fstream.binance.com/ws/!forceOrder@arr";
    let (ws_stream, _) = connect_async(force_order_url).await?;
    let (write, read) = ws_stream.split();

    info!("Producer connected to Binance Futures");

    let actor = EngineActor{ 
        command_reciever: rx,
        ws_write: write,
        ws_read: read,
        state: EngineState { paused: true }
    };
    tokio::spawn(actor.run());

    let state = AxumState {
        engine_sender: tx
    };

    let app = Router::new()
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await?;

    info!("Producer Axum is ready to be served");

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn pause(State(state):State<AxumState>) -> StatusCode {
    let (tx, rx) = oneshot::channel();
    match state.engine_sender.send(Commands::Pause { respond_to: tx }).await {
        Ok(_) => {
            match rx.await {
                Ok(true) => StatusCode::ACCEPTED,     
                Ok(false) => StatusCode::NOT_MODIFIED, 
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        },
        _ => StatusCode::NO_CONTENT,
    }     
}
async fn resume(State(state):State<AxumState>) -> StatusCode {
    let (tx, rx) = oneshot::channel();
    match state.engine_sender.send(Commands::Resume { respond_to: tx }).await {
        Ok(_) => {
            match rx.await {
                Ok(true) => StatusCode::ACCEPTED,      
                Ok(false) => StatusCode::NOT_MODIFIED,
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
            }
        },
        _ => StatusCode::NO_CONTENT,
    }
}

// The produce function stays exactly the same
async fn push_to_topic(brokers: &str, topic_name: &str) -> anyhow::Result<()> {
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