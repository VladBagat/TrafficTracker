use axum::extract::State;
use axum::http::StatusCode;
use log::{info, warn};
use rdkafka::{ClientContext, TopicPartitionList, Message};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::*;
use rdkafka::error::KafkaResult;
use rdkafka::message::Headers;
use axum::{routing::get, Router, routing::post};
use std::default;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use apache_avro::{Schema, to_avro_datum, to_value};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;


use crate::force_order::{ForceOrder, ForceOrderFlat};
mod force_order;

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
    state: EngineState
}

#[derive(Clone)]
struct EngineState {
    paused: bool,
    force_order_avro_schema: Option<Schema>,
    brokers: String,
    consumer: Option<Arc<KafkaConsumer>>
}

type KafkaConsumer = StreamConsumer<CustomContext>;

#[derive(Clone)]
struct AxumState {
    engine_sender: Sender<Commands>
}

impl EngineActor {
    async fn run(mut self) {
        loop {
            tokio::select! {
                Ok(msg) = self.state.consumer.as_ref().unwrap().recv() => {
                    
                }
                Some(msg) = self.command_reciever.recv() => {
                    match msg {
                        Commands::GetStatus { respond_to } => {
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
                    }
                }
            }
        }
    }

    fn engine_startup(&mut self) {
        self.state.force_order_avro_schema = Some(force_order::get_avro_schema());

        let context = CustomContext;

        let mut config = ClientConfig::new();

        config
            .set("group.id", "1") //todo: assign proper val
            .set("bootstrap.servers", &self.state.brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("statistics.interval.ms", "30000")
            .set_log_level(RDKafkaLogLevel::Debug);

        /*if let Some(assignor) = assignor {
            config
                .set("group.remote.assignor", assignor)
                .set("group.protocol", "consumer")
                .remove("session.timeout.ms");
        }*/

        let consumer: KafkaConsumer = config
            .create_with_context(context)
            .expect("Consumer creation failed");

        consumer
            .subscribe(&["binance_liquidations"])
            .expect("Can't subscribe to specified topics");

        self.state.consumer = Some(Arc::new(consumer));
    }
}
struct CustomContext;
impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init(); 
    info!("Processor #1 started");

    let (tx, rx) = mpsc::channel(128);

    let mut actor = EngineActor{ 
        command_reciever: rx,
        state: EngineState { 
            paused: true,
            force_order_avro_schema: None,
            brokers:std::env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".to_string()),
            consumer: None
        }
    };

    actor.engine_startup();

    tokio::spawn(actor.run());

    let state = AxumState {
        engine_sender: tx
    };

    let app = Router::new()
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3001));
    let listener = TcpListener::bind(addr).await?;

    info!("Processor #1 Axum is ready to be served");

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