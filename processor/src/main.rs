use axum::extract::State;
use axum::http::StatusCode;
use log::{debug, error, info};
use rdkafka::message::{BorrowedMessage, Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientContext, Message, Statistics, TopicPartitionList};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::*;
use rdkafka::error::KafkaResult;
use axum::{Router, routing::post};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use apache_avro::{Schema, from_avro_datum, from_value, to_avro_datum, to_value};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;


use shared::{force_order, processed_force_order};

enum Commands {
    GetStatus {
        respond_to: oneshot::Sender<LoggingState>
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

struct EngineState {
    paused: bool,
    force_order_avro_schema: Option<Schema>,
    processed_avro_schema: Option<Schema>,
    brokers: String,
    consumer: Option<KafkaConsumer>,
    producer: Option<FutureProducer>,
    calculation_state: CalculationState,
    logging_state: LoggingState
}

#[derive(Clone, PartialEq, PartialOrd, Eq)]
struct LoggingState {
    running_time: u64,
    successful_runs: u64,
    failed_runs: u64,
    state_message: String
}

impl Default for LoggingState {
    fn default() -> Self {
        Self { 
            running_time: 0,
            successful_runs: 0,
            failed_runs: 0,
            state_message: String::from("State logging is not implemented")
        }
    }
}

#[derive(Clone, Debug, Default)]
struct CalculationState {
    symbol_ca_map: HashMap<String, CumulativeAverage>
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Default)]
struct CumulativeAverage{
    average: f64,
    n: i32,
    first_timestamp: i64
}

impl CumulativeAverage {
    fn new(first_timestamp: i64) -> CumulativeAverage {
        CumulativeAverage {
            average: 0f64,
            n: 0,
            first_timestamp: first_timestamp
        }
    }
    fn add(&mut self, x: f64) {
        self.average = (x + (self.n as f64 * self.average)) / (self.n + 1) as f64;
        self.n += 1;
    }
}

type KafkaConsumer = StreamConsumer<CustomContext>;

#[derive(Clone)]
struct AxumState {
    engine_sender: Sender<Commands>
}

impl EngineActor {
    async fn run(mut self) {
        let consumer = self.state.consumer.take().unwrap(); 
        loop {
            tokio::select! {
                msg = consumer.recv() => {
                    match msg {
                        Ok(msg) => {
                            if self.state.paused { continue; }
                            self.process_message(msg).await;
                        }
                        Err(e) => {
                            error!("Consumer recv error: {:?}.", e);
                        }
                    }
                }
                Some(msg) = self.command_reciever.recv() => {
                    match msg {
                        Commands::GetStatus { respond_to } => {
                            let _ = respond_to.send(LoggingState::default());
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
        self.state.processed_avro_schema = Some(processed_force_order::get_avro_schema());

        let context = CustomContext;

        let mut config = ClientConfig::new();

        config
            .set("group.id", "1") //todo: assign proper val
            .set("bootstrap.servers", &self.state.brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("statistics.interval.ms", "60000")
            .set_log_level(RDKafkaLogLevel::Warning);

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
            .subscribe(&["binance-liquidations"])
            .expect("Can't subscribe to specified topics");

        self.state.consumer = Some(consumer);

        let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &self.state.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

        self.state.producer = Some(producer);
    }

    async fn process_message(&mut self, message: BorrowedMessage<'_>) {
        let now = shared::current_time_micros!();
        let mut payload = message.payload().expect("Message has no payload");
        let schema = self.state.force_order_avro_schema.as_mut().expect("Avro scheme failed to initialize");
        let value = from_avro_datum(schema, &mut payload, None).unwrap();
        let decoded = from_value::<force_order::ForceOrderFlat>(&value).expect("Deserialization failed");
        
        if !decoded.x.eq("FILLED") && !decoded.x.eq("PARTIALLY_FILLED") {
            return;
        }
        let data = self.state.calculation_state.symbol_ca_map.entry(decoded.s.clone()).or_insert(CumulativeAverage::new(decoded.t));

        if decoded.t - data.first_timestamp > 15000 {
            let processed = shared::processed_force_order::ProcessedFOFlat{
                e: decoded.e2,
                s: decoded.s.clone(),
                s2: decoded.s2,
                o: decoded.o,
                f: decoded.f,
                ca: data.average,
                p: 15,
            };
            let schema = self.state.processed_avro_schema.as_mut().expect("Avro scheme failed to initialize");
            let avro_value = to_value(&processed).expect("serde -> Avro Value failed");
            let encoded = to_avro_datum(schema, avro_value).expect("Non-compliance with Avro scheme. Bad sanitization.");

            
            *data = CumulativeAverage::new(decoded.t);
            data.add(decoded.ap * decoded.l);
            let _ = self.push_to_topic(&decoded.s, &encoded, now).await;
        } else {
            data.add(decoded.ap * decoded.l);
        }
    }

    async fn push_to_topic(&self, key: &str, message: &Vec<u8>, process_start: u64) -> anyhow::Result<()> {
        // ts when Processor sends message
        let now = shared::current_time_micros!();

        let record = FutureRecord::to("binance-liquidations-processed")
                .key(key)
                .payload(message)
                .headers(OwnedHeaders::new()
                    .insert(Header { key: "source", value: Some("Processor #1") })
                    .insert(Header { key: "recieval_ts", value: Some(&process_start.to_string()) })
                    .insert(Header { key: "ingest_ts", value: Some(&now.to_string()) })
                );

        if let Err(e) = self.state.producer.as_ref().unwrap().send_result(record) {
            error!("Message send failed.{:?}", e);
        }

        Ok(())
    }
}
struct CustomContext;
impl ClientContext for CustomContext {
    fn stats(&self, stats: Statistics) {
        debug!("rdkafka stats: {:?}", stats);
    }
}

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
            processed_avro_schema: None,
            brokers:std::env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".to_string()),
            consumer: None,
            producer: None,
            calculation_state: CalculationState::default(),
            logging_state: LoggingState::default()
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