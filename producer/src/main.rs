use std::time::{SystemTime, UNIX_EPOCH};
use apache_avro::{Schema, to_avro_datum, to_value};
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


use shared::force_order::{ForceOrder, ForceOrderFlat, get_avro_schema};

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

#[derive(Clone)]
struct EngineState {
    paused: bool,
    force_order_avro_schema: Option<Schema>,
    brokers: String,
    producer: Option<FutureProducer>
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
    async fn process_message(&mut self, msg: Message) {
        // ts when Producer recieved message
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;

        let json_msg = &msg.into_text().unwrap();

        // Best effort to deserialize, else drop
        if let Ok(data) = serde_json::from_str::<ForceOrder>(&json_msg) {
            // Currency Symbol as topic key
            let symbol_key = data.o.s.as_str();

            // When Biannce Event happened
            let event_time = data.e2 as u64;

            let flat = ForceOrderFlat {
                e: data.e,
                e2: data.e2,
                s: data.o.s.clone(),
                s2: data.o.s2,
                o: data.o.o,
                f: data.o.f,
                q: data.o.q.parse().unwrap_or(0f64),
                p: data.o.p.parse().unwrap_or(0f64),
                ap: data.o.ap.parse().unwrap_or(0f64),
                x: data.o.x,
                l: data.o.l.parse().unwrap_or(0f64),
                z: data.o.z.parse().unwrap_or(0f64),
                t: data.o.t,
            };

            let schema = self.state.force_order_avro_schema.as_mut().expect("Avro scheme failed to initialize");
            let avro_value = to_value(&flat).expect("serde -> Avro Value failed");
            let encoded = to_avro_datum(schema, avro_value).expect("Non-compliance with Avro scheme. Bad sanitization.");

            if let Err(e) = self.push_to_topic(symbol_key, &encoded, event_time, now).await {
                info!("Failed to push to topic: {:?}", e);
            }
        }
    }

    fn engine_startup(&mut self) {
        self.state.force_order_avro_schema = Some(get_avro_schema());

        let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &self.state.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

        self.state.producer = Some(producer);
    }

    async fn push_to_topic(&mut self, key: &str, message: &Vec<u8>, event_time: u64, process_start: u64) -> anyhow::Result<()> {
        // ts when Producer ingested message
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;

        let record = FutureRecord::to("binance-liquidations")
                .key(key)
                .payload(message)
                .headers(OwnedHeaders::new()
                    .insert(Header { key: "source", value: Some("binance_websocket") })
                    .insert(Header { key: "event_ts", value: Some(&event_time.to_string()) })
                    .insert(Header { key: "recieval_ts", value: Some(&process_start.to_string()) })
                    .insert(Header { key: "ingest_ts", value: Some(&now.to_string()) })
                );

        if let Err(e) = self.state.producer.as_ref().unwrap().send_result(record) {
            info!("Message send failed.{:?}", e);
        }

        Ok(())
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

    info!("Producer connected to Binance Futures Stream");

    let mut actor = EngineActor{ 
        command_reciever: rx,
        ws_write: write,
        ws_read: read,
        state: EngineState { 
            paused: true,
            force_order_avro_schema: None,
            brokers:std::env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".to_string()),
            producer: None
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

