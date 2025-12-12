use std::{convert::Infallible, net::SocketAddr, time::Duration};

use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
    Router,
};
use futures_util::Stream;
use log::{error, info, warn};
use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::Message,
};
use tokio::net::TcpListener;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

#[derive(Clone)]
struct AppState {
    tx: tokio::sync::broadcast::Sender<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let topic = std::env::var("KAFKA_TOPIC")
        .unwrap_or_else(|_| "binance-liquidations-processed".to_string());

    // fan-out buffer for connected SSE clients
    let (tx, _rx) = tokio::sync::broadcast::channel::<String>(1024);
    let state = AppState { tx: tx.clone() };

    tokio::spawn(async move {
        if let Err(e) = kafka_consume_loop(&brokers, &topic, tx).await {
            error!("kafka consumer loop exited: {e:?}");
        }
    });

    let app = Router::new().route("/events", get(sse_events)).with_state(state);

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "4010".to_string())
        .parse()
        .unwrap();
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    info!("kafka-sink listening on http://{addr}/events");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn kafka_consume_loop(
    brokers: &str,
    topic: &str,
    tx: tokio::sync::broadcast::Sender<String>,
) -> anyhow::Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "kafka-sink-1")
        .set("bootstrap.servers", brokers)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest")
        .create()?;

    consumer.subscribe(&[topic])?;
    info!("subscribed to topic: {topic}");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    let json = String::from_utf8_lossy(payload).to_string();
                    let _ = tx.send(json);
                }
            }
            Err(e) => warn!("kafka recv error: {e:?}"),
        }
    }
}

async fn sse_events(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx.subscribe();

    let stream = BroadcastStream::new(rx).filter_map(|item| {
        match item {
            Ok(json) => Some(Ok(Event::default().event("message").data(json))),
            Err(e) => {
                warn!("sse broadcast error: {e:?}");
                None
            }
        }
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}