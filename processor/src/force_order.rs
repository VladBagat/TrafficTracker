// This is a auto-generated serde schema for forceOrder event from Binance Futures Websocket

use apache_avro::Schema;
use serde_derive::Deserialize;
use serde_derive::Serialize;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForceOrder {
    pub e: String,
    #[serde(rename = "E")]
    pub e2: i64,
    pub o: O,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct O {
    pub s: String,
    #[serde(rename = "S")]
    pub s2: String,
    pub o: String,
    pub f: String,
    pub q: String,
    pub p: String,
    pub ap: String,
    #[serde(rename = "X")]
    pub x: String,
    pub l: String,
    pub z: String,
    #[serde(rename = "T")]
    pub t: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForceOrderFlat {
    pub e: String,
    #[serde(rename = "E")]
    pub e2: i64,
    pub s: String,
    #[serde(rename = "S")]
    pub s2: String,
    pub o: String,
    pub f: String,
    pub q: f64,
    pub p: f64,
    pub ap: f64,
    #[serde(rename = "X")]
    pub x: String,
    pub l: f64,
    pub z: f64,
    #[serde(rename = "T")]
    pub t: i64,
}

pub fn get_avro_schema() -> Schema {
    let raw_schema = r#"
    {
    "name": "ForceOrderSchema",
    "namespace": "com.group.ForceOrderSchema",
    "doc": "Flat schema for ForceOrder message from Binance WS Stream",
    "type": "record",
    "fields": [
        {
            "name": "e",
            "type": "string"
        },
        {
            "name": "E",
            "type": "long"
        },
        {
            "name": "s",
            "type": "string"
        },
        {
            "name": "S",
            "type": "string"
        },
        {
            "name": "o",
            "type": "string"
        },
        {
            "name": "f",
            "type": "string"
        },
        {
            "name": "q",
            "type": "double"
        },
        {
            "name": "p",
            "type": "double"
        },
        {
            "name": "ap",
            "type": "double"
        },
        {
            "name": "X",
            "type": "string"
        },
        {
            "name": "l",
            "type": "double"
        },
        {
            "name": "z",
            "type": "double"
        },
        {
            "name": "T",
            "type": "long"
        }
    ]
    }
    "#;

    Schema::parse_str(raw_schema).unwrap()
}
