/*
{
	"E":1568014460893,                  // Event Time
	"s":"BTCUSDT",                      // Symbol
    "S":"SELL",                         // Side
    "o":"LIMIT",                        // Order Type
    "f":"IOC",                          // Time in Force
    "ca":"7.52139"                      // Cumulative Average
    "p":"15"                            // Period for average accumulation in seconds
}
*/

use apache_avro::Schema;
use serde_derive::Deserialize;
use serde_derive::Serialize;

pub fn get_avro_schema() -> Schema {
    let raw_schema = r#"
    {
    "name": "ProcessedForceOrderSchema",
    "namespace": "com.group.ProcessedForceOrderSchema",
    "doc": "Flat schema for Processed ForceOrder message",
    "type": "record",
    "fields": [
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
            "name": "ca",
            "type": "double"
        },
        {
            "name": "p",
            "type": "long"
        }
    ]
    }
    "#;

    Schema::parse_str(raw_schema).unwrap()
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessedFOFlat {
    #[serde(rename = "E")]
    pub e: i64,
    pub s: String,
    #[serde(rename = "S")]
    pub s2: String,
    pub o: String,
    pub f: String,
    pub ca: f64,
    pub p: i64,
}