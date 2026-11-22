-- Tables du pipeline de détection de fraude (base : fraud)
-- Pas de TTL sur event_time : les dates de Sparkov (2019-2020) seraient supprimées aussitôt.

CREATE TABLE IF NOT EXISTS fraud.transactions (
    trans_num   String,
    event_time  DateTime,
    cc_num      String,
    merchant    String,
    category    LowCardinality(String),
    amount      Float64,
    state       LowCardinality(String),
    lat         Float64,
    lon         Float64,
    merch_lat   Float64,
    merch_lon   Float64,
    is_fraud    UInt8
) ENGINE = MergeTree
ORDER BY (cc_num, event_time);

CREATE TABLE IF NOT EXISTS fraud.alerts (
    trans_num   String,
    event_time  DateTime,
    cc_num      String,
    rule        LowCardinality(String),
    alert_time  DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
ORDER BY (event_time, trans_num);