# Modern Data Architectures for Big Data I
## Lab 8.2 — Binance Crypto Exchange Dual Data Ingestion

*Batch + streaming with NiFi 2, MinIO and Kafka on OSBDET 2026 · Updated October 2026*

---

## Agenda

1. **Introduction to the lab** — Binance, WebSockets and "dual" ingestion
2. **Storage layer preparation** — a MinIO bucket for the raw zone
3. **NiFi flow import & review** — controller services and the two steps
4. **NiFi flow execution** — files every minute, events every second
5. **Going further** — challenges and troubleshooting

---

## Time to turn OSBDET on!

1. **Start the VM** — VirtualBox: *Start without GUI* · UTM: *Play*
2. **Open the WebUI** — `http://localhost:2026`
3. **Get the flow** — `binance_dual_ingestion.json` from Blackboard

---

## 1. Introduction to the Lab

### Binance

- The **largest cryptocurrency exchange** by trading volume
- Several markets: **spot**, futures, options…
- Mobile and web apps, plus **REST** and **WebSocket** APIs to automate operations
- We ingest **spot market** data with the [WebSocket Streams](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md), a high-frequency real-time interface

| REST (TfL lab) | WebSocket (this lab) |
|---|---|
| You ask every time (polling): `GET → response`, `GET → response` | One connection stays open; the server **pushes** updates |

### WebSocket Streams: a two-step pub/sub

```
 You (NiFi)  ── Step 1 · Send: list of channels you want ──►  Binance: channel subscription handler
 You (NiFi)  ◄── Step 2 · Capture: updates in real time ───  Binance: updates dispatcher ◄ exchange activity
```

### Both steps speak JSON

**Step 1 — subscribe:**

```json
{ "method": "SUBSCRIBE", "params": ["btcusdt@miniTicker"], "id": 1 }
```

Stream names follow `<symbol>@<stream>` (e.g. `btcusdt@aggTrade`, `btcusdt@depth`).

**Step 2 — updates (every 1,000 ms):**

```javascript
{ "e": "24hrMiniTicker",   // event type
  "E": 1719415554901,      // event time (ms since 1970)
  "s": "BTCUSDT",          // symbol
  "c": "61385.66",         // close (last) price
  "o": "61689.99",         // open price, 24 h ago
  "h": "62487.81",         // high, rolling 24 h
  "l": "61135.37",         // low, rolling 24 h
  "v": "24648.59",         // volume in BTC
  "q": "1521208916.60" }   // volume in USDT
```

### Try it yourself first (Python)

```python
import json, websocket                    # pip install websocket-client

ws = websocket.create_connection("wss://stream.binance.com:9443/ws")
ws.send(json.dumps({"method": "SUBSCRIBE",      # step 1
                    "params": ["btcusdt@miniTicker"], "id": 1}))
print(ws.recv())                            # {"result": null, "id": 1} = OK

for _ in range(5):                          # step 2
    t = json.loads(ws.recv())
    print(t["s"], "last price:", t["c"])
ws.close()
```

> **Good to know** (Binance docs): a connection lasts at most **24 h**; the server pings every **20 s**; max **5 messages/s** sent by you and **1,024 streams** per connection.

### The lab's data value chain: dual ingestion

```
SOURCES               INGESTION              STORAGE
Binance WebSocket ──► Apache NiFi ─┬─ batch · 1 file per minute (JSON Lines) ──► MinIO  (bucket raw-binance)
btcusdt@miniTicker    one connection └─ streaming · every event as it arrives ──► Kafka  (topic binance.bitcoin)
```

The two paths from lectures 5.1 and 7.1 in one flow: the same data feeds the **data lake** and the **real-time** pipeline.

---

## 2. Storage Layer Preparation

### Introducing object stores

- In lab 8.1 we used **HDFS** (a distributed file system) for batch storage
- Now we use **MinIO**, an **S3-compatible object store**
- Object storage is the base of **decoupled** architectures: storage on one side (MinIO), table formats in the middle (Iceberg, Hudi, Delta Lake), any compute engine on top (Spark, Trino, StarRocks, Druid)

*Recall from lecture 6.1: MinIO's free community edition stopped being maintained in 2025; OSBDET ships an earlier release that works well for learning.*

### Steps 1–2 — Start MinIO and log in

1. **Start MinIO** in the WebUI (its tile is at the bottom; scroll down)
2. **Open the MinIO console** — UI icon → `localhost:29001`, user `osbdet`, password `osbdet123$`

### Steps 3–6 — Create the raw-binance bucket

1. **Administrator → Buckets**
2. **Create Bucket +** (top-right)
3. Name it **`raw-binance`**
4. **User → Object Browser** to watch files arrive later

Or in code: `s3.create_bucket(Bucket="raw-binance")`

---

## 3. NiFi Flow Import & Review

### Steps 1–4 — Start NiFi and import the flow

1. **Start NiFi** in the WebUI
2. **Open the NiFi UI** — `localhost:29090/nifi`
3. **Create a Process Group** from `binance_dual_ingestion.json`; name it **Binance Dual Ingestion**
4. **Open it** — Step 1 (yellow) and Step 2 (green)

### The whole flow at a glance

```
                       ConnectWebSocket (keeps the connection open)
            ┌── connected ──────────┐            ┌── text message ──┬── text message ──┐
 STEP 1 · channels subscription      │  STEP 2 · collecting updates   │                  │
 Subscription Message (ReplaceText)  │  Send events to Kafka          │  New line at the end (ReplaceText)
        │ success                    │  (PublishKafka)  ⚡ streaming   │        │ success
 Send to Binance (PutWebSocket)      │                                │  Batch of events, 1 min (MergeContent)
                                     │                                │        │ merged
                                     │                                │  Add timestamp to batch (UpdateAttribute)
                                     │                                │        │ success
                                     │                                │  Send batch to MinIO (PutS3Object) 🗄 batch
```

### Step 5 — Controller services: shared resources

- Shared **services** used by processors: SSL contexts, server connections, credentials…
- Part of the **Flow Controller** from the NiFi lecture
- Defined globally or per Process Group; open with right-click → **Controller Services**

| Service | Used by | Purpose |
|---|---|---|
| StandardRestrictedSSLContextService | JettyWebSocketClient | Trusted certificates for `wss://` |
| JettyWebSocketClient | ConnectWebSocket, PutWebSocket | Connection to Binance |
| AWSCredentialsProviderControllerService | PutS3Object | MinIO access keys |
| Kafka3ConnectionService | PublishKafka | Connection to Kafka |

### Steps 6–9 — Check and fill in the four services

| Service | Settings | You must… |
|---|---|---|
| StandardRestrictedSSLContextService | Truststore Filename `/opt/jdk-21-cacerts` · Type `JKS` · Password `changeit` | **type the password** |
| JettyWebSocketClient | WebSocket URI `wss://stream.binance.com:9443/ws` · SSL Context Service: the one above | just check |
| AWSCredentialsProviderControllerService | Access Key ID `s3access` · Secret Access Key `_s3access123$` | **type both** |
| Kafka3ConnectionService | Bootstrap Servers `localhost:9092` | just check |

> **Why are passwords empty?** NiFi **never exports secrets** in flow definitions. You re-enter them after every import. The JettyWebSocketClient shows as **Invalid** until the SSL service is enabled.

### Steps 10–13 — Enable them, in order

1. SSL context → 2. Jetty WebSocket client (needs 1) → 3. AWS credentials → 4. Kafka3 connection

Click the **⚡ Enable** icon on each and keep the default scope **"Service only"**.

### Step 14 — Step 1: subscribe to the channel

`ConnectWebSocket ─connected→ Subscription Message (ReplaceText) ─success→ Send to Binance (PutWebSocket)`

When the connection opens, ConnectWebSocket emits a FlowFile on **connected**. ReplaceText replaces its content with the subscription message, and PutWebSocket sends it through the **same WebSocket session**:

```json
{"method": "SUBSCRIBE", "params": ["btcusdt@miniTicker"], "id": 1}
```

### Step 15 — Step 2: collect updates, two ways

- **⚡ Streaming path:** every **text message** (one per second) goes straight to **PublishKafka**, topic `binance.bitcoin`
- **🗄 Batch path:**
  1. **ReplaceText** appends a newline → each event becomes one **JSON Line**
  2. **MergeContent** glues 1 minute of events into one FlowFile (~60 events)
  3. **UpdateAttribute** sets a timestamped name, e.g. `binanceupdates_202605141511.json`
  4. **PutS3Object** writes it to `raw-binance` in MinIO

---

## 4. NiFi Flow Execution

### Steps 1–3 — Start Kafka, then the Process Group

1. **Start Kafka** in the WebUI (NiFi and MinIO should already be on; keep Hadoop, Spark… off)
2. **Start the Process Group** — Operation palette ▶, or right-click → Start
3. **Go inside and watch** — counters grow every second; open queues and provenance to see FlowFiles

### Step 4 — Batch path: a new file every minute

In **Object Browser → raw-binance**, refresh: one new file per minute (~11 KB each), e.g.:

```
binanceupdates_202605141511.json   Today, 15:11   11.3 KiB
binanceupdates_202605141510.json   Today, 15:10   11.3 KiB
binanceupdates_202605141509.json   Today, 15:09   11.3 KiB
```

Read one in Python:

```python
import boto3, pandas as pd, io
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="osbdet", aws_secret_access_key="osbdet123$")
key = s3.list_objects_v2(Bucket="raw-binance")["Contents"][-1]["Key"]
body = s3.get_object(Bucket="raw-binance", Key=key)["Body"].read()
df = pd.read_json(io.BytesIO(body), lines=True)
print(len(df), "events", df["c"].min(), "→", df["c"].max())
```

### Steps 5–6 — Streaming path: events in real time

Kafka has no web UI in OSBDET, so open a **terminal** from Jupyter (`localhost:28888/lab` → File → New → Terminal):

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic binance.bitcoin
```

```
{"e":"24hrMiniTicker","E":1719415554901,"s":"BTCUSDT","c":"61385.66000000","o":"61689.98000000",…}
{"e":"24hrMiniTicker","E":1719415556826,"s":"BTCUSDT","c":"61385.66000000","o":"61689.99000000",…}
```

Add `--from-beginning` to replay everything Kafka has stored. Stop with `Ctrl + C`.

### Same data, two speeds

| | Kafka (streaming) | MinIO (batch) |
|---|---|---|
| Latency | Milliseconds | Up to a minute |
| Unit | One event | One file of ~60 events |
| Kept for | Retention period (7 days by default) | Forever (raw zone) |
| Good for | Live dashboards, alerts | History, analytics, ML |

### Steps 7–10 — Shut everything down

7. **Stop the Process Group** (otherwise NiFi starts it again on boot)
8. **Stop NiFi and Kafka** in the WebUI
9. **Stop MinIO** (bottom of the WebUI; your bucket and files are kept)
10. **Stop OSBDET** — power button (VirtualBox: *Stop → Shut Down*)

---

## 5. Going Further

### Challenges

- **A · More coins:** edit the Subscription Message to `"params": ["btcusdt@miniTicker", "ethusdt@miniTicker"]`. Where do ETH events end up?
- **B · Bigger batches:** change MergeContent's **Max Bin Age** to `5 min`. What happens to file count and size?
- **C · Follow the lake convention:** make PutS3Object write to `btcusdt/${now():format('yyyy/MM/dd')}/` so files are partitioned by day.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| A controller service won't enable | Truststore or MinIO password not typed after import | Edit, enter the secret, enable again |
| ConnectWebSocket never says "connected" | No internet in the VM, or a network blocking Binance | Try the Python test; the market-data-only endpoint `wss://data-stream.binance.vision:9443/ws` may work |
| No files in MinIO | Bucket name typo, wrong keys, or under a minute has passed | Bucket must be exactly `raw-binance`; check AWS credentials |
| The Kafka consumer shows nothing | Kafka started after the group, or wrong topic | Start Kafka, restart the group, use `binance.bitcoin` |
| Laptop very slow | Too many tools on, or the group left running | Stop the group; keep only NiFi, MinIO, Kafka on |

---

## Congrats, we're done!

One stream, two destinations: you built a dual (batch + streaming) ingestion.

---
