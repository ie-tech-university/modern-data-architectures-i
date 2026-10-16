# Modern Data Architectures for Big Data I
## Lab — Transport for London Data Ingestion with Apache NiFi

*Lecture 4.1 lab · Apache NiFi 2 on OSBDET 2026 · Updated October 2026*

---

## Agenda

1. **Introduction to the lab** — the TfL Unified API and what we'll build
2. **Exploring bike points** — the data, in the browser and in code
3. **TfL data ingestion lab** — build, run and explore a NiFi flow
4. **Exporting NiFi flows** — download and re-import flow definitions
5. **Going further** — challenges and troubleshooting

---

## Time to turn OSBDET on!

This is a lab: we'll use the **course environment** in a few minutes.

1. **Start the VM** — VirtualBox: *Start without GUI* · UTM: *Play*
2. **Open the WebUI** — `http://localhost:2026`
3. **Wait for it** — the first start can take a couple of minutes

---

## 1. Introduction to the Lab

### Transport for London's Unified API

The [TfL Unified API](https://api.tfl.gov.uk/) is an **open data** service:

- London's public transport data in **real time**
- Open protocols (**REST API**) and open formats (**JSON**)
- The foundation for **end-user apps** and **data analysis**
- No key needed to start; register for an `app_key` to get higher rate limits

Many datasets are available (AccidentStats, AirQuality, **BikePoint**, Journey, Line, Mode, Occupancy, Place, Road, Search, StopPoint, Vehicle). We'll only ingest **bike stations** (*BikePoint*).

### The lab's data value chain

```
 SOURCES                  INGESTION              STORAGE        PROCESSING
 TfL BikePoint API  ───►  Apache NiFi            (next labs)    (next labs)
 api.tfl.gov.uk           InvokeHTTP
                 HTTPS GET every 5 min
```

### Lab outline

1. **Explore** BikePoint data in your web browser (and in code)
2. **Create** a simple NiFi flow
3. **Run and explore** the flow and its FlowFiles
4. **Export** the flow definition and re-import it

**What you'll build:** one InvokeHTTP processor, *TfL Bike Point Data*, running every 5 minutes inside a Process Group called **TfL Data Ingestion**.

---

## 2. Exploring Bike Points

### In your browser

REST APIs return **JSON** you can open in any browser. Go to **<https://api.tfl.gov.uk/BikePoint>**:

```
▾ 0:
    id:          "BikePoints_1"
    url:         "/Place/BikePoints_1"
    commonName:  "River Street , Clerkenwell"
    placeType:   "BikePoint"
  ▾ additionalProperties:
    ▾ 0:  key: "TerminalName"   value: "001023"
    ▸ 1 … 10
    lat:         51.529163
    lon:         -0.10997
▸ 1: { … }      … 799 bike points, about 2 MB
```

### Anatomy of a bike point

| Part | Fields |
|---|---|
| **Identity** | `id`, `commonName` |
| **Location** | `lat`, `lon` |
| **Live status** | `additionalProperties`: a list of `{key, value}` pairs |

| Key | Meaning |
|---|---|
| `NbBikes` | Bikes available now |
| `NbStandardBikes` | Of which standard bikes |
| `NbEBikes` | Of which e-bikes |
| `NbEmptyDocks` | Free docks to return a bike |
| `NbDocks` | Total docks at the station |
| `Locked` / `Installed` | Station status |

Values are strings (e.g. `"7"`), so convert them before doing maths.

### Three useful endpoints

| Endpoint | Returns |
|---|---|
| `/BikePoint` | All bike points (about 800) |
| `/BikePoint/BikePoints_1` | One bike point by id |
| `/BikePoint/Search?query=Waterloo` | Bike points whose name matches |

### Try it from a terminal (curl + jq)

```bash
# how many bike points?
curl -s https://api.tfl.gov.uk/BikePoint | jq 'length'

# name and bikes available at the first 3 stations
curl -s https://api.tfl.gov.uk/BikePoint | jq '.[:3][] | {name: .commonName,
   bikes: (.additionalProperties[] | select(.key=="NbBikes") | .value)}'
```

### Try it in Python (OSBDET's Jupyter)

```python
import requests

r = requests.get("https://api.tfl.gov.uk/BikePoint", timeout=30)
stations = r.json()
print(len(stations), "bike points")

def prop(station, key):
    for p in station["additionalProperties"]:
        if p["key"] == key:
            return int(p["value"])

top = sorted(stations, key=lambda s: prop(s, "NbBikes"), reverse=True)[:5]
for s in top:
    print(prop(s, "NbBikes"), s["commonName"])
```

Output from one run (October 2026; yours will differ):

```
799 bike points
51 Royal College Street, Camden Town
50 Waterloo Station 2, Waterloo
46 Doric Way , Somers Town
...
```

> **Why bother?** Exploring first tells you the data's **size** (about 2 MB per call), its **shape** and how often it **changes**. The API caches responses for about a minute, so polling every 5 minutes is plenty.

---

## 3. TfL Data Ingestion Lab

### Step 1 — Start NiFi from the OSBDET WebUI

Open `http://localhost:2026` and flip the **NiFi switch** on.

> **Be patient:** NiFi can take a few minutes to start. Keep other tools off to save memory.

### Step 2 — Open the NiFi WebUI

Click the **UI icon** on the NiFi tile. NiFi opens at `localhost:29090`, showing an empty canvas (the root Process Group, *NiFi Flow*).

### Step 3 — Create the Process Group "TfL Data Ingestion"

1. **Drag** the Process Group icon from the toolbar onto the canvas
2. **Name it** `TfL Data Ingestion`
3. Click **Add**

### Step 4 — Add an InvokeHTTP processor inside it

1. **Double-click** the Process Group to go inside (the breadcrumb shows *NiFi Flow » TfL Data Ingestion*)
2. **Drag** the Processor icon onto the canvas
3. **Filter** by `InvokeHTTP` and click **Add**

### Step 5 — Configure InvokeHTTP

**Double-click** the processor. Goal: call the TfL API **every 5 minutes**.

| Tab | Setting | Value |
|---|---|---|
| **Settings** | Name | `TfL Bike Point Data` |
| **Scheduling** | Scheduling strategy | Timer driven |
| | Run schedule | `5 min` |
| **Properties** | HTTP Method | `GET` |
| | HTTP URL | `https://api.tfl.gov.uk/BikePoint` |
| **Relationships** | Auto-terminate | ✓ Failure, No Retry, Original, Response, Retry |

> **Why terminate every relationship?** There's no processor after InvokeHTTP yet, so each output must be **auto-terminated**. Until then NiFi shows a **⚠ warning** and won't let you start the processor.

### Step 6 — Start InvokeHTTP

**Right-click** the processor → **Start**. After the first run you'll see something like:

| | |
|---|---|
| Read/Write | 0 bytes / **2.07 MB** |
| Tasks/Time | **1** / 00:00:00.611 |

It called the API once and wrote about 2 MB (all 799 stations). It will call again every 5 minutes.

### Step 7 — Explore data provenance

**Right-click** the processor → **View Data Provenance**. Every FlowFile leaves a trail of events:

```
 RECEIVE  ─── auto-terminated ───►  DROP
 InvokeHTTP gets the response:      No next processor, so NiFi drops it
 a new 2.07 MB FlowFile is born     (the provenance record remains)
```

Click **View details** on the RECEIVE event and look at both parts of the FlowFile:

**Attributes** (examples)

| Attribute | Value |
|---|---|
| `filename` / `uuid` | `75b67c63-b1f3-…` |
| `mime.type` | `application/json` |
| `invokehttp.status.code` | `200` |
| `invokehttp.status.message` | `OK` |
| `invokehttp.request.url` | `https://api.tfl.gov.uk/BikePoint` |

**Content:** the JSON list of 799 bike points. Click **View** in the Content tab to see it formatted.

### Step 8 — Stop InvokeHTTP

**Right-click** the processor → **Stop**. The icon turns into a red square.

> **Tip:** you can also start and stop a whole Process Group at once.

---

## 4. Exporting NiFi Flows

### Step 1 — Download the flow definition

**Right-click** the Process Group → **Download Flow Definition** → **Without External Services**. You get `TfL_Data_Ingestion.json` (about 9 KB).

NiFi 2 exports flows as **JSON flow definitions** (the old XML templates are gone). Keep them in Git!

### What's inside a flow definition?

```json
// simplified excerpt of TfL_Data_Ingestion.json
{
  "flowContents": {
    "name": "TfL Data Ingestion",
    "processors": [{
      "name": "TfL Bike Point Data",
      "type": "org.apache.nifi.processors.standard.InvokeHTTP",
      "schedulingStrategy": "TIMER_DRIVEN",
      "schedulingPeriod": "5 min",
      "properties": {
        "HTTP Method": "GET",
        "HTTP URL": "https://api.tfl.gov.uk/BikePoint"
      },
      "autoTerminatedRelationships":
        ["Failure", "No Retry", "Original", "Response", "Retry"]
    }],
    "connections": []
  }
}
```

Everything you configured in the UI is in the file, so you can version, review and share flows like code.

### Step 2 — Re-import it when creating a Process Group

1. Drag a new **Process Group** onto the canvas
2. Click the **upload icon** next to the name and pick `TfL_Data_Ingestion.json`
3. Give it a **new, meaningful name** (e.g. `TfL Data Ingestion (Copy)`) and click **Add**

### Steps 3–4 — Tidy up

3. **Stop NiFi** in the OSBDET WebUI (flip its switch off)
4. **Stop OSBDET** when you're done: power button at the top right of the WebUI (VirtualBox users can also use *Stop → Shut Down*)

---

## 5. Going Further

### Challenges: grow your flow

```
InvokeHTTP ──Response──► SplitJson ──split──► EvaluateJsonPath ──matched──► PutFile / PutS3Object
                         ($.*)                (name, bikes)                 (store each station)
                          A                    B                             C
```

- **A · Split the list:** connect *Response* to **SplitJson** with JsonPath `$.*`. How many FlowFiles come out?
- **B · Extract fields:** use **EvaluateJsonPath** to put `$.commonName` and `$.lat` into attributes. Check them in provenance.
- **C · Land the data:** write each station with **PutFile** (e.g. `/tmp/tfl`) or to MinIO with **PutS3Object**.

Remember to **un-terminate** the *Response* relationship once it has somewhere to go.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| The NiFi UI doesn't load | NiFi is still starting | Wait a few minutes, then refresh `localhost:29090` |
| ⚠ on the processor, Start greyed out | Relationships not terminated or property missing | Hover over ⚠ to read why; terminate all relationships (step 5) |
| Nothing happens after Start | Waiting for the next 5-minute run | Use **Run Once** from the right-click menu |
| `invokehttp.status.code` is 429 | Too many requests | Increase the run schedule; get a free `app_key` |
| Failure / Retry instead of Response | No internet from the VM | Check your connection; try the URL in your browser |
| Laptop very slow | Too many tools running | Turn off what you don't need in the WebUI |

---

## Congrats, we're done!

You ingested live data from London's bike-sharing network with Apache NiFi.

---

<details>
<summary><strong>Instructor notes — what changed from the original deck</strong></summary>

- **Kept:** the full lab sequence (turn on OSBDET, TfL API, lab value chain, outline, BikePoint exploration, steps 1–8, export/import, stop NiFi and OSBDET). Every screenshot is redrawn as a sketch.
- **Corrected:** "NiFi templates are downloaded as JSON files" → NiFi 2 downloads **flow definitions**; the OSBDET welcome page screenshot said v2025r1, the deck uses 2026r1.
- **Verified live (Oct 2026):** `/BikePoint` returns 799 stations, about 2.07 MB, cached for about 60 s; no API key needed for basic use.
- **Added:** anatomy of a bike point and its live-status keys, endpoints table, curl/jq and Python examples, NiFi URL `localhost:29090`, the RECEIVE → DROP provenance lifecycle, FlowFile attributes written by InvokeHTTP, a flow definition excerpt, optional challenges (SplitJson, EvaluateJsonPath, PutFile/PutS3Object) and troubleshooting.
- **Logos and photos:** TfL, NiFi and other logos plus the bike photos and movie still are not reproduced; tools are shown by name with original drawings.

</details>
