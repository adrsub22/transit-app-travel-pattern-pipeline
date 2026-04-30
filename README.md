# Transit App Travel Pattern Pipeline

A sanitized portfolio example of a Python and ArcGIS automation workflow that converts cleaned mobility trip records from Transit App into GIS-ready layers for ArcGIS Online dashboards, web maps, and planning analysis.

This project demonstrates how detailed trip-level activity can be transformed into origin-destination flows, destination geography summaries, transfer hotspots, transfer-pattern metrics, and route-level reporting outputs that support public-sector planning and data storytelling.

---

## Why this project matters

Mobility datasets are often too detailed for direct planning interpretation. A single trip dataset may contain thousands or millions of individual records, timestamps, coordinates, routes, modes, and service types.

This workflow turns those detailed records into map-ready outputs that help planners and analysts understand:

- where trips are beginning and ending,
- which geographies are generating activity,
- where transfers are occurring,
- which route or service patterns are most common,
- how recent activity can be summarized in rolling time windows,
- and how spatial outputs can support dashboards, web maps, and planning narratives.

The goal is not just to process data. The goal is to create repeatable spatial decision-support products from complex mobility records.

---

## Core workflow

1. Read cleaned trip-leg records from a tabular source.
2. Apply rolling date windows and quality-control safeguards.
3. Build origin-destination line summaries.
4. Create destination geography summaries.
5. Identify transfer chains and transfer hotspot locations.
6. Calculate wait-time, travel-time, travel-distance, and transfer-pattern metrics.
7. Write GIS-ready outputs to a local file geodatabase.
8. Publish dashboard-ready layers to ArcGIS Online.

---

## Primary outputs

| Output | Purpose |
|---|---|
| Origin-destination line layer | Shows summarized movement patterns between origin and destination geographies. |
| Destination geography summary layer | Shows destination activity by planning geography, with trip counts and ranking fields. |
| Transfer hotspot point layer | Identifies common transfer locations and route-to-route patterns. |
| Unified transfer-pattern layer | Summarizes step-level and chain-level transfer behavior. |
| Monthly route-share table | Provides a route-level summary table for understanding Transit App usage. |

---

## Technical stack

- Python
- ArcPy
- ArcGIS Pro
- ArcGIS Online
- File geodatabases
- SQL or CSV-based cleaned input data

---

## Repository structure

```text
mobility-spatial-decision-support-pipeline/
├── README.md
├── config/
│   └── config.example.yml
├── docs/
│   ├── methodology.md
│   └── sample_outputs.md
└── src/
    ├── main.py
    ├── config_loader.py
    ├── data_access.py
    ├── od_processing.py
    ├── transfer_processing.py
    ├── route_share_processing.py
    ├── arcgis_publish.py
    └── utils.py
```

---

## Note

This repository is a sanitized demonstration of a production-style spatial analytics workflow. Agency-specific names, internal paths, database references, hosted service names, and private data sources have been replaced with generic placeholders.

The focus of this repo is the Python and ArcGIS automation pattern: transforming cleaned mobility records into GIS-ready outputs for planning analysis, web maps, and dashboards.

---

## Potential planning applications

This workflow can support:

- corridor planning
- mobility pattern analysis
- service planning
- transfer hotspot analysis
- public-facing dashboard development
- scenario and baseline condition reporting
- spatial storytelling
