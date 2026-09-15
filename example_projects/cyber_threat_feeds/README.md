Cyber Threat Feeds — Multi-Source Normalization
---

Multi-source cybersecurity pipeline that loads CISA KEV, Shodan CVEDB, and FIRST EPSS data (JSON, gzip CSV) directly into ClickHouse via URL ingestion, normalizes and consolidates them into an AggregatingMergeTree table keyed by CVE ID, and produces a threat intelligence report.

```bash
./cyber_threat_feeds.sh

# Job kwargs are forwarded to `run-job`
./cyber_threat_feeds.sh --set shodan_limit=1000
```
