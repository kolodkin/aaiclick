Cyber Threat Feeds
---

Multi-source cybersecurity pipeline that loads CISA KEV, Shodan CVEDB, and FIRST EPSS data directly into ClickHouse via URL ingestion, consolidates them into an AggregatingMergeTree table keyed by CVE ID, and produces a threat intelligence report.

# How to Run

```bash
./cyber_threat_feeds.sh
```
