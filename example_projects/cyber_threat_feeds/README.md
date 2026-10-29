Cyber Threat Feeds — Multi-Source Normalization
---

Multi-source cybersecurity pipeline that loads CISA KEV, Shodan CVEDB, and FIRST EPSS data (JSON, gzip CSV) directly into ClickHouse via URL ingestion, normalizes and consolidates them into an AggregatingMergeTree table keyed by CVE ID, and produces a threat intelligence report. Runs in local mode: the whole job in one process on embedded chdb + SQLite, no servers.

```bash
./cyber_threat_feeds.sh

# Override job kwargs
./cyber_threat_feeds.sh --params '{"shodan_limit": 1000}'
```
