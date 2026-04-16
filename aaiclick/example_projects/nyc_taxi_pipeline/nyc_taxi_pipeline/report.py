"""Final report printout logic for the NYC Taxi pipeline."""

from aaiclick.data.object import Object
from aaiclick.orchestration import task


def _fmt(value: object) -> str:
    """Format a numeric value for display."""
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


def _print_report(
    report: dict,
    trips_md: str,
    by_payment_md: str,
    by_pickup_zone_md: str,
    by_passenger_md: str,
    top_zones_md: str,
) -> None:
    """Print formatted summary report to stdout as markdown."""
    ov = report["overview"]
    print("\n## NYC Taxi Analysis Report\n")

    print("### Overview\n")
    print(f"- Total trips: {_fmt(ov['total_trips'])}")
    print(f"- Total revenue: ${_fmt(ov['total_revenue'])}")
    print(f"- Total tips: ${_fmt(ov['total_tips'])}")
    print(f"- Avg fare: ${_fmt(ov['avg_fare'])}")
    print(f"- Avg distance: {_fmt(ov['avg_distance'])} mi")

    print("\n#### Sample (first 5 rows)\n")
    print(trips_md)

    fd = report["fare_distribution"]
    print("\n### Fare Distribution\n")
    print(f"- Mean: ${_fmt(fd['mean'])}")
    print(f"- Std: ${_fmt(fd['std'])}")
    print(f"- P25: ${_fmt(fd['p25'])}")
    print(f"- Median: ${_fmt(fd['median'])}")
    print(f"- P75: ${_fmt(fd['p75'])}")
    print(f"- P90: ${_fmt(fd['p90'])}")
    print(f"- P99: ${_fmt(fd['p99'])}")

    ta = report["tip_analysis"]
    print("\n### Tip Analysis\n")
    print(f"- Avg tip: ${_fmt(ta['avg_tip'])}")
    print(f"- Median tip: ${_fmt(ta['median_tip'])}")
    print(f"- Avg tip %: {_fmt(ta['avg_tip_pct'])}%")
    print(f"- Median tip %: {_fmt(ta['median_tip_pct'])}%")
    print(f"- Max tip: ${_fmt(ta['max_tip'])}")
    print(f"- Tip share total: {_fmt(ta['tip_share_of_total'])}%")

    da = report["distance_analysis"]
    print("\n### Distance Analysis\n")
    print(f"- Avg distance: {_fmt(da['avg_distance'])} mi")
    print(f"- Median distance: {_fmt(da['median_distance'])} mi")
    print(f"- Short trips (<1mi): {_fmt(da['short_trips_pct'])}%")
    print(f"- Long trips (>10mi): {_fmt(da['long_trips_pct'])}%")
    print(f"- Avg fare/mile: ${_fmt(da['avg_fare_per_mile'])}")
    print(f"- Median fare/mile: ${_fmt(da['median_fare_per_mile'])}")

    print("\n### By Payment Type\n")
    print(by_payment_md)

    print("\n#### Statistics\n")
    for name, data in report["payment_breakdown"].items():
        print(
            f"- **{name}**: avg fare ${_fmt(data['avg_fare'])}, "
            f"avg tip ${_fmt(data['avg_tip'])}, "
            f"avg distance {_fmt(data['avg_distance'])} mi"
        )

    print("\n### By Pickup Zone\n")
    print(by_pickup_zone_md)

    print("\n### By Passenger Count\n")
    print(by_passenger_md)

    print("\n### Top Revenue Zones\n")
    print(top_zones_md)


@task
async def generate_summary_report(
    trips: Object,
    basic_stats: dict,
    statistical_metrics: dict,
    tip_analysis: dict,
    distance_analysis: dict,
    by_payment: Object,
    by_pickup_zone: Object,
    by_passenger: Object,
    top_zones: Object,
) -> dict:
    """Combine all analysis results into a markdown report."""
    payment_data = await by_payment.data()

    payment_names = {1: "Credit", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown"}

    report = {
        "overview": {
            "total_trips": basic_stats["total_trips"],
            "total_revenue": basic_stats["total_revenue"],
            "total_tips": basic_stats["total_tips"],
            "avg_fare": basic_stats["avg_fare"],
            "avg_distance": basic_stats["avg_distance"],
        },
        "fare_distribution": {
            "mean": basic_stats["avg_fare"],
            "std": statistical_metrics["fare_std"],
            "p25": statistical_metrics["fare_p25"],
            "median": statistical_metrics["fare_median"],
            "p75": statistical_metrics["fare_p75"],
            "p90": statistical_metrics["fare_p90"],
            "p99": statistical_metrics["fare_p99"],
        },
        "tip_analysis": tip_analysis,
        "distance_analysis": distance_analysis,
        "payment_breakdown": {
            payment_names.get(ptype, f"Type_{ptype}"): {
                "avg_fare": payment_data["fare_amount"][i],
                "avg_tip": payment_data["tip_amount"][i],
                "avg_distance": payment_data["trip_distance"][i],
            }
            for i, ptype in enumerate(payment_data["payment_type"])
        },
    }

    trips_md = await trips.view(limit=5).markdown()
    by_payment_md = await by_payment.view(limit=10).markdown()
    by_pickup_zone_md = await by_pickup_zone.view(limit=10).markdown()
    by_passenger_md = await by_passenger.view(limit=10).markdown()
    top_zones_md = await top_zones.view(limit=10).markdown()

    _print_report(report, trips_md, by_payment_md, by_pickup_zone_md, by_passenger_md, top_zones_md)
    return report
