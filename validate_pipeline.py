from __future__ import annotations

from fraud_pipeline.analytics import fetch_flagged_transactions, fetch_heatmap_data, fetch_recent_transactions, fetch_summary
from fraud_pipeline.database import init_db, insert_transactions
from fraud_pipeline.simulator import _build_cards, generate_transactions


def main() -> None:
    init_db()
    cards = _build_cards()
    insert_transactions(generate_transactions(cards, batch_size=120))

    summary = fetch_summary()
    flagged = fetch_flagged_transactions(limit=25)
    recent = fetch_recent_transactions(limit=10)
    heatmap = fetch_heatmap_data()

    print("Validation snapshot")
    print(f"total_transactions={summary['metrics']['total_transactions']}")
    print(f"flagged_transactions={summary['metrics']['flagged_transactions']}")
    print(f"fraud_types={len(summary['type_counts'])}")
    print(f"risk_bands={len(summary['risk_counts'])}")
    print(f"rule_hits={len(summary['rule_counts'])}")
    print(f"recent_rows={len(recent)}")
    print(f"flagged_rows={len(flagged)}")
    print(f"heatmap_rows={len(heatmap)}")

    if not flagged.empty:
        top = flagged.iloc[0]
        print(f"top_alert={top['fraud_type']} score={top['risk_score']} rules={top['triggered_rules']}")


if __name__ == "__main__":
    main()
