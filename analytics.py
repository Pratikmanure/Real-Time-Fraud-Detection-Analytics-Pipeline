from __future__ import annotations

import pandas as pd

from database import read_sql


BASE_FLAGGED_QUERY = """
WITH ordered AS (
    SELECT
        t.*,
        CAST(strftime('%s', t.event_timestamp) AS INTEGER) AS event_epoch,
        LAG(t.event_timestamp) OVER (PARTITION BY t.card_id ORDER BY t.event_timestamp) AS prev_event_timestamp,
        LAG(t.merchant_city) OVER (PARTITION BY t.card_id ORDER BY t.event_timestamp) AS prev_city,
        LAG(t.merchant_country) OVER (PARTITION BY t.card_id ORDER BY t.event_timestamp) AS prev_country,
        LAG(t.device_id) OVER (PARTITION BY t.card_id ORDER BY t.event_timestamp) AS prev_device_id,
        LAG(t.channel) OVER (PARTITION BY t.card_id ORDER BY t.event_timestamp) AS prev_channel,
        AVG(t.amount) OVER (
            PARTITION BY t.card_id
            ORDER BY t.event_timestamp
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_prior_amount,
        COUNT(*) OVER (
            PARTITION BY t.card_id
            ORDER BY CAST(strftime('%s', t.event_timestamp) AS INTEGER)
            RANGE BETWEEN 900 PRECEDING AND CURRENT ROW
        ) AS txn_count_15m,
        COUNT(*) OVER (
            PARTITION BY t.card_id, t.merchant_name
            ORDER BY CAST(strftime('%s', t.event_timestamp) AS INTEGER)
            RANGE BETWEEN 600 PRECEDING AND CURRENT ROW
        ) AS merchant_repeat_10m,
        COUNT(*) OVER (
            PARTITION BY t.card_id, t.device_id
            ORDER BY CAST(strftime('%s', t.event_timestamp) AS INTEGER)
            RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
        ) AS device_seen_24h,
        ROW_NUMBER() OVER (
            PARTITION BY t.card_id, date(t.event_timestamp)
            ORDER BY t.amount DESC
        ) AS amount_rank_today
    FROM transactions t
),
flagged AS (
    SELECT
        ordered.*,
        ROUND((event_epoch - CAST(strftime('%s', prev_event_timestamp) AS INTEGER)) / 60.0, 2) AS minutes_since_prev,
        CASE
            WHEN prev_city IS NOT NULL
             AND prev_city <> merchant_city
             AND (event_epoch - CAST(strftime('%s', prev_event_timestamp) AS INTEGER)) <= 3600
            THEN 1 ELSE 0
        END AS city_jump_flag,
        CASE WHEN txn_count_15m >= 4 THEN 1 ELSE 0 END AS velocity_flag,
        CASE
            WHEN avg_prior_amount IS NOT NULL
             AND amount >= avg_prior_amount * 3
             AND amount >= 250
            THEN 1 ELSE 0
        END AS amount_spike_flag,
        CASE WHEN merchant_repeat_10m >= 3 THEN 1 ELSE 0 END AS merchant_burst_flag,
        CASE
            WHEN CAST(strftime('%H', event_timestamp) AS INTEGER) BETWEEN 0 AND 4
            THEN 1 ELSE 0
        END AS late_night_flag,
        CASE WHEN merchant_country <> home_country THEN 1 ELSE 0 END AS cross_border_flag,
        CASE
            WHEN channel = 'online'
             AND prev_device_id IS NOT NULL
             AND prev_device_id <> device_id
             AND COALESCE(device_seen_24h, 0) = 1
             AND (event_epoch - CAST(strftime('%s', prev_event_timestamp) AS INTEGER)) <= 1800
            THEN 1 ELSE 0
        END AS device_change_flag,
        CASE
            WHEN prev_channel IS NOT NULL
             AND prev_channel <> channel
             AND (event_epoch - CAST(strftime('%s', prev_event_timestamp) AS INTEGER)) <= 900
            THEN 1 ELSE 0
        END AS channel_switch_flag
    FROM ordered
)
SELECT
    transaction_id,
    event_timestamp,
    card_id,
    customer_id,
    card_tier,
    merchant_name,
    merchant_category,
    merchant_city,
    merchant_country,
    amount,
    currency,
    channel,
    entry_mode,
    device_id,
    home_city,
    home_country,
    scenario_label,
    prev_event_timestamp,
    prev_city,
    prev_country,
    prev_device_id,
    prev_channel,
    COALESCE(minutes_since_prev, 0.0) AS minutes_since_prev,
    txn_count_15m,
    merchant_repeat_10m,
    COALESCE(device_seen_24h, 0) AS device_seen_24h,
    ROUND(COALESCE(avg_prior_amount, 0.0), 2) AS avg_prior_amount,
    amount_rank_today,
    city_jump_flag,
    velocity_flag,
    amount_spike_flag,
    merchant_burst_flag,
    late_night_flag,
    cross_border_flag,
    device_change_flag,
    channel_switch_flag
FROM flagged
WHERE city_jump_flag = 1
   OR velocity_flag = 1
   OR amount_spike_flag = 1
   OR merchant_burst_flag = 1
   OR late_night_flag = 1
   OR cross_border_flag = 1
   OR device_change_flag = 1
   OR channel_switch_flag = 1
"""


FLAGGED_TRANSACTIONS_SQL = BASE_FLAGGED_QUERY + """
ORDER BY event_timestamp DESC
LIMIT ?
"""


RECENT_TRANSACTIONS_SQL = """
SELECT
    event_timestamp,
    card_id,
    merchant_name,
    merchant_city,
    merchant_country,
    amount,
    channel,
    scenario_label
FROM transactions
ORDER BY event_timestamp DESC
LIMIT ?
"""


HEATMAP_SQL = """
WITH recent_flags AS (
    SELECT *
    FROM (
        """ + BASE_FLAGGED_QUERY + """
        ORDER BY event_timestamp DESC
        LIMIT 500
    )
)
SELECT
    merchant_city,
    merchant_country,
    AVG(amount) AS avg_amount,
    COUNT(*) AS flagged_count
FROM recent_flags
GROUP BY merchant_city, merchant_country
ORDER BY flagged_count DESC, avg_amount DESC
"""


SUMMARY_SQL = """
WITH totals AS (
    SELECT COUNT(*) AS total_transactions, COALESCE(SUM(amount), 0) AS volume_usd
    FROM transactions
),
flags AS (
    SELECT *
    FROM (
        """ + BASE_FLAGGED_QUERY + """
        ORDER BY event_timestamp DESC
        LIMIT 2000
    )
)
SELECT
    totals.total_transactions,
    totals.volume_usd,
    COUNT(flags.transaction_id) AS flagged_transactions,
    COALESCE(SUM(flags.amount), 0) AS flagged_volume,
    COALESCE(MAX(flags.amount), 0) AS max_flagged_amount
FROM totals
LEFT JOIN flags ON 1 = 1
"""


RULE_COLUMNS = [
    "city_jump_flag",
    "velocity_flag",
    "amount_spike_flag",
    "merchant_burst_flag",
    "late_night_flag",
    "cross_border_flag",
    "device_change_flag",
    "channel_switch_flag",
]


RULE_LABELS = {
    "city_jump_flag": "Different cities within 1 hour",
    "velocity_flag": "4+ transactions in 15 minutes",
    "amount_spike_flag": "Amount > 3x recent baseline",
    "merchant_burst_flag": "3+ repeats at same merchant in 10 minutes",
    "late_night_flag": "Transaction between 00:00 and 04:59",
    "cross_border_flag": "Spend occurred outside home country",
    "device_change_flag": "New device used shortly after previous activity",
    "channel_switch_flag": "Rapid switch between channels",
}


def _triggered_rules(row: pd.Series) -> str:
    labels = [label for column, label in RULE_LABELS.items() if int(row[column]) == 1]
    return ", ".join(labels)


def _classify_type(row: pd.Series) -> str:
    if row["city_jump_flag"] and row["cross_border_flag"]:
        return "Impossible cross-border travel"
    if row["device_change_flag"] and row["channel_switch_flag"]:
        return "Account takeover pattern"
    if row["velocity_flag"] and row["merchant_burst_flag"]:
        return "Card testing burst"
    if row["amount_spike_flag"] and row["late_night_flag"]:
        return "High-value off-hours spend"
    if row["amount_spike_flag"]:
        return "High-value spend anomaly"
    if row["velocity_flag"]:
        return "Velocity spike"
    if row["merchant_burst_flag"]:
        return "Merchant repeat burst"
    if row["cross_border_flag"]:
        return "Cross-border anomaly"
    if row["device_change_flag"]:
        return "Device takeover risk"
    return "Off-hours spend"


def _risk_score(row: pd.Series) -> int:
    score = 12
    score += int(row["city_jump_flag"]) * 35
    score += int(row["velocity_flag"]) * 18
    score += int(row["amount_spike_flag"]) * 18
    score += int(row["merchant_burst_flag"]) * 14
    score += int(row["late_night_flag"]) * 8
    score += int(row["cross_border_flag"]) * 12
    score += int(row["device_change_flag"]) * 16
    score += int(row["channel_switch_flag"]) * 9
    if row["amount"] >= 1000:
        score += 10
    if row["txn_count_15m"] >= 6:
        score += 8
    if row["minutes_since_prev"] and row["minutes_since_prev"] <= 10 and row["prev_city"] not in (None, "", row["merchant_city"]):
        score += 10
    if row["device_change_flag"] and row["cross_border_flag"]:
        score += 8
    return min(score, 99)


def fetch_flagged_transactions(limit: int = 100) -> pd.DataFrame:
    flagged = read_sql(FLAGGED_TRANSACTIONS_SQL, (limit,))
    if flagged.empty:
        return flagged

    flagged["event_timestamp"] = pd.to_datetime(flagged["event_timestamp"])
    flagged["fraud_type"] = flagged.apply(_classify_type, axis=1)
    flagged["triggered_rules"] = flagged.apply(_triggered_rules, axis=1)
    flagged["rule_count"] = flagged[RULE_COLUMNS].sum(axis=1)
    flagged["risk_score"] = flagged.apply(_risk_score, axis=1)
    flagged["risk_band"] = pd.cut(
        flagged["risk_score"],
        bins=[0, 40, 70, 100],
        labels=["monitor", "review", "critical"],
        include_lowest=True,
        right=False,
    )
    return flagged.sort_values(["risk_score", "rule_count", "event_timestamp"], ascending=[False, False, False]).reset_index(drop=True)


def fetch_recent_transactions(limit: int = 20) -> pd.DataFrame:
    recent = read_sql(RECENT_TRANSACTIONS_SQL, (limit,))
    if not recent.empty:
        recent["event_timestamp"] = pd.to_datetime(recent["event_timestamp"])
    return recent


def fetch_heatmap_data() -> pd.DataFrame:
    return read_sql(HEATMAP_SQL)


def fetch_summary() -> dict:
    summary = read_sql(SUMMARY_SQL)
    record = summary.iloc[0].to_dict() if not summary.empty else {}
    flagged = fetch_flagged_transactions(limit=250)

    type_counts = (
        flagged["fraud_type"].value_counts().rename_axis("fraud_type").reset_index(name="count")
        if not flagged.empty
        else pd.DataFrame(columns=["fraud_type", "count"])
    )
    risk_counts = (
        flagged["risk_band"].astype(str).value_counts().rename_axis("risk_band").reset_index(name="count")
        if not flagged.empty
        else pd.DataFrame(columns=["risk_band", "count"])
    )
    rule_counts = (
        flagged[RULE_COLUMNS]
        .sum()
        .rename_axis("rule_name")
        .reset_index(name="count")
        .assign(rule_name=lambda frame: frame["rule_name"].map(RULE_LABELS))
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
        if not flagged.empty
        else pd.DataFrame(columns=["rule_name", "count"])
    )

    return {
        "metrics": {
            "total_transactions": int(record.get("total_transactions", 0) or 0),
            "volume_usd": float(record.get("volume_usd", 0.0) or 0.0),
            "flagged_transactions": int(record.get("flagged_transactions", 0) or 0),
            "flagged_volume": float(record.get("flagged_volume", 0.0) or 0.0),
            "max_flagged_amount": float(record.get("max_flagged_amount", 0.0) or 0.0),
        },
        "type_counts": type_counts,
        "risk_counts": risk_counts,
        "rule_counts": rule_counts,
        "flagged": flagged,
    }
