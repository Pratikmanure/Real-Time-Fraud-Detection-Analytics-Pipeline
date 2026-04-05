# SQL Window Function Rules

The fraud analytics layer relies on window functions rather than simple group-bys so the rules can evaluate temporal behavior per card:

```sql
LAG(event_timestamp) OVER (PARTITION BY card_id ORDER BY event_timestamp)
```

Used to compare the current swipe against the immediately previous swipe for the same card.

```sql
COUNT(*) OVER (
    PARTITION BY card_id
    ORDER BY event_epoch
    RANGE BETWEEN 900 PRECEDING AND CURRENT ROW
)
```

Counts how many times the same card was used in the trailing 15 minutes.

```sql
AVG(amount) OVER (
    PARTITION BY card_id
    ORDER BY event_timestamp
    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
)
```

Builds a rolling baseline so a spend can be compared against that cardholder's recent behavior instead of a static threshold.

## Flag logic

- `city_jump_flag`: same card, different city, less than or equal to 60 minutes apart
- `velocity_flag`: four or more uses in 15 minutes
- `amount_spike_flag`: transaction is at least 3x the rolling baseline and at least $250
- `merchant_burst_flag`: three or more uses at the same merchant within 10 minutes
- `cross_border_flag`: transaction occurred outside the cardholder home country
- `device_change_flag`: new online device appears within 30 minutes of previous activity
- `channel_switch_flag`: rapid switch between card-present and online channels
