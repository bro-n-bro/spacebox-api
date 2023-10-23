-- FEES PAID API
CREATE MATERIALIZED VIEW spacebox.test_fees_paid
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS select toStartOfHour(b.timestamp) as timestamp, sumState(amount) as y from (
    SELECT
        height,
        JSONExtractString(arrayJoin(JSONExtractArrayRaw(JSONExtractString(fee, 'coins'))), 'denom')  as denom,
        toUInt256OrZero(JSONExtractString(arrayJoin(JSONExtractArrayRaw(JSONExtractString(fee, 'coins'))), 'amount')) as amount
    FROM spacebox.transaction WHERE denom = 'uatom') as sp
    LEFT JOIN (
        SELECT * FROM spacebox.block
    ) AS b ON sp.height = b.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- AVG BLOCK TIME
CREATE MATERIALIZED VIEW spacebox.avg_block_time
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS select toStartOfHour(timestamp) as timestamp_start_of_hour, avgState(lifetime) as y from (
    select
      t1.timestamp as timestamp,
      coalesce(
        timestampdiff(
          SECOND, t1.timestamp, t2.timestamp
        ),
        0
      ) as lifetime
    from
      spacebox.block t1
      left join spacebox.block t2 on t1.height = t2.height - 1
      where lifetime > 0
    order by
      t1.height DESC
    offset 1
)
group by timestamp_start_of_hour
order by timestamp_start_of_hour

-- TOTAL SUPPLY
CREATE MATERIALIZED VIEW spacebox.total_supply
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, (avgState(toFloat64(sp.not_bonded_tokens)) + avgState(toFloat64(sp.bonded_tokens))) AS y
from (
   select * FROM spacebox.staking_pool
) AS sp
LEFT JOIN (
    SELECT * FROM spacebox.block
) AS b ON sp.height = b.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour


-- BONDED TOKENS
CREATE MATERIALIZED VIEW spacebox.bonded_tokens
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, medianState(sp.bonded_tokens) AS y
from (
select * FROM spacebox.staking_pool
) AS sp
LEFT JOIN (
            SELECT * FROM spacebox.block
        ) AS b ON sp.height = b.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- TODO: TIMEOUT ERROR, RECHECK
-- CIRCULATING SUPPLY
CREATE MATERIALIZED VIEW spacebox.circulating_supply
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS select toStartOfHour(b.timestamp) as timestamp_start_of_hour, avgState(JSONExtractFloat(coin, 'amount')) as y from
(
    select arrayJoin(JSONExtractArrayRaw(JSONExtractString(coins))) as coin, height
    from spacebox.supply AS s
    where JSONExtractString(coin, 'denom') = 'uatom'
) as s
LEFT JOIN (SELECT * FROM spacebox.block) AS b ON s.height = b.height
group by timestamp_start_of_hour
order by timestamp_start_of_hour

-- BONDED RATIO
CREATE MATERIALIZED VIEW spacebox.bonded_ratio
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, avgState(bonded_ratio)*100 AS y
from (
select * FROM spacebox.annual_provision
) AS ap
LEFT JOIN (
    SELECT * FROM spacebox.block
) AS b ON ap.height = b.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour


-- COMMUNITY POOL
CREATE MATERIALIZED VIEW spacebox.community_pool
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, avgState(toFloat64(replaceAll(replaceAll(JSON_QUERY(JSONExtractString(coins, -1), '$.amount'), '[', ''), ']', ''))) AS y
from (
select * FROM spacebox.community_pool
) AS cp
LEFT JOIN (
    SELECT * FROM spacebox.block
) AS b ON cp.height = b.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- INFLATION
CREATE MATERIALIZED VIEW spacebox.inflation
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, avgState(inflation) AS y
from (
select * FROM spacebox.annual_provision
) AS ap
LEFT JOIN (
    SELECT * FROM spacebox.block
) AS b ON ap.height = b.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- RESTAKE EXECUTION COUNT
CREATE MATERIALIZED VIEW spacebox.restake_execution_count
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, countState() AS y
from (
select * FROM spacebox.exec_message
) AS dp
LEFT JOIN (
    SELECT * FROM spacebox.block
) AS b ON dp.height = b.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- NEW ACCOUNTS
CREATE MATERIALIZED VIEW spacebox.new_accounts
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, countState() as y
from (
select * FROM spacebox.account
) AS a
LEFT JOIN spacebox.block b ON b.height = a.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- GAS PAID
CREATE MATERIALIZED VIEW spacebox.gas_paid
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, sumState(total_gas) as y
from (
select * FROM spacebox.block
) as b
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- TRANSACTIONS
CREATE MATERIALIZED VIEW spacebox.transactions_view
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, countState() as y
from (
select * FROM spacebox.transaction
) AS t
LEFT JOIN spacebox.block b ON b.height = t.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- REDELEGATION MESSAGE
CREATE MATERIALIZED VIEW spacebox.redelegation_message_view
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, sumState(JSONExtractInt(coin, 'amount')) as y
from (
select * FROM spacebox.redelegation_message
) AS dm
LEFT JOIN (
    SELECT * FROM spacebox.block  FINAL
) AS b ON dm.height = b.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- UNBONDING MESSAGE
CREATE MATERIALIZED VIEW spacebox.unbonding_message_view
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour, sumState(JSONExtractInt(coin, 'amount')) as y
from (
select * FROM spacebox.unbonding_delegation_message
) AS dm
LEFT JOIN (
            SELECT * FROM spacebox.block
        ) AS b ON dm.height = b.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- DELEGATION MESSAGE
CREATE MATERIALIZED VIEW spacebox.delegation_message_view
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT toStartOfHour(b.timestamp) AS timestamp_start_of_hour,sumState(JSONExtractInt(coin, 'amount')) AS y
from (
select * FROM spacebox.delegation_message
) AS dm
LEFT JOIN (
            SELECT * FROM spacebox.block
        ) AS b ON dm.height = b.height
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- ACTIVE ACCOUNTS
CREATE MATERIALIZED VIEW spacebox.active_accounts_view
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS SELECT timestamp_start_of_hour , countState() as y FROM (
    SELECT DISTINCT ON (toStartOfHour(b.timestamp) AS timestamp_start_of_hour, signer AS y) timestamp_start_of_hour, y
    from (
        select * FROM spacebox.transaction
        ) AS t
        LEFT JOIN (
            SELECT * FROM spacebox.block
        ) AS b ON t.height = b.height
)
GROUP BY timestamp_start_of_hour
ORDER BY timestamp_start_of_hour

-- RESTAKE TOKEN AMOUNT
CREATE MATERIALIZED VIEW spacebox.restake_token_amount
ENGINE = AggregatingMergeTree() ORDER BY timestamp_start_of_hour
POPULATE AS select toStartOfHour(timestamp) as timestamp_start_of_hour, sumState(amount) as y from (
SELECT b.timestamp as timestamp, toUInt256OrZero(JSONExtractString(JSONExtractString(_t, 'amount'), 'amount')) as amount  FROM (
    SELECT height, JSONExtractString(arrayJoin(JSONExtractArrayRaw(JSONExtractString(msgs)))) as _t FROM spacebox.exec_message
) as t
left join (select * from spacebox.block) as b on b.height = t.height
)
GROUP by timestamp_start_of_hour
ORDER BY timestamp_start_of_hour







