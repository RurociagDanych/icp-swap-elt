{{ config(severity='warn') }}

-- The canister archive and the REST backfill hash the same swap differently, so the
-- ~6.25 h overlap window (seam) is reconciled by content in seam_reconciliation.py: same
-- pool, ledgers, action and amounts with timestamps at most DEFAULT_TOLERANCE_SECONDS (2 s)
-- apart. This check applies exactly that rule and returns every such pair still stored.
--
-- Severity is WARN, deliberately: the reconciliation drops a B row only for a positive 1:1
-- match against an A row, so two genuine, content-identical trades a few seconds apart in
-- one feed are legitimately kept as two rows. The loaded columns cannot tell such a pair
-- from a missed A/B duplicate, and an ERROR on immutable facts would fail every later run
-- forever with no remediation. A returned pair is a lead to investigate via lineage_ref.
with seam as (
    select event_id, pool_id, token0_ledger_id, token1_ledger_id, action,
           token0_amount, token1_amount, event_timestamp
    from {{ source('silver', 'icp_swap_events') }}
    where event_timestamp >= timestamptz '2026-06-07 00:22:33+00'
      and event_timestamp <= timestamptz '2026-06-07 06:37:26+00'
)
select a.event_id as first_event_id, b.event_id as second_event_id,
       a.pool_id, a.action, a.event_timestamp as first_ts, b.event_timestamp as second_ts
from seam a
join seam b
  on a.event_id < b.event_id
 and a.pool_id = b.pool_id
 and a.token0_ledger_id = b.token0_ledger_id
 and a.token1_ledger_id = b.token1_ledger_id
 and a.action = b.action
 and a.token0_amount = b.token0_amount
 and a.token1_amount = b.token1_amount
 and abs(epoch(a.event_timestamp) - epoch(b.event_timestamp)) <= 2
