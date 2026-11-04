{{ config(severity='warn') }}

-- Rows the transformation could not fully resolve, or whose event_id arrived with two
-- different payloads. Reported (not fatal): each returned row is one flagged event.
select event_id, quality_status
from {{ source('silver', 'icp_swap_events') }}
where quality_status in ('unresolved', 'conflict')
