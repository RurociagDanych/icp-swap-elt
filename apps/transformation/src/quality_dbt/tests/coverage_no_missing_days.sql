{{ config(severity='warn') }}

-- Calendar days (UTC) without a single event between the first and the last event.
-- Every returned row is one missing day.
with days as (
    select distinct cast(event_timestamp at time zone 'UTC' as date) as d
    from {{ source('silver', 'icp_swap_events') }}
    where event_timestamp is not null
),
bounds as (
    select min(d) as first_day, max(d) as last_day from days
),
calendar as (
    select cast(unnest(generate_series(first_day, last_day, interval 1 day)) as date) as d
    from bounds
    where first_day is not null
)
select calendar.d as missing_day
from calendar
left join days using (d)
where days.d is null
order by 1
