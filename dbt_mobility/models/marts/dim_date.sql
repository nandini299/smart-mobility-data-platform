with days as (
    select d as date_day
    from unnest(
        sequence(
            date '2023-01-01',
            date '2026-12-31',
            interval '1' day
        )
    ) as t (d)
)

select
    cast(date_format(date_day, '%Y%m%d') as integer) as date_key,
    cast(date_day as date)                         as date_day,
    year(date_day)                                as year,
    quarter(date_day)                             as quarter,
    month(date_day)                               as month,
    day(date_day)                                 as day_of_month,
    day_of_week(date_day)                         as day_of_week,
    date_format(date_day, '%W')                    as day_name,
    day_of_week(date_day) in (6, 7)               as is_weekend
from days
