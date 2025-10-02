create or replace table fruit_metrics as
select
    id,
    fruit,
    length(fruit) as name_length,
    id * 10 as score
from raw_numbers
order by id