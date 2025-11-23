create or replace table main.python_consumer as
select
    src.id,
    src.payload,
    upper(src.payload) as payload_upper
from main.python_source_table src;
