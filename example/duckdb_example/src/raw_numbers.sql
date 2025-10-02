create or replace table raw_numbers as
select * from (
    values
        (1, 'apple'),
        (2, 'banana'),
        (3, 'cherry'),
        (4, 'dragonfruit'),
        (5, 'elderberry')
) as t(id, fruit)