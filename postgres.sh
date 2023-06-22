#!/usr/bin/env bash

psql -c "DROP TABLE if exists visits" 
psql -c "CREATE TABLE visits ( id INT, start_date DATE, end_date DATE, sub_id INT)" 
psql -c "\copy visits FROM 'visits.csv' with DELIMITER ',' " 

psql -c "select * from visits"

SQL="select *, \
         rank() over(partition by id order by sub_id) as idx, \
         sum(sub_id) over(partition by id order by sub_id) as sub_id_sum, \
         avg(sub_id) over(partition by id order by sub_id) as sub_id_avg, \
         max(sub_id) over(partition by id order by sub_id) as sub_id_max \
         from visits"

echo "$SQL"

psql -c "$SQL"
