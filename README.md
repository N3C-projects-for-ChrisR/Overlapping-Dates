# Overlapping Visits (a gist really)
I ran into some SQL code using windowing functions that didn't work like I'd expect it to.
Since it was in a context that made simple test files like these awkward, I
wrote this for a simple, self-contained, Python and Spark install on my mac.

Oddly, Postgres seems to do what I would expect: https://www.postgresql.org/docs/current/tutorial-window.html 
Moreso, my experiments in postgres.sh show that it works like Spark where the aggregations are applied to a sliding window and you get a rolling sum or average affect.

Chris Roeder, June 2023


## Running (assuming python3 is installed)
- create an environment
  - python3 -m venv env
- activate it
  - source env/bin/activate
- install the requirements
  - python3 -m pip install -r requirements.txt
- use the script to run the code
  - This just deletes any prior tables using the 'rm' command because i haven't found how to do that from Python.

## Demonstration 1, aggregating over a partition's rows: visits.py
The first thing I used this for was to see how the aggration functions DO NOT aggregate over the entire partition.
Run the code and notice how the selected max and sum values grow with each line within a parition.
To my mind, if you parition on id, you get a sub-group of all those rows and a max(id) or max(start_date) would be
the biggest in the set of rows with the same id. It's not. It's the max of those seen *so* *far*.

## Demonstration 2, finding gaps in dates actually works this way: also visits.py
Since the aggregation functions work the way they do, you can solve the overlapping dates problem.

The overlapping dates problem here is finding the date range that a set of overlapping date-ranges spans.
Clearly, if you have a  range that ends on the same day the next one starts, like 
(2023-01-01, 2023-01-10) and (2023-01-10, 2023-01-11), 
they should combine. I think it's an open question how to deal with
the end date on one day and the start date on the next, like
(2023-01-01, 2023-01-10) and (2023-01-10, 2023-01-11).

(to be continued)

## Demo 3: visits_2.py
This one shows how the LAG function is used to find the gap. When the current row's start_date comes on or before the prior max(start_date), there is no gap, so 0. When it comes after, or if there is no prior max from LAG, there is a gap, so 1. Gap here means you are starting a new block of visits.

visits_2a.py shows how  you can tweak this to accept small gaps. Here I've modified it so it's OK to have a single day gap, where a the first range ends one day, and the next range starts the very next day. If  you run visits_2.py and look at rows with ids 7 and 8, and then compare that output with a run from visits_2a.py, you'll see that the rows with id 7 don't have gaps as they do when run with visits_2.py. The rows with id 8 have 2-day gaps and are marked with gaps in either query.

## Demo visits_3.py 
This is a small step that adds the gaps to create group numbers. No gaps means it's the same group. The presence of gaps breaks a range so you have many groups and an id or group number to acces them.



## To Do
- figure out how to remove or drop a table so I don't need run.sh
- figoure out how to load a table into spark and then query it from a second script so I don't have to repeat all that code. I suspect this has to do with a formal install of Spark rather than the way I'm using it on the fly here.

