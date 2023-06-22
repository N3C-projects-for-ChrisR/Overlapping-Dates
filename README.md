# Overlapping Visits
I ran into some SQL code using windowing functions that didn't work like I'd expect it to.
Since it was in a context that made simple test files like these awkward, I
wrote this for a simple, self-contained, Python and Spark install on my mac.

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

## Demonstration 1, aggregating over a partition's rows
The first thing I used this for was to see how the aggration functions DO NOT aggregate over the entire partition.
Run the code and notice how the selected max and sum values grow with each line within a parition.
To my mind, if you parition on id, you get a sub-group of all those rows and a max(id) or max(start_date) would be
the biggest in the set of rows with the same id. It's not. It's the max of those seen *so* *far*.

## Demonstration 2, finding gaps in dates actually works this way
Since the aggregation functions work the way they do, you can solve the overlapping dates problem.

The overlapping dates problem here is finding the date range that a set of overlapping date-ranges spans.
Clearly, if you have a  range that ends on the same day the next one starts, like 
(2023-01-01, 2023-01-10) and (2023-01-10, 2023-01-11), 
they should combine. I think it's an open question how to deal with
the end date on one day and the start date on the next, like
(2023-01-01, 2023-01-10) and (2023-01-10, 2023-01-11).

(to be continued)


