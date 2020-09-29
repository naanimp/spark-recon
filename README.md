1) Count the number of rows inserted and check that they match what you are expecting
2) Check columns based on values; for example, count the number of rows that have NULLs in particular columns
3) Sum the amount fields if there are any


Clarification:

1) File based - input, target and database table

In linux only
database - common table sql-server

2) Do we need to copy it to the HDFS or local is fine?

No need to copy HDFS

3) Src files are available in same location or do you have any base path for each file
----

4) Is same applies for destination file as well
---

5) Do we need to process sheets based on files type?
-- Src-only one sheet -- need to confirm the name
-- target --



TOOD

PG - run_history

-- Join exception file with target data
-- run-history joined with metadata
-- Run the job for each records (nothing but each file)
-- Each file have it own key columns to compare.

========================================================

 --- TODO ---
 - Select columns which are required for sql server ingestion

 - Code read from arg.
 - Should single job at a time.

 - Sql server table stru. -- S
 - postgress run status table. - filename, run_status, run_date, description, last_updated_date.


 - Script for spark run job.

