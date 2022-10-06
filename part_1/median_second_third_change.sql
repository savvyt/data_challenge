-- Author: Tania Savitri
-- Date: 06/10/2022
-- Version: 1.0

WITH
   -- sort records by column rn to show ranking of record updated date, partitioned by user_id, with earliest record having highest rank
  sorted AS (
  SELECT
    user_id,
    created,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY created ASC) AS rn 
  FROM
    `<<project-id>>.<<dataset>>.<<table>>` ),

    -- get second record change
  second AS (
  SELECT *
  FROM sorted
  WHERE rn = 2 ),
 
   -- get third record change
  third AS (
  SELECT *
  FROM sorted
  WHERE rn = 3 )

   -- using built in function TIMESTAMP_DIFF to get difference between the two record change dates, divide by two to get median, then cast the result into seconds. Using built in function TIMESTAMP_ADD, add the result in seconds onto the second change date
SELECT
  s.user_id,
  TIMESTAMP_ADD(s.created,INTERVAL CAST((TIMESTAMP_DIFF(t.created, s.created, second)/2) AS int64) second) AS median
FROM second s
JOIN third t
ON s.user_id = t.user_id