-- Author: Tania Savitri
-- Date: 06/10/2022
-- Version: 1.0

SELECT
  user_id,
  name,
  email
FROM (
  SELECT
    user_id,
    name,
    email,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY created DESC) AS rn --add column rn to show ranking of record updated date, partitioned by user_id, with latest record having highest rank
  FROM `<<project-id>>.<<dataset>>.<<table>>`) m
WHERE rn = 1