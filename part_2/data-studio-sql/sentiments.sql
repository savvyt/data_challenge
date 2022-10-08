-- Author: Tania Savitri
-- Date: 08/10/2022
-- Version: 1.0

-- Get tweet in periode interval between 1, 5, 15 minutes
with tweet_in_periode as (
  SELECT id, user.username, sentiment FROM `<<project-id>>.<<dataset-id>>.<<table-id>>` 
  where DATETIME(CAST( created_at AS TIMESTAMP)) > datetime_sub(current_datetime(), INTERVAL <<insert 1, 5, 15>> minute) -- choose interval between 1, 5, 15 minutes
)
-- Group by sentiment and sort
, count_sentiment as (
  select sentiment
  , count(*) as count_sentiment
  from tweet_in_periode
  group by sentiment
  order by count_sentiment desc
)
-- Get most sentiment from the time periode
select sentiment
from count_sentiment
limit 1