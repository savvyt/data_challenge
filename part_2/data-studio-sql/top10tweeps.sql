-- Author: Tania Savitri
-- Date: 08/10/2022
-- Version: 1.0

-- Get tweet in periode interval between 1, 5, 15 minutes
with tweet_in_periode as (
  SELECT id, user.username, sentiment FROM `<<project-id>>.<<dataset-id>>.<<table-id>>` 
  where DATETIME(CAST( created_at AS TIMESTAMP)) > datetime_sub(current_datetime(), INTERVAL 60 minute) -- choose interval between 1, 5, 15 minutes
)
-- Group by username and sort
, quantity_per_user as (
  select username
  , count(*) as count_tweet
  from tweet_in_periode
  group by username
  order by count_tweet desc
)
-- Get top 10 tweeps from the time periode
select username
from quantity_per_user
limit 10