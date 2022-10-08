SELECT 
  data 
from 
  `extended-study-364220.part2.count` 
where 
  publish_time = (
    select 
      max(publish_time) 
    FROM 
      `extended-study-364220.part2.count`
  )