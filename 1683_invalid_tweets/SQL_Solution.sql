--Input:
--Tweets table:
--+----------+-----------------------------------+
--| tweet_id | content                           |
--+----------+-----------------------------------+
--| 1        | Let us Code                       |
--| 2        | More than fifteen chars are here! |
--+----------+-----------------------------------+
--Output:
--+----------+
--| tweet_id |
--+----------+
--| 2        |
--+----------+

select
    tweet_id
from Tweets
where length(content) > 15