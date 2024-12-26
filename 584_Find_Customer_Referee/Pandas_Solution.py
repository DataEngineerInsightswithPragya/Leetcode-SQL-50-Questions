# Input:
# Customer table:
# +----+------+------------+
# | id | name | referee_id |
# +----+------+------------+
# | 1  | Will | null       |
# | 2  | Jane | null       |
# | 3  | Alex | 2          |
# | 4  | Bill | null       |
# | 5  | Zack | 1          |
# | 6  | Mark | 2          |
# +----+------+------------+
# Output:
# +------+
# | name |
# +------+
# | Will |
# | Jane |
# | Bill |
# | Zack |
# +------+

import pandas as pd

data = {
    "id" : [1,2,3,4,5,6],
    "name" : ['Will','Jane','Alex','Bill','Zack','Mark'],
    "referee_id": [None, None, 2, None, 1, 2]
}

Customer = pd.DataFrame(data)
print(Customer)

filter_df = Customer[(Customer['referee_id'] !=2) | (Customer['referee_id'].isnull())]
print(filter_df)

result_df = filter_df[['name']]
print(result_df)
