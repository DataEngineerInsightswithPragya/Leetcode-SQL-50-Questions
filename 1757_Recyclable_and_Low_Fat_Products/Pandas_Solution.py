# -- Products table:
# -- +-------------+----------+------------+
# -- | product_id  | low_fats | recyclable |
# -- +-------------+----------+------------+
# -- | 0           | Y        | N          |
# -- | 1           | Y        | Y          |
# -- | 2           | N        | Y          |
# -- | 3           | Y        | Y          |
# -- | 4           | N        | N          |
# -- +-------------+----------+------------+

import pandas as pd

product = {
    "product_id" : [0,1,2,3,4],
    "low_fats" : ['Y','Y','N','Y','N'],
    "recyclable" : ['N','Y','Y','Y','N']
}

product_df = pd.DataFrame(product)
print(product_df)

filter_df = product_df[(product_df['low_fats'] == 'Y') & (product_df['recyclable'] == 'Y')]
print(filter_df)

result_df = filter_df[['product_id']]
print(result_df)