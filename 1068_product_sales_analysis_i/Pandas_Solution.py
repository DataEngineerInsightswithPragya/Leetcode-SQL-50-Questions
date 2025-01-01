# --Sales table:
# --+---------+------------+------+----------+-------+
# --| sale_id | product_id | year | quantity | price |
# --+---------+------------+------+----------+-------+
# --| 1       | 100        | 2008 | 10       | 5000  |
# --| 2       | 100        | 2009 | 12       | 5000  |
# --| 7       | 200        | 2011 | 15       | 9000  |
# --+---------+------------+------+----------+-------+
# --Product table:
# --+------------+--------------+
# --| product_id | product_name |
# --+------------+--------------+
# --| 100        | Nokia        |
# --| 200        | Apple        |
# --| 300        | Samsung      |
# --+------------+--------------+
# --Output:
# --+--------------+-------+-------+
# --| product_name | year  | price |
# --+--------------+-------+-------+
# --| Nokia        | 2008  | 5000  |
# --| Nokia        | 2009  | 5000  |
# --| Apple        | 2011  | 9000  |
# --+--------------+-------+-------+


import pandas as pd

sales_data = {
    "sale_id": [1, 2, 7],
    "product_id": [100, 100, 200],
    "year": [2008, 2009, 2011],
    "quantity": [10, 12, 15],
    "price": [5000, 5000, 9000]
}

product_data = {
    "product_id": [100, 200, 300],
    "product_name": ["Nokia", "Apple", "Samsung"]
}

sales = pd.DataFrame(sales_data)
product = pd.DataFrame(product_data)

print(sales)
print(product)

joined_df = pd.merge(sales, product, on='product_id', how='inner')
print(joined_df)

result_df = joined_df[['product_name', 'year', 'price']]
print(result_df)