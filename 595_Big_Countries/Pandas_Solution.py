# --Input:
# --World table:
# --+-------------+-----------+---------+------------+--------------+
# --| name        | continent | area    | population | gdp          |
# --+-------------+-----------+---------+------------+--------------+
# --| Afghanistan | Asia      | 652230  | 25500100   | 20343000000  |
# --| Albania     | Europe    | 28748   | 2831741    | 12960000000  |
# --| Algeria     | Africa    | 2381741 | 37100000   | 188681000000 |
# --| Andorra     | Europe    | 468     | 78115      | 3712000000   |
# --| Angola      | Africa    | 1246700 | 20609294   | 100990000000 |
# --+-------------+-----------+---------+------------+--------------+
# --Output:
# --+-------------+------------+---------+
# --| name        | population | area    |
# --+-------------+------------+---------+
# --| Afghanistan | 25500100   | 652230  |
# --| Algeria     | 37100000   | 2381741 |
# --+-------------+------------+---------+

import pandas as pd

data = {
    "name": ["Afghanistan", "Albania", "Algeria", "Andorra", "Angola"],
    "continent": ["Asia", "Europe", "Africa", "Europe", "Africa"],
    "area": [652230, 28748, 2381741, 468, 1246700],
    "population": [25500100, 2831741, 37100000, 78115, 20609294],
    "gdp": [20343000000, 12960000000, 188681000000, 3712000000, 100990000000]
}

world = pd.DataFrame(data)
print(world)

filter_df = world[(world['area'] >= 3000000) | (world['population'] >= 25000000)]
print(filter_df)

result_df = filter_df[['name','population','area']]
print(result_df)

