# --Views table:
# --+------------+-----------+-----------+------------+
# --| article_id | author_id | viewer_id | view_date  |
# --+------------+-----------+-----------+------------+
# --| 1          | 3         | 5         | 2019-08-01 |
# --| 1          | 3         | 6         | 2019-08-02 |
# --| 2          | 7         | 7         | 2019-08-01 |
# --| 2          | 7         | 6         | 2019-08-02 |
# --| 4          | 7         | 1         | 2019-07-22 |
# --| 3          | 4         | 4         | 2019-07-21 |
# --| 3          | 4         | 4         | 2019-07-21 |
# --+------------+-----------+-----------+------------+
# --Output:
# --+------+
# --| id   |
# --+------+
# --| 4    |
# --| 7    |
# --+------+
import pandas as pd

# Define the Views DataFrame
data = {
    "article_id": [1, 1, 2, 2, 4, 3, 3],
    "author_id": [3, 3, 7, 7, 7, 4, 4],
    "viewer_id": [5, 6, 7, 6, 1, 4, 4],
    "view_date": ["2019-08-01", "2019-08-02", "2019-08-01", "2019-08-02", "2019-07-22", "2019-07-21", "2019-07-21"]
}

views_df = pd.DataFrame(data)

# Step 1: Filter rows where author_id equals viewer_id
filter_df = views_df[views_df["author_id"] == views_df["viewer_id"]]
print("Filtered DataFrame:")
print(filter_df)
print(type(filter_df))  # Should be a DataFrame

# Step 2: Drop duplicates from author_id
drop_dup_df = filter_df["author_id"].drop_duplicates()
print(drop_dup_df)
print(type(drop_dup_df))  # Should be a DataFrame

# Step 3: Sort the values
asc_df = drop_dup_df.sort_values()
print(asc_df)
print(type(asc_df))  # Should be a DataFrame

# Step 4: Rename column using rename function
renamed_df = asc_df.to_frame().rename(columns={"author_id": "id"})
print("\nFinal DataFrame:")
print(renamed_df)
print(type(renamed_df))  # Should be a DataFrame

