import pandas as pd

# Create the Employees table
employees_data = {
    "id": [1, 7, 11, 90, 3],
    "name": ["Alice", "Bob", "Meir", "Winston", "Jonathan"]
}

employees_df = pd.DataFrame(employees_data)

# Create the EmployeeUNI table
employee_uni_data = {
    "id": [3, 11, 90],
    "unique_id": [1, 2, 3]
}

employee_uni_df = pd.DataFrame(employee_uni_data)

# Display the DataFrames
print("Employees Table:")
print(employees_df)

print("\nEmployeeUNI Table:")
print(employee_uni_df)

joined_df = pd.merge(employees_df,employee_uni_df,on ="id",how = "left")
print(joined_df)

result_df = joined_df[['unique_id','name']]
print(result_df)

