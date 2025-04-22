import pandas as pd

# Read the CSV file into a pandas DataFrame
data = pd.read_csv('GradeRosterReport.csv')

# Define the columns that form the primary key
primary_key_columns = ["Student ID", "Subject Code / Name"]

# Check for null values in the primary key columns
print("Number of rows before handling nulls:", len(data))
print("\nChecking for null values in primary key columns:")
print(data[primary_key_columns].isnull().sum())

# Delete rows where ANY of the primary key columns have a null value
data.dropna(subset=primary_key_columns, how='any', inplace=True)

print("\nNumber of rows after deleting rows with null primary key values:", len(data))
print("\nChecking for null values in primary key columns after deletion:")
print(data[primary_key_columns].isnull().sum())

# Store the cleaned data into a new CSV file named 'cleaned_grades.csv'
cleaned_grades_file = 'cleaned_grades.csv'
data.to_csv(cleaned_grades_file, index=False)  # index=False prevents writing the DataFrame index to the CSV

print(f"\nCleaned data has been saved to '{cleaned_grades_file}'.")