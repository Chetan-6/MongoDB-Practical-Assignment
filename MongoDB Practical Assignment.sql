# Practical Assignment

use superstore_db;

# Que 1.  Write a Python script to load the Superstore dataset from a CSV file into MongoDB.
# Ans - 

# Python Script: Load Superstore CSV to MongoDB
import pandas as pd
from pymongo import MongoClient

# === Step 1: Load the CSV file ===
file_path = "superstore.csv"   # Change path if needed
df = pd.read_csv(file_path)

# === Step 2: Connect to MongoDB ===
# Update connection string if using MongoDB Atlas
client = MongoClient("mongodb://localhost:27017/")  
db = client["SuperstoreDB"]         # Database name
collection = db["SalesData"]        # Collection name

# === Step 3: Convert DataFrame to Dictionary and Insert ===
data = df.to_dict(orient="records")  # Convert rows to JSON-like dicts
collection.insert_many(data)

print(f"✅ Successfully inserted {len(data)} records into MongoDB collection '{collection.name}'")

# Setup Instructions - 
# 1. Make sure you have MongoDB running locally or on Atlas.
# 2. Install required libraries if not installed:
pip install pandas pymongo
# 3. Save this code in a file like load_superstore_to_mongo.py.
# 4. Run it in your terminal:
python load_superstore_to_mongo.py

# Que 2. Retrieve and print all documents from the Orders collection.
# Ans - Here’s how you can retrieve and print all documents from the Orders collection in MongoDB using Python

# Python Script: Retrieve All Documents from Orders Collection
from pymongo import MongoClient

# === Step 1: Connect to MongoDB ===
client = MongoClient("mongodb://localhost:27017/")  # Change if using MongoDB Atlas
db = client["SuperstoreDB"]          # Database name
collection = db["Orders"]            # Collection name

# === Step 2: Retrieve All Documents ===
documents = collection.find()        # Returns a cursor

# === Step 3: Print Each Document ===
for doc in documents:
    print(doc)

# Notes:
# 	* If you used the earlier script where the collection name was SalesData, replace:
collection = db["Orders"]
# with:
collection = db["SalesData"]
# 	* To make the output more readable, you can pretty-print the results:
import pprint
pprint.pprint(doc)

# Que 3. Count and display the total number of documents in the Orders collection.
# Ans - 
# Mongo Shell / MongoDB Compass:
db.Orders.countDocuments();
# or (older method):
db.Orders.count();

# Python (using PyMongo):
# If you’re using Python, here’s the script:
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Select the database and collection
db = client["SuperstoreDB"]
orders_collection = db["Orders"]

# Count documents
count = orders_collection.count_documents({})
print("Total number of documents in Orders collection:", count)

# Que 4.  Write a query to fetch all orders from the "West" region.
# Ans - To fetch all orders from the "West" region in MongoDB, here’s the query you can use
# MongoDB Shell / Compass Query
db.Orders.find({ Region: "West" });
# This will return all documents (orders) where the field Region is "West".

# Python (using PyMongo)
# If you’re querying from a Python script:
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Select the database and collection
db = client["SuperstoreDB"]
orders_collection = db["Orders"]

# Fetch all orders from the West region
west_orders = orders_collection.find({"Region": "West"})

# Display results
for order in west_orders:
    print(order)

# Que 5. Write a query to find orders where Sales is greater than 500.
# Ans - Here’s how you can find all orders where the Sales value is greater than 500
# MongoDB Shell / Compass Query
db.Orders.find({ Sales: { $gt: 500 } });

# Explanation:
# 	* Sales → field name in your collection
# 	* $gt → MongoDB operator meaning “greater than”
# 	* 500 → value threshold
# This query returns all documents where the Sales field value is greater than 500.

# Python (using PyMongo)
# If you’re running it from Python:
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Select the database and collection
db = client["SuperstoreDB"]
orders_collection = db["Orders"]

# Find orders where Sales > 500
high_sales_orders = orders_collection.find({"Sales": {"$gt": 500}})

# Display the results
for order in high_sales_orders:
    print(order)

# Que 6. Fetch the top 3 orders with the highest Profit.
# Ans - Here’s how you can fetch the top 3 orders with the highest Profit
# MongoDB Shell / Compass Query
db.Orders.find().sort({ Profit: -1 }).limit(3);

# Explanation:
# 	* .find() → fetches all documents
# 	* .sort({ Profit: -1 }) → sorts by Profit in descending order (-1 = highest first)
# 	* .limit(3) → returns only the top 3 results

# Python (using PyMongo)
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Select the database and collection
db = client["SuperstoreDB"]
orders_collection = db["Orders"]

# Fetch top 3 orders with highest Profit
top_orders = orders_collection.find().sort("Profit", -1).limit(3)

# Display results
for order in top_orders:
    print(order)

# Que 7. Update all orders with Ship Mode as "First Class" to "Premium Class."
# Ans - Here’s how you can do it in both Python (using pandas) and SQL, depending on your setup:
# Python (pandas) Method
import pandas as pd

# Load the CSV file
df = pd.read_csv("superstore.csv")

# Update "Ship Mode" values
df.loc[df["Ship Mode"] == "First Class", "Ship Mode"] = "Premium Class"

# Save the updated file (optional)
df.to_csv("superstore_updated.csv", index=False)

print("Updated all 'First Class' to 'Premium Class' successfully.")

# This code:
# 	* Reads your CSV into a DataFrame
# 	* Finds rows where "Ship Mode" = "First Class"
# 	* Replaces them with "Premium Class"
# 	* Saves the modified file

# SQL Method
# If your data is stored in a SQL table named orders:
UPDATE orders
SET Ship_Mode = 'Premium Class'
WHERE Ship_Mode = 'First Class';
# This command updates all matching rows in the database.

# Que 8.  Delete all orders where Sales is less than 50.
# Ans - Here’s how you can delete all orders where Sales < 50, in both Python (pandas) and SQL
# Python (pandas) Method
import pandas as pd

# Load the CSV file
df = pd.read_csv("superstore.csv")

# Delete (filter out) rows where Sales < 50
df = df[df["Sales"] >= 50]

# Save the updated file (optional)
df.to_csv("superstore_updated.csv", index=False)

print("Deleted all orders where Sales < 50 successfully.")

# Explanation:
# 	* Reads the CSV into a DataFrame
# 	* Keeps only rows where Sales ≥ 50
# 	* Writes the updated data to a new file

# SQL Method
# If your data is in a SQL table named orders:
DELETE FROM orders
WHERE Sales < 50;
# This removes all rows where the Sales value is less than 50.

# Que 9.  Use aggregation to group orders by Region and calculate total sales per region.
# Ans - Here’s how you can group orders by Region and calculate total sales per region using both Python (pandas) and SQL
# Python (pandas) Method
import pandas as pd

# Load the CSV file
df = pd.read_csv("superstore.csv")

# Group by Region and calculate total Sales
region_sales = df.groupby("Region")["Sales"].sum().reset_index()

# Rename columns for clarity (optional)
region_sales.columns = ["Region", "Total Sales"]

print(region_sales)

# This will output something like:
      Region      Total Sales
0  Central        125678.45
1  East           145987.12
2  South          118763.90
3  West           167543.25

# SQL Method
# If your table is named orders:
SELECT Region, SUM(Sales) AS Total_Sales
FROM orders
GROUP BY Region;
# This query returns each Region with the total Sales in that region.

# Que 10. Fetch all distinct values for Ship Mode from the collection.
# Ans - Here’s how you can fetch all distinct Ship Mode values using both Python (pandas) and SQL
# Python (pandas) Method
import pandas as pd

# Load the CSV file
df = pd.read_csv("superstore.csv")

# Get distinct Ship Mode values
distinct_ship_modes = df["Ship Mode"].unique()

print(distinct_ship_modes)

# Output Example:
['Second Class' 'Standard Class' 'Premium Class' 'Same Day']

# SQL Method
# If your table name is orders:
SELECT DISTINCT Ship_Mode
FROM orders;
# This will return one row for each unique shipping mode in your data.

# Que 11. Count the number of orders for each category.
# Ans - Here’s how you can count the number of orders for each Category using both Python (pandas) and SQL
# Python (pandas) Method
import pandas as pd

# Load the CSV file
df = pd.read_csv("superstore.csv")

# Count number of orders for each Category
category_counts = df["Category"].value_counts().reset_index()

# Rename columns for clarity (optional)
category_counts.columns = ["Category", "Number of Orders"]

print(category_counts)

# Example output:
      Category      Number of Orders
0  Office Supplies        6026
1  Furniture              2121
2  Technology             1813

# SQL Method
# If your table is named orders:
SELECT Category, COUNT(*) AS Number_of_Orders
FROM orders
GROUP BY Category;
# This query returns each Category and the total number of orders in that category.




