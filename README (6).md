
# Data Modeling with Apache Cassandra

### Project Overview

Sparkify, a music streaming startup, has collected data on songs and user activity within their new app. They want to analyze this data to gain insights into user behavior. However, the data is currently stored in CSV files, making it challenging to query and analyze effectively. To address this issue,  a data engineer  creates an Apache Cassandra database and designs a data model that allows for efficient querying. The primary task is to model the data and implement an ETL pipeline in Python to load the data into the Cassandra database.

### Datasets

The dataset of this project calls event_data, which consists of CSV files partitioned by date. Here are examples of file paths within the dataset:

- event_data/2018-11-08-events.csv
- event_data/2018-11-09-events.csv

### Project Template

This project uses the provided Jupyter notebook. The notebook contains the following key components:

1. Data preprocessing to create a denormalized dataset.
2. Designing data tables tailored to specific queries.
3. Writing Apache Cassandra **CREATE KEYSPACE** and **SET KEYSPACE** statements.
4. Developing **CREATE** statements for tables.
5. Loading data into the tables using **INSERT** statements.
6. Implementing the ETL pipeline to process the data from CSV files and insert it into the Cassandra database.
7. Testing the data model by running **SELECT** statements with appropriate **WHERE** clauses.

### Project Steps
#### Modeling the NoSQL Database

1. Design tables to answer the queries specified in the project template.
2. Write Apache Cassandra **CREATE KEYSPACE** and **SET KEYSPACE** statements.
3. Develop **CREATE** statements for each table to address the query requirements.
4. Load the data using **INSERT** statements into the tables, ensuring to include **IF NOT EXISTS** clauses to prevent creating duplicate tables.
5. Consider including **DROP TABLE** statements for each table for easy database reset during testing.
6. Test by running **SELECT** statements with appropriate WHERE clauses.

### Building the ETL Pipeline

1. Implement the logic to iterate through each event file in the **event_data** directory to process and create a new denormalized CSV file using Python.
2. Make necessary edits to the provided notebook to include Apache Cassandra CREATE and **INSERT** statements to load processed records into the relevant tables in data model.
3. Test the ETL pipeline by running **SELECT** statements after loading data into the Cassandra database.

### Queries to Address

#### creating tables and queries to answer the following three questions from the data:

- Query 1: Retrieve the artist, song title, and song length for a specific session and item in the session:
       
- Query 2: Retrieve the artist, song (sorted by item in session), and user (first and last name) for a specific user and session:

- Query 3: Retrieve the user names (first and last) for users who listened to a specific song:

