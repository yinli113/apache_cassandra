#!/usr/bin/env python
# coding: utf-8

# #  ETL with Apache Cassandra
# 
# ## This project is to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ### Import Python packages

# In[1]:


import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


filepath = os.getcwd() + '/event_data'

for root, dirs, files in os.walk(filepath):
    file_path_list = glob.glob(os.path.join(root,'*'))


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


full_data_rows_list = []
for f in file_path_list:
 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
        csvreader = csv.reader(csvfile) 
        next(csvreader)
               
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[4]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[5]:


# make a connection to a Cassandra instance of local machine (127.0.0.1)

from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) 
    session = cluster.connect()
except Exception as e:
    print(e)

# establish connection and begin executing queries
session = cluster.connect()


# #### Create Keyspace

# In[6]:


try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS event_datafile_new 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[7]:


# Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('event_datafile_new')
except Exception as e:
    print(e)


# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# ### Creating a table to answer question 1
# 
# - The rows are uniquely identified by the combination of **sessionId** and **itemInSession**. This will allow me to efficiently retrieve data for a specific session and item in session

# In[8]:


# Creating a table to answer question 1
new_query = "CREATE TABLE IF NOT EXISTS song_library "
new_query = new_query + "(sessionId INT, itemInSession INT,artist_name TEXT, song TEXT, length FLOAT, PRIMARY KEY (sessionId, itemInSession))"
try:
    session.execute(new_query)
except Exception as e:
    print(e) 


# ### Creating a table to answer question 2
# 
# - **userId** and **sessionId** are both used in the WHERE clause of query. Therefore, they should be part of the partition key to efficiently locate the data, sothat all data for a specific userId and sessionId is stored on the same node.
# 
# - **itemInSession** can be a clustering column because it's used for sorting the results. With **itemInSession** as a clustering column, data within the same partition (specific userId and sessionId) is sorted by **itemInSession**.

# In[9]:


query2 = "CREATE TABLE IF NOT EXISTS song_user(                                    sessionId INT,                                    userId INT,                                    itemInSession INT,                                    artist_name TEXT,                                    firstName TEXT,                                    lastName TEXT,                                    song TEXT,                                    PRIMARY KEY((sessionId, userId), itemInSession))"

try:
    session.execute(query2)
except Exception as e:
    print(e)


# ### Creating a table to answer question 3
# 
# - Partition key: Using **song** as the partition key allows me to efficiently retrieve data for a specific song title because all rows with the same song will be stored together on the same node.
# 
# - Clustering Column: using **userId** as a clustering column. This ensures that within a partition (for a specific song), data is sorted by userId. By including userId as a clustering column, I can efficiently retrieve the user names for a specific song.

# In[10]:


# Creating a table to answer question 3
query3 = "CREATE TABLE IF NOT EXISTS user_library ("        "song TEXT,"        "firstName TEXT,"        "lastName TEXT,"        "sessionId INT,"        "itemInSession INT,"         "userId INT,"         "PRIMARY KEY (song, userId))"

try:
    session.execute(query3)
except Exception as e:
    print(e)


# **convert a CSV file into a pandas DataFrame**

# In[11]:


# Read the CSV file into a DataFrame
df = pd.read_csv('event_datafile_new.csv')

# convert np.array into list
i_list = []
for i in df.values:
    i = i.tolist()
    i_list.append(i)


# **Insert data into the tables**

# In[12]:


# insert into table song_library
for line1 in i_list:
    query1 = "INSERT INTO song_library (sessionId, itemInSession,artist_name, song, length) "
    query1 = query1 + "VALUES (%s,%s,%s,%s,%s)"

    session.execute(query1, (line1[8], line1[3],line1[0], line1[9],line1[5]))


# In[13]:


# insert into table song_user
for line2 in i_list:
    query2 = "INSERT INTO song_user (sessionId, userId, itemInSession, artist_name, firstName, lastName, song) "
    query2 = query2 + "VALUES (%s,%s,%s,%s,%s,%s,%s)"

    session.execute(query2, (line2[8],line2[10],line2[3], line2[0],line2[1],line2[4],line2[9]))  


# In[14]:


# insert into table user_library
for line3 in i_list: 
    query3 = "INSERT INTO user_library (song, firstName, lastName, sessionId, itemInSession, userId) "
    query3 = query3 + "VALUES (%s, %s, %s, %s, %s, %s)"
    
    session.execute(query3, (line3[9],line3[1],line3[4],line3[8],line3[3],line2[10]))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[15]:


# answering question1
select_query_1 = "SELECT artist_name, song, length FROM song_library WHERE sessionId = 338 and itemInSession = 4"

try:
    rows = session.execute(select_query_1)
except Exception as e:
    print(e)

# Create a list to hold the results
result_data_1 = []

for row in rows:
    result_data_1.append([row.artist_name, row.song, row.length])

# Create a Pandas DataFrame from the result_data list
df_1 = pd.DataFrame(result_data_1, columns=["artist_name", "song", "length"])

# Print the DataFrame
print(df_1)


# In[16]:


# answering question2
select_query_2 = "SELECT artist_name, song, firstName, lastName FROM song_user WHERE userId = 10 AND sessionId = 182"

try:
    rows = session.execute(select_query_2)
except Exception as e:
    print(e)

# Create a list to hold the results
result_data_2 = []

for row in rows:
    result_data_2.append([row.artist_name, row.song, row.firstname, row.lastname])

# Create a Pandas DataFrame from the result_data list
df_2 = pd.DataFrame(result_data_2, columns=["artist_name", "song", "firstName", "lastName"])

# Print the DataFrame
print(df_2)


# In[17]:


# answering question3
select_query_3 = "SELECT firstname, lastname FROM user_library WHERE song = 'All Hands Against His Own' ALLOW FILTERING"
try:
    rows = session.execute(select_query_3)
except Exception as e:
    print(e)

# Create a list to hold the results
result_data_3 = []

for row in rows:
    result_data_3.append([row.firstname, row.lastname])

# Create a Pandas DataFrame from the result_data list
df_3 = pd.DataFrame(result_data_3, columns=["firstName", "lastName"])

# Print the DataFrame
print(df_3)


# ### Drop the tables before closing out the sessions

# In[19]:


song_library_table_drop = "DROP TABLE IF EXISTS song_library"
song_user_table_drop = "DROP TABLE IF EXISTS song_user"
user_library_table_drop = "DROP TABLE IF EXISTS user_library"
    
drop_table_queries = [
        song_library_table_drop,
        song_user_table_drop,
        user_library_table_drop
]

for query in drop_table_queries:
    try:
        session.execute(query)
    except Exception as e:
        print(e)


# ### Close the session and cluster connection¶

# In[20]:


session.shutdown()
cluster.shutdown()


# In[ ]:




