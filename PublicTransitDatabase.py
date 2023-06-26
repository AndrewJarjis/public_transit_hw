#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import psycopg2
from datetime import datetime


# In[3]:



# Read New York data
df = pd.read_csv('CTA_-_Ridership_-_Daily_Boarding_Totals.csv')
df.tail()




# In[4]:


# Read Chicago data
url_chicago = "https://data.cityofchicago.org/api/views/6iiy-9s97/rows.csv"
chicago_data = pd.read_csv(url_chicago, delimiter=',', quotechar='"')
chicago_data.tail()


# In[6]:


#transforming newyork data


new_york_data_transformed = df[['service_date', 'total_rides']].copy()
new_york_data_transformed.columns = ['month', 'ridership_count']
new_york_data_transformed['city'] = 'New York'
new_york_data_transformed['transportation_type'] = 'metro' 

# Display the transformed New York data
print(new_york_data_transformed.head())


# In[7]:


# Transform Chicago data
chicago_data_transformed = chicago_data[['service_date', 'total_rides']].copy()
chicago_data_transformed.columns = ['month', 'ridership_count']
chicago_data_transformed['city'] = 'Chicago'
chicago_data_transformed['transportation_type'] = 'bus' 
# Display the transformed Chicago data
print(chicago_data_transformed.head())


# In[8]:


monthly_totals = pd.concat([df, chicago_data], ignore_index=True)


# In[10]:


import psycopg2
import pandas as pd

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="DEHW",
    user="postgres",
    password="Rigatoni123"
)


# Create the monthly_totals table
create_monthly_totals_table_query = '''
CREATE TABLE IF NOT EXISTS monthly_totals (
    id SERIAL PRIMARY KEY,
    month DATE,
    city TEXT,
    transportation_type INTEGER,
    ridership_count INTEGER
);
'''

# Create the bus_monthly_totals table
create_bus_monthly_totals_table_query = '''
CREATE TABLE IF NOT EXISTS bus_monthly_totals (
    id SERIAL PRIMARY KEY,
    month DATE,
    city TEXT,
    ridership_count INTEGER
);
'''

# Create the metro_monthly_totals table
create_metro_monthly_totals_table_query = '''
CREATE TABLE IF NOT EXISTS metro_monthly_totals (
    id SERIAL PRIMARY KEY,
    month DATE,
    city TEXT,
    ridership_count INTEGER
);
'''

# Execute the table creation queries
with conn.cursor() as cursor:
    cursor.execute(create_monthly_totals_table_query)
    cursor.execute(create_bus_monthly_totals_table_query)
    cursor.execute(create_metro_monthly_totals_table_query)
    conn.commit()

# Define a dictionary to map transportation types to numeric codes
transportation_types = {'bus': 1, 'metro': 2}

# Transform the Chicago data
chicago_data_transformed = chicago_data[['service_date', 'total_rides']].copy()
chicago_data_transformed.columns = ['month', 'ridership_count']
chicago_data_transformed['city'] = 'Chicago'
chicago_data_transformed['transportation_type'] = transportation_types['bus']

# Transform the New York data
new_york_data_transformed = df[['service_date', 'total_rides']].copy()
new_york_data_transformed.columns = ['month', 'ridership_count']
new_york_data_transformed['city'] = 'New York'
new_york_data_transformed['transportation_type'] = transportation_types['metro']

# Concatenate the transformed data
data = pd.concat([chicago_data_transformed, new_york_data_transformed], ignore_index=True)

# Convert ridership_count to integer
data['ridership_count'] = data['ridership_count'].astype(int)

# Load data into the monthly_totals table
insert_query = '''
INSERT INTO monthly_totals (month, city, transportation_type, ridership_count)
VALUES (%s, %s, %s, %s);
'''

data_values = [(row['month'], row['city'], row['transportation_type'], row['ridership_count']) for _, row in data.iterrows()]

with conn.cursor() as cursor:
    cursor.executemany(insert_query, data_values)
    conn.commit()

# Close the connection
conn.close()



