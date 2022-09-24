# Azure-Modern-Data-Warehouse
Building an Azure Data Warehouse for Bike Share Data Analytics. Project to load data from Postgres DB in Azure into Azure Synapse.

## Overview of Project

Divvy is a bike sharing program in Chicago, Illinois USA that allows riders to purchase a pass at a kiosk or use a mobile application to unlock a bike at stations around the city and use the bike for a specified amount of time. The bikes can be returned to the same station or to another station. The City of Chicago makes the anonymized bike trip data publicly available for projects like this where we can analyze the data.

Since the data from Divvy are anonymous, we have created fake rider and account profiles along with fake payment data to go along with the data from Divvy


The goal of this project is to develop a data warehouse solution using Azure Synapse Analytics. You will:



### Design a star schema based on the business outcomes listed below;
* Import the data into Synapse;
* Transform the data into the star schema;
* and finally, view the reports from Analytics.

### The business outcomes you are designing for are as follows:
* Analyze how much time is spent per ride
    * Based on date and time factors such as day of week and time of day
    * Based on which station is the starting and / or ending station
    * Based on age of the rider at time of the ride
    * Based on whether the rider is a member or a casual rider

* Analyze how much money is spent
    * Per month, quarter, year
    * Per member, based on the age of the rider at account start

* EXTRA CREDIT - Analyze how much money is spent per member
    * Based on how many rides the rider averages per month
    * Based on how many minutes the rider spends on a bike per month

## Data

- name: rider.csv
  schema: |
    rider_id int primary key not null,
    address varchar(255),
    first varchar(255),
    last varchar(255),
    birthday date,
    account_number int foreign key not null

- name: account.csv
  schema: |
    account_number int primary key not null,
    member bool,
    start_date date,
    end_date date

- name: payment.csv
  schema: |
    payment_id int primary key not null,
    date date,
    amount decimal, 
    account_number int foreign key not null

- name: trip.csv
  schema: |
    trip_id int primary key not null,
    rideable_type string, 
    started_at datetime,
    ended_at datetime,
    start_station_id int foreign key not null, 
    end_station_id int foreign key not null, 
    member_id int foreign key not null

- name: station.csv
  schema: |
    station_id int primary key not null,
    name string,
    longitude float,
    latitude float

![Divy ERD](/images/divvy_erd.png)


## Task 1: Create your Azure resources
* Create an Azure PostgreSQL database
* Create an Azure Synapse workspace
* Create a Dedicated SQL Pool and database within the Synapse workspace

***Run Terraform Script*** 

```bash
  cd /devops/infrastructure/
  terraform init
  terraform plan
  terraform apply
  cd ../../
```

![Resource Group](/images/resource_group.png)



## Task 2: Design a star schema
You are being provided a relational schema that describes the data as it exists in PostgreSQL. In addition, you have been given a set of business requirements related to the data warehouse. You are being asked to design a star schema using fact and dimension tables.

![Star Schema](/images/star_schema_design.png)

## Task 3: Create the data in PostgreSQL
To prepare your environment for this project, you first must create the data in PostgreSQL. This will simulate the production environment where the data is being used in the OLTP system. 

***Run the ETL setup script to import data into Azure Postgres***


Create virtual env

```bash
    python -m venv venv
```

Activate virtual env 

```bash
    venv/Scripts/activate
```

Install required packages

```bash
  pip install -r requirement.txt    
```

Allow access to the database

![Firewall](/images/postgres_firewall.png)

Run ETL script to set up Postgres Database 
```bash
  python src/etl_postgres/etl.py
```

Verify Postgres Database is populated
```bash
  python src/etl_postgres/test.py
```


## Task 4: EXTRACT the data from PostgreSQL
In your Azure Synapse workspace, you will use the ingest wizard to create a one-time pipeline that ingests the data from PostgreSQL into Azure Blob Storage. This will result in all four tables being represented as text files in Blob Storage, ready for loading into the data warehouse.

![Copy1](/images/copy_data.png)

![staging](/images/staging_data.png)


## Task 5: LOAD the data into external tables in the data warehouse
Once in Blob storage, the files will be shown in the data lake node in the Synapse Workspace. From here, you can use the script generating function to load the data from blob storage into external staging tables in the data warehouse you created using the Dedicated SQL Pool.


![CopyAndLoad](/images/copy_and_load.png)

![Data](/images/loaded_staging_tables.png)



## Task 6: TRANSFORM the data to the star schema
You will write SQL scripts to transform the data from the staging tables to the final star schema you designed.


Transform the data into the Star schema
```bash
  python src/etl_synapse/etl.py
```

## Analysis: Extras
* Based on how many rides the rider averages per month


```sql
  SELECT SUM(t.trip_counter), 
  t.user_id, 
  d.month
  FROM [star].[Trip] t
  JOIN [star].[Date] d
  ON t.start_at_date_id = d.date_id
  GROUP BY t.user_id, d.month
```

* Based on how many minutes the rider spends on a bike per month

```sql
  SELECT SUM(t.total_trip_time_seconds)/60, 
  t.user_id, 
  d.month
  FROM [star].[Trip] t
  JOIN [star].[Date] d
  ON t.start_at_date_id = d.date_id
  GROUP BY t.user_id, d.month
```