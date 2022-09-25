# Azure-Modern-Data-Warehouse
Building an Azure Data Lake for Bike Share Data Analytics

## Overview of Project

In this project, you'll build a data lake solution for Divvy bikeshare.

Divvy is a bike sharing program in Chicago, Illinois USA that allows riders to purchase a pass at a kiosk or use a mobile application to unlock a bike at stations around the city and use the bike for a specified amount of time. The bikes can be returned to the same station or to another station. The City of Chicago makes the anonymized bike trip data publicly available for projects like this where we can analyze the data.

Since the data from Divvy are anonymous, we have generated fake rider and account profiles along with fake payment data to go along with the data from Divvy. 

### The goal of this project is to develop a data lake solution using Azure Databricks using a lake house architecture. You will:

* Design a star schema based on the business outcomes below;
* Import the data into Azure Databricks using Delta Lake to create a Bronze data store;
* Create a gold data store in Delta Lake tables;
* Transform the data into the star schema for a Gold data store;

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
* Create an Azure Databricks Workspace
  * Databricks cluster

***Run Terraform Script*** 

```bash
  cd /devops/infrastructure/
  terraform init
  terraform plan
  terraform apply
  cd ../../
```

![Resource Group New](/images/resource_group%20-%20Copy.png)



## Task 2: Design a star schema
You are being provided a relational schema that describes the data as it exists in PostgreSQL. In addition, you have been given a set of business requirements related to the data warehouse. You are being asked to design a star schema using fact and dimension tables.

![Star Schema](/images/udacitysynapse_star.png)

## Task 3: Upload data to Databricks

```bash
  pip install -r requirement.txt
```

```bash
  databricks configure --token
```


```bash
  bash import_to_databricks.sh
```

![Upload](/images/data_in_dbfs.png)

## Task 4: EXTRACT the data from DBFS into Bronze tables
In your Azure Databricks workspace, you will use an ingestion script to extract the uploaded zip files

Unzip zip files
```bash
  python src/databricks/0_unzip_files.py
```

Ingest files into bronze table 
```bash
  python src/databricks/1_ingestion.py
```

## Task 5: Transform the data into Silver tables 

Transform files into silver table 
```bash
  python src/databricks/2_bronze_to_silver.py
```

## Task 6: Populate the data into Gold tables 

Populate data into gold table 
```bash
  python src/databricks/3_silver_to_gold.py
```


