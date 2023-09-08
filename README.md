# Adventure Works Orders - BI Modeling

## Overview

This repository contains the code and resources for modeling Adventure Works orders data for Business Intelligence (BI) purposes. Adventure Works is a fictional company often used as a sample dataset in Microsoft SQL Server and other BI tools.

In this project, I aim to provide a comprehensive BI model for Adventure Works orders following Kimball's data warehouse modelling practices, allowing users to perform various analytics and gain insights into sales, customer behavior, and product performance.

## Key Features

### Data Modeling: 
I have designed a data model that organizes Adventure Works orders data efficiently, making it suitable for BI and reporting.
### ETL Processes: 
The repository includes Transformation processes only. Data was extracted using Stitch to extract data from the Adventure Works database, transform it into a structured format, and load it into a BI-friendly data store.
### Documentation: 
Detailed documentation is available on yml files to help you understand the data model, ETL processes, and how to use the provided reports. This is one of the many advantages od dbt, namely: bringing model documentation closer to .sql files.

## Reporting: 
The BI report/dashboard built on top of the modeled data can be found at https://app.powerbi.com/links/p_UR7Q6jAj?ctid=b34c1d55-a43e-4a20-9218-a1a42eab149a&pbi_source=linkShare. 


# The Stack
### Extraction
Stitch was used to extract data

### Loading
Data was loaded on Big Query (Google Cloud's data warehouse solution)

### Transformation
Data was transformed was dbt (Learn more about dbt at: https://www.getdbt.com/dbt-learn) 

## The data modelling process
### Step 1
First, I started out understanding Adventure Works business, their possible questions to be answered, and their data structure

### Step 2
Then, I mapped out the tables I would require to model data to answer such questions.

### Step 3
Afterwards, I build a star-schema model and created the staging files on dbt. I also set the appropriate tools to be able to model this data

### Step 4
Data was modelled and joined in fact tables to be used in Power BI. Additionally, models were appropriately documented.

### Step 5
BI was prototyped and materialized in Power BI.

