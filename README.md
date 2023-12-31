# Task_ETL

Overview
The Task_ETL DAG is designed to handle the ETL process in several steps:

**Extract Data**: Download data from a specified URL.

**Transform Data**: Convert and clean the data, then split it into different dataframes for separate tables.

**Truncate Tables:** Optionally truncate existing tables before loading new data.

**Load Data:** Load the transformed data into the PostgreSQL database.

# REQUIREMENTS

Apache Airflow.

Python 3.x

Libraries: requests, pandas, sqlalchemy
PostgreSQL Database

# DAG Configuration

**DAG ID**: Task_ETL

**Schedule**: Run once on initiation.

**Start Date**: One day before the current date.

**Retry Delay**: 5 minutes


# Database Schema Description for Task_ETL Airflow DAG
This section describes the database 
schema used in the Task_ETL Airflow 
DAG for storing the extracted, 
transformed, and loaded data.
Four primary tables are created:
devices, publishers, trackers, and clicks.
Below is the description of each table along 
with their respective fields and data types.



1. **Devices Table**
Table Name: devices
Purpose: Stores information about various devices from which clicks are generated.
Fields:

**device_id**: (BIGINT, PRIMARY KEY) Unique identifier for each device.

**ios_ifa**: (VARCHAR(255)) Identifier for Advertisers for iOS devices.

**ios_ifv**: (VARCHAR(255)) Identifier for Vendors for iOS devices.

**android_id**: (VARCHAR(255)) Android ID of the device.

**google_aid**: (VARCHAR(255)) Google Advertising ID.

**os_name**: (VARCHAR(10)) Name of the operating system.

**os_version**: (VARCHAR(255)) Version of the operating system.

**device_manufacturer**: (VARCHAR(255)) Manufacturer of the device.

**device_model**: (VARCHAR(255)) Model of the device.

**device_type**: (VARCHAR(10), NOT NULL) Type of the device.

**is_bot**: (BOOLEAN, NOT NULL) Indicates whether the device is a bot.

**country_iso_code**: (VARCHAR(2), NOT NULL) ISO code of the country from where the click originated.

**city**: (VARCHAR(60)) City from where the click originated.

2. **Publishers Table**
Table Name: publishers

Purpose: Contains information about publishers.

Fields:

publisher_id: (BIGINT, PRIMARY KEY) Unique identifier for each publisher.

publisher_name: (VARCHAR(255), NOT NULL) Name of the publisher.

**3. Trackers Table**

Table Name: trackers

Purpose: Stores information about trackers used in the click stream.

Fields:

tracking_id: (BIGINT, PRIMARY KEY) Unique identifier for each tracker.

tracker_name: (TEXT, NOT NULL) Name of the tracker.

4. **Clicks Table**

Table Name: clicks

Purpose: Records details of each click event.

Fields:

**click_id**: (BIGINT) Unique identifier for each click.

**click_ipv6**: (VARCHAR(39)) IPv6 address from which the click was made.

**click_timestamp**: (BIGINT, NOT NULL) Timestamp of the click event.

**click_url_parameters**: (TEXT) URL parameters associated with the click.

**click_user_agent**: (TEXT) User agent of the device at the time of click.

**device_id**: (BIGINT) Foreign key referencing the device_id in the devices table.

**publisher_id**: (BIGINT) Foreign key referencing the publisher_id in the publishers table.

**tracking_id**: (BIGINT) Foreign key referencing the tracking_id in the trackers table.
