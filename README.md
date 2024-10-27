# Data warehouse teck stack with PostgreSQL, Airflow, dbt, and Powerbi

## Project Overview
This project focuses on analyzing vehicle trajectories with SQL and Python. The analysis covers calculations of each vehicle's maximum and minimum speeds, quickest times, and total distance traveled. Additionally, dbt is utilized for data transformation and to create lineage graphs.

### Data

pNEUMA is a comprehensive, large-scale dataset containing naturalistic trajectories for half a million vehicles, collected by a fleet of drones and fixed roadside cameras in the busy downtown area of Athens, Greece.

The vehicle trajectory data is derived from traffic footage analysis, with each file corresponding to a specific (area, date, time) and averaging around 87 MB in size.

## Tools to be used

### PostgreSQL
PostgreSQL is a robust, open-source object-relational database system that builds on the SQL language with additional features to securely handle and scale complex data workloads. In this project, PostgreSQL serves as the data warehouse for storing vehicle trajectory data extracted from footage captured by drone swarms and stationary roadside cameras. The data is ingested into PostgreSQL from a data source (such as a CSV file) via an Airflow task.

### Airflow
Apache Airflow is an open-source platform designed for programmatically creating, scheduling, and monitoring workflows. In this project, Airflow is utilized to schedule and automate data pipeline tasks, such as loading data into PostgreSQL and executing transformation scripts with dbt. Each Airflow task is defined as a Python function and structured within a Directed Acyclic Graph (DAG), which outlines the task sequence and dependencies.

### dbt (Data Build Tool)
dbt is a command-line tool that helps data analysts and engineers perform data transformations more efficiently within their data warehouses. In this project, dbt is used to transform data stored in PostgreSQL by running SQL scripts defined in the dbt project. These scripts handle tasks like aggregation, joining, filtering, and data cleaning. Once transformed, the data is reloaded into PostgreSQL for analysis. dbt also includes features for data testing (to verify it meets specific criteria) and documenting data models.

### PowerBI
PowerBI helps you make sense of your data by allowing you to connect and query your data sources, build dashboards to visualize data, and share them with your company. In this project, PowerBI is used to create visualizations and dashboards from the transformed data. It connects to your PostgreSQL database, allows you to visualize the data imported from the different views created by dbt to visualizations in various formats (like tables, line charts, bar charts, etc.).