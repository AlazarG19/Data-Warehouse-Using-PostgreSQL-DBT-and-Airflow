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
PowerBI helps you make sense of your data by allowing you to connect and query your data sources, build dashboards to visualize data, and share them with your company. In this project, PowerBI is used to create visualizations and dashboards from the transformed data. It connects to your PostgreSQL database, allows you to visualize the data imported from the different views created by dbt to visualizations in various formats (like tables, line charts, bar charts, etc.)
## Getting Started

### Prerequisites
- Python 3.x
- PostgreSQL
- dbt
- Airflow
- Using WSL(recommended since dbt and airflow might display some issues on windows terminal. This is not applicable if you're using a windows system) 

### Installation
#### 1. Clone the repository
```bash
git clone https://github.com/AlazarG19/Data-Warehouse-Using-PostgreSQL-DBT-and-Airflow.git
```
#### 2. Setup WSL(recommended for window users)

```bash
wsl -l -v 
``` 
(list all installed distros in ubuntu)
```bash
wsl -d Ubuntu-22.04 
```
(distro i had installed and used for the project)

#### 3. Setup Virtual Environment
```bash
python3 -m venv venv 
```
or
```bash 
py -3.10 -m venv venv
``` 

(for 3.10 version. This is the version i used)

#### 4. Activate Virtual Environment

```bash 
source venv/bin/activate
```

#### 5. Install the required Python packages
```bash
pip install -r requirements.txt
```

#### 6. Set up PostgreSQL
- Install PostgreSQL
- Create a new database
- Create an env file in the dag file and the dbt file and add credentials like 

HOST=your_db_host
DATABASE=your_db
PORT=your_port
USER=your_user
PASSWORD=your_password
FILE_PATH=the file path where you'll be putting the csv data files

when importing the .env files you might encounter some issues so the best approach would be to use 

```bash
set -o allexport; source .env; set +o allexport
```
this is used to load environment variable from the .env to the current shell session

#### 7. Set up dbt
- Install dbt(included in the requirements.txt)
- Move to the dbt path
```bash 
cd dbt
```
```bash 
dbt init my_project
```

if no profiles.yml is found create it in the same folder. the credentials are imported from the env file so be to sure to follow the previous steps

#### 8. Set up Airflow
- Install Airflow(included in the requirement folder)

- Initialize the Airflow database
```bash
airflow db init
```
- Start the Airflow web server
```bash
airflow webserver --port 8080

```
- Start the Airflow scheduler
```bash
airflow scheduler
```

- To Do it all together use 
```bash
airflow standalone
```
This is only recommended for development

- to change the airflow config location (since it will resort to default everytime you close and open it).this will be the directory where your airflow.cfg will exist. You won't be able to locate your dag and the airflow server won't start in some cases.
```bash 
export AIRFLOW_HOME=$(pwd)
```

- Access the Airflow web interface
```
http://localhost:8080
```
- Create a new DAG in Airflow using the `dags/vehicle_trajectory_dag.py` file
- Trigger the DAG to run
- Monitor the DAG run in the Airflow web interface
- Check the transformed data in your PostgreSQL database
- Create visualizations in Redash using the transformed data

## Usage
1. Load data into PostgreSQL
2. Transform data using dbt
3. Create visualizations in PowerBI
4. Schedule the data pipeline in Airflow
5. Monitor the data pipeline in Airflow
6. Analyze the data in PostgreSQL
7. Document the data models in dbt
8. Update the data pipeline as needed
9. Repeat steps 1-8 as needed

# Visualisation


## Visualisations from the powerbi
![Visualisation 1](/visualisation/1.jpg)
![Visualisation 2](/visualisation/2.jpg)
![Visualisation 3](/visualisation/3.jpg)

## Visualisations from the dbt docs 
![Average Distance by Vehicle Type](/dbt/dbt_docs/average_distance_by_vehilce_type.png)
![Average Speed by Vehicle Type](/dbt/dbt_docs/average_speed_by_vehilce_type.png)
![Location Density Analysis ](/dbt/dbt_docs/location_density_analysis.png)
![Location Traffic Analysis ](/dbt/dbt_docs/location_traffic_analysis.png)
![Performance Summary by Type ](/dbt/dbt_docs/performance_summary_by_type.png)
![Route Efficiency](/dbt/dbt_docs/route_efficience.png)
![Total Distance Traveled By Type](/dbt/dbt_docs/total_distance_traveled_by_type.png)
![Vehicle Type DIstribution](/dbt/dbt_docs/vehicle_type_distribution.png)
![Vehicle Type Proportion](/dbt/dbt_docs/vehicle_type_proportion.png)


## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgements
- [PostgreSQL](https://www.postgresql.org/)
- [Airflow](https://airflow.apache.org/)
- [dbt](https://www.getdbt.com/)
- [License](https://opensource.org/licenses/MIT)