<a name="readme-top"></a>
<br />
<div align="center">
    <h1 align="center">Nordeus Data Engineering Challenge Solution</h1>
</div>

<!-- TABLE OF CONTENTS -->
<details>
    <summary>Table of Contents</summary>
    <ol>
        <li>
            <a href="#about-the-project">About The Project</a>
                <ul>
                    <li><a href="#built-with">Built With</a></li>
                </ul>
        </li>
        <li> <a href="#solution">Solution</a></li>
            <ul>
                <li><a href="#architecture">Architecture</a></li>
                <li><a href="#etl">ETL</a></li>
                    <ul>
                        <li><a href="#transformation">Transformation</a></li>
                        <li><a href="#load">Load</a></li>
                            <ul>
                                <li><a href = "#stats-calculations">Stats Calculations</a></li>
                            </ul>
                    </ul>
                <li> <a href="#nordeus-cli">Nordeus CLI</a></li>
            </ul>
        <li><a href="#getting-started">Getting Started</a></li>
            <ul>
                <li><a href="start-metrics-api-session-with-nordeus-cli">Metrics API Session with nordeus-cli</a></li>
                <ul>
                    <li><a href="#prerequisites">Prerequisites</a></li>
                    <li><a href="#setup-instructions">Setup Instructions</a></li>
                </ul>
            <ul>
    </ol>
</details>


# About The Project
This project is a solution to the <a href="https://nordeus.com/nordeus-challenge/data-engineering/" target="_blank" rel="noopener noreferrer">Data Engineering Challenge - From Matches to Metrics</a> by <a href="https://nordeus.com/" target="_blank" rel="noopener noreferrer">Nordeus</a>. The goal is to address <i>data cleaning tasks</i>, compute <i>game-level</i> and <i>user-level</i> statistics for the Top Eleven season (October 7 - November 3), and develop an API to fetch the calculated metrics.

For more details about the challenge and raw datasets, refer to the <a href="https://github.com/rbojan2000/nordeus-de-challenge/blob/main/docs/JobFair%202024-%20Data%20Engineering%20Challenge.pdf" target="_blank" rel="noopener noreferrer">JobFair 2024 - Data Engineering Challenge.pdf</a> file in the documentation.
## Built With

* [![Python][Python]][Python-url]
* [![Spark][Spark]][Spark-url]
* <a href="https://www.postgresql.org/">PostgreSQL</a>

<p align="right">(<a href="#readme-top">back to top</a>)</p>

# Solution
The solution consists of two modules:
1. <a href="#etl"> ETL </a>: Handles data cleaning, statistical calculations, and loading the processed data into a database for querying metrics and
2. <a href="#nordeus-cli">Nordeus CLI</a>: Serves as a client for retrieving <i>game-level</i> and <i>user-level</i> metrics from the database.

## Architecture
Overall Architecture is shown in <i> Diagram-1 </i> below:
[![](/docs/nordeus-de-c-s-architecture.png)](/docs/nordeus-de-c-s-architecture.png)
<p align="center"> <i>Diagram-1</i></p>

## ETL

### Transformation
The Transformation app reads raw event data from the <i><a href ="https://github.com/rbojan2000/nordeus-de-challenge/tree/main/data/raw">data/raw</a></i> directory and performs the following data processing:
 - Removes duplicate events,
 - Discards invalid events (event_id, event_type, or event_timestamp are null),
 - Renames columns for consistency and
 - Validates columns to ensure data integrity.

The Transformation app writes the curated datasets in <a href="https://parquet.apache.org/">Parquet</a> format to the <i><a href ="https://github.com/rbojan2000/nordeus-de-challenge/tree/main/data/curated">data/curated</a></i> directory.

Curated Datasets:
* `match`

| Column                | Description                                                                                           |
|-----------------------|-------------------------------------------------------------------------------------------------------|
| `event_id`            | Unique identifier representing a match event                                                          |
| `match_id`            | Identifier representing one match (has the same value for both match start and match end)             |
| `home_user_id`        | Unique identifier representing a user                                                                 |
| `away_user_id`        | Unique identifier representing a user                                                                 |
| `home_goals_scored`   | Amount of goals scored in a match                                                                     |
| `away_goals_scored`   | Amount of goals scored in a match                                                                     |
| `match_timestamp`     | Time of match represented as Unix time                                                                |
| `match_status`        | Indicates the status of the match, with possible values being match_start or match_end                |


* `registration`

| Column                    | Description                                                                                           |
|---------------------------|-------------------------------------------------------------------------------------------------------|
| `event_id`                | Unique identifier representing a registration event                                                   |
| `user_id`                 | Unique identifier representing a user                                                                 |
| `country`                 | Country that the user comes from                                                                      |
| `device_os`               | Unique identifier representing a user                                                                 |
| `registration_timestamp`  | Time of registration represented as Unix time                                                         |


* `session_ping`

| Column                    | Description                                                                                           |
|---------------------------|-------------------------------------------------------------------------------------------------------|
| `event_id`                | Unique identifier representing a registration event                                                   |
| `user_id`                 | Unique identifier representing a user                                                                 |
| `type`                    | Possible values are: session_start, session_end, and “”                                               |
| `session_timestamp`       | Time of session ping represented as Unix time                                                         |

### Load
The Load app reads curated event datasets from the <i><a href ="https://github.com/rbojan2000/nordeus-de-challenge/tree/main/data/curated">data/curated</a></i> directory, calculates match statistics and user session statistics, and writes the resulting `match-stats` and `user-session-stats` datasets into the database.


#### Stats Calculations
* `user-session-stats`

| Column                        | Description                                                                                                                 |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `user_id`                     | Unique identifier representing a user                                                                                       |
| `event_id`                    | Unique identifier representing a registration event                                                                         |
| `type`                        | Possible values are: session_start, session_end, and “”                                                                     |
| `session_timestamp`           | Time of session ping represented as Unix time                                                                               |
| `country`                     | Country that the user comes from                                                                                            |
| `registration_timestamp`      | Time of registration represented as Unix time                                                                               |
| `timezone`                    | Timezone in the country                                                                                                     |
| `registration_local_datetime` | Registration datetime in local timezone                                                                                     |
| `previous_session_timestamp`  | Time of previous user session ping                                                                                          |
| `time_diff`                   | Time difference between two session pings in seconds                                                                        |
| `is_new_session`              | Indicates if the session is new. A new session is assumed if the time between two session pings is greater than 60 seconds  |
| `session_duration`            | Duration of the user session in seconds for the session ping                                                                |

> Note: Stats are calculated over a SQL window partitioned by `user_id`.

<br>

* `match-stats`

| Column                | Description                                                                                           |
|-----------------------|-------------------------------------------------------------------------------------------------------|
| `match_id`            | Identifier representing one match (has the same value for both match start and match end)             |
| `home_user_id`        | Unique identifier representing a user                                                                 |
| `away_user_id`        | Unique identifier representing a user                                                                 |
| `start_time`          | Match start time, represented as Unix time                                                            |
| `end_time`            | Match end time, represented as Unix time                                                              |
| `match_duration`      | Total duration of the match in seconds                                                                |
| `home_goals_scored`   | Number of goals scored by the home player during the match                                            |
| `away_goals_scored`   | Number of goals scored by the away player during the match                                            |
| `home_user_points`    | Home player match points                                                                              |
| `away_user_points`    | Away player match points                                                                              |

<br>

> [!NOTE]
> The database is automatically populated with `match-stats` and `user-session-stats` data when starting PostgreSQL as a Docker container. For the initial `match-stats` and `user-session-stats` data, see the <a href ="https://github.com/rbojan2000/nordeus-de-challenge/blob/main/solution/infrastructure/init.sql">init.sql</a> file.

## Nordeus CLI
Command-line interface (CLI) application that provides an API to retrieve `user-level` and `game-level` stats, along with data visualization for the fetched stats.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

# Getting Started

## Metrics API Session with `nordeus-cli`

### Prerequisites
Ensure the following tools are installed on your system:
* Poetry
* Python 3.12
* Python virtueal environment (pyenv, virtualenv)
* Docker
* Docker Compose

### Setup Instructions

1. Clone the repo
   ```sh
    $ git clone https://github.com/rbojan2000/nordeus-de-challenge
   ```
2. Start PostgreSQL database as a Dcoker container and load the initial `match-stats` and `user-session-stats` data using the <a href = "https://github.com/rbojan2000/nordeus-de-challenge/blob/main/solution/infrastructure/init.sql">init.sql</a>  script
   ```sh
    $ docker compose -f solution/infrastructure/docker-compose.yml up -d
   ```
3. Change directory to `nordeus-cli`
   ```sh
    $ cd solution/nordeus_clis
   ```
4. Create virtual environment in project. E.g.:
   ```sh
    $ virtualenv venv
   ```
5. Activate virtual environment
    ```sh
    $ source venv/bin/activate
    ```
6. Install all python libraries
   ```sh
    $ poetry install
   ```
7. Query the data from database using `nordeus-cli`. For detailed usage instructions, see the nordeus-cli <a href = "https://github.com/rbojan2000/nordeus-de-challenge/blob/main/solution/nordeus_cli/README.md#usage">usage</a>
   ```sh
    $ poetry run nordeus-cli
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

> [!NOTE]
> Each App(<a href = "https://github.com/rbojan2000/nordeus-de-challenge/blob/main/solution/etl/transformation/README.md">transformation</a>, <a href="https://github.com/rbojan2000/nordeus-de-challenge/blob/main/solution/etl/load/README.md">load</a>, <a href="https://github.com/rbojan2000/nordeus-de-challenge/blob/main/solution/nordeus_cli/README.md">nordeus-cli</a>) of the project has its own `README.md` file providing detailed information and usage instructions.

<!-- MARKDOWN LINKS & IMAGES -->
[Python]: https://img.shields.io/badge/Python-white?style=for-the-badge&logo=python&logoColor=3776AB
[Python-url]: https://www.python.org/
[Spark]: https://spark.apache.org/images/spark-logo-rev.svg
[Spark-url]: https://spark.apache.org/
