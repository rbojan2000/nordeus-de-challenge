<a name="readme-top"></a>
<br />
<div align="center">
  <h3 align="center">Nordeus Data Engineering Challenge Solution</h3>
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
    <li>
      <a href="#solution">Solution</a>
    </li>
    <li>
      <a href="#gettihttps://spark.apache.org/ng-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#local-setup">Local setup</a></li>
        <li><a href="#local-run">Local run</a></li>
        <li><a href="#local-run">CLI API session</a></li>
      </ul>
    </li>
  </ol>
</details>


# About The Project

## Built With

* [![Python][Python]][Python-url]
* [![Spark][Spark]][Spark-url]
* PostgreSQL

<p align="right">(<a href="#readme-top">back to top</a>)</p>

# Solution

# Getting Started

## Prerequisites

Requirements for running applications:

* Poetry
* Python 3.10
* Python virtueal environment (pyenv, virtualenv)


## Local Setup
This section provides detailed instructions on setting up your local development environment. Follow these steps carefully to contribute to the project effectively and explore its features effortlessly.

1. Clone the repo
   ```sh
    $ git clone https://github.com/rbojan2000/nordeus-de-challenge
   ```
2. Create virtuale environment in project. E.g.:
   ```sh
    $ cd project_folder
    $ virtualenv venv
   ```
3. Activate virtual environment
    ```sh
    $ source venv/bin/activate
    ```
4. Install all python libraries
   ```sh
    $ poetry install
   ```

## Local run
In order to run applications locally, add to `~/.bash_profile` or `~/.zshrc` environment variable `export ENV_FOR_DYNACONF=dev`. Position yourself in the application directory and run the following command:
```sh
$ python -m application_name.module.entrypoint
```
Entry point is usually `main.py` script.
> **_NOTE:_** The settings.toml file contains configuration parameters of applications. Each application has its own pyproject file with dependencies where pyproject.toml in the root of the project contains Python tools.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- MARKDOWN LINKS & IMAGES -->
[Python]: https://img.shields.io/badge/Python-white?style=for-the-badge&logo=python&logoColor=3776AB
[Python-url]: https://www.python.org/
[Spark]: https://spark.apache.org/images/spark-logo-rev.svg
[Spark-url]: https://spark.apache.org/
[PostgreSQL]: https://www.postgresql.org/media/img/about/press/elephant.png
[PostgreSQL-url]: https://www.postgresql.org/
