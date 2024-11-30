# nordeus-cli
Command-line interface (CLI) application that provides an API to retrieve `user-level` and `game-level` stats, along with data visualization for the fetched stats.

## Getting Started

### Prerequisites
Ensure the following tools are installed on your system:
* Poetry
* Python 3.12
* Python virtueal environment (pyenv, virtualenv)

### Setup Instructions
1. Create virtual environment in project. E.g.:
   ```sh
    $ virtualenv venv
   ```
2. Activate virtual environment
   ```sh
    $ source venv/bin/activate
   ```
3. Install all python libraries
   ```sh
    $ poetry install
   ```
4. Query the data from database using `nordeus-cli`
   ```sh
    $ poetry run nordeus-cli
   ```

## Usage
   ```sh
    $ poetry run nordeus-cli query [OPTIONS]
   ```
[OPTIONS]:
   - api: Specify the API type (user-level or game-level).
   - date: Provide the date for the query.
   - user-id: Specify the user ID for the query.

### Examples
   ```sh
    $ poetry run nordeus-cli query --api user-level --user-id d7f20e07-ed42-02ed-c4bb-895c608099f6 --date 2024-10-19
    $ poetry run nordeus-cli query --api user-level --user-id d7f20e07-ed42-02ed-c4bb-895c608099f6
    $ poetry run nordeus-cli query --api game-level --date 2024-10-13
    $ poetry run nordeus-cli query --api game-level
   ```
