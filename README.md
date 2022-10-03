# Snowpark CI/CD Template

This repo is a sample project for a stored procedure (that created / depends on a user defined function) which is configured for CI/CD via pytest and GitHub Actions.

## Prerequisites

* Snowflake account
* Python 3.8
* Git

## Setup

After creating a copy of the template in your own repo, clone to your local machine and run the following commands:

```bash
git clone <github-url>
```

Then you need to rename the `config.py.sample` file to `config.py` and update the values to match your environment.

You will also want to update the settings for your repo and add the following GitHub Actions Secrets:

| Name | Description |
| --- | --- |
| SNOWFLAKE_ACCOUNT | Snowflake account name |
| SNOWFLAKE_USERNAME | Snowflake user name |
| SNOWFLAKE_PASSWORD | Snowflake password |
| SNOWFLAKE_ROLE | Snowflake role to execute the GitHub actions |
| SNOWFLAKE_WAREHOUSE | Snowflake warehouse to execute the GitHub actions |
| SNOWFLAKE_DATABASE | Snowflake database to execute the GitHub actions |