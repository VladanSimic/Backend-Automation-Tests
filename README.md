# Snowflake Long Running Job Monitor

This Python script is designed to monitor long-running jobs in Snowflake and send an email alert if certain jobs are taking significantly longer than average. It's intended to be used in an AWS Lambda environment or executed as a standalone script.

## Features

- Connects to Snowflake using credentials provided via environment variables or a `.properties` file.
- Executes two SQL queries:
  - One for historical average durations of finished jobs.
  - One for currently running jobs.
- Compares current job durations with historical averages.
- Sends an email notification if a job is running 25% longer than the average duration.

## Prerequisites

- Python 3.x
- Required libraries:
  - `snowflake-connector-python`
  - `prettytable`
  - `smtplib` (standard library)
  - `email` (standard library)
- Snowflake account and job log table structure as used in the SQL queries.
- `.properties` file (e.g., `files/JA.properties`) for Snowflake credentials.

## Environment Variables (if used)

| Variable Name       | Description                    |
|---------------------|--------------------------------|
| `SF_USER_NAME`      | Snowflake username             |
| `SF_PASSWORD`       | Snowflake password             |
| `SF_ACCOUNT`        | Snowflake account identifier   |
| `SF_DATABASE_NAME`  | Snowflake database name        |
| `SF_SCHEMA`         | Schema where job log is stored |
| `SF_WAREHOUSE`      | Warehouse to use               |
| `SENDER_EMAIL`      | Sender email address           |
| `RECIPIENT_EMAIL`   | Recipient email address        |
| `SMTP_SERVER`       | SMTP server for sending emails |
| `SMTP_PORT`         | SMTP port                      |
| `SMTP_USER`         | (optional) SMTP login username |
| `SMTP_PASSWORD`     | (optional) SMTP login password |

## Usage

1. **Configure `.properties` file:**

