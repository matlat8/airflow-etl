# Airflow DAGs & Configuration

## Setting up a new development environment
*Taken directly from [Airflow Docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)*

**Initialize the database**
On all operating systems, you need to run database migrations and create the first user account. To do this, run.
``` bash
docker compose up airflow-init
```

The account created has the login `airflow` and the password `airflow`.

**Running Airflow**
```bash
docker compose up -d
```

**Cleaning up the environment**
- Run docker compose down --volumes --remove-orphans

