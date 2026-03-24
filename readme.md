## ETL-App Airflow

Ce projet implémente un pipeline ETL pour récupérer les données météo et qualité de l’air, les transformer et les stocker dans une base PostgreSQL.

## Prérequis technique
Docker & Docker Compose
Python 3.8+

## Prérequis data
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_HOST=
API_OPEN=<ta_api_open>
API_AIR=<ta_api_air>

## Lancement
    "docker-compose up --build"