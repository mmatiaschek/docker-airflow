# Airflow proof of concept

This repository contains the necessary files to showcase the proof of concept for using airflow.

It is forked from [puckel/docker-airflow](https://github.com/puckel/docker-airflow) with minimal additions.
All the heavy lifting is done by the contributions from the upstream repository.

## Usage

For now, we only support **LocalExecutor** :

    docker-compose -f docker-compose-LocalExecutor.yml up -d

