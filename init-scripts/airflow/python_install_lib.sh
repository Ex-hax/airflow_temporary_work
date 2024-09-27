#!/bin/bash
pip install flask[async] aiohttp
pip install apache-airflow-providers-microsoft-mssql[common.sql]
pip install esdk-obs-python --trusted-host pypi.org
