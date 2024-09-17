from airflow.plugins_manager import AirflowPlugin
from operators__c.async_python_operator__c import AsyncPythonOperator__c
from blueprints import external_log

class AsyncPythonOperator__c(AirflowPlugin):
    name = 'AsyncPythonOperator__c'
    operators = [AsyncPythonOperator__c]

# Define the plugin
class ExternalLog(AirflowPlugin):
    name = "external_log"
    flask_blueprints = [external_log]
