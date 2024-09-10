from airflow.plugins_manager import AirflowPlugin
from operators__c.async_python_operator__c import AsyncPythonOperator__c
from blueprints import get_message

class AsyncPythonOperator__c(AirflowPlugin):
    name = 'AsyncPythonOperator__c'
    operators = [AsyncPythonOperator__c]

# Define the plugin
class GetMessage(AirflowPlugin):
    name = "get_message"
    flask_blueprints = [get_message]
