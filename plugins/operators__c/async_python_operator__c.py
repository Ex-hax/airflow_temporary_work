import asyncio
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from inspect import iscoroutinefunction

class AsyncPythonOperator__c(BaseOperator):

    @apply_defaults
    def __init__(self, python_callable, op_args=None, op_kwargs=None, *args, **kwargs):
        super(AsyncPythonOperator__c, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

        if not callable(self.python_callable):
            raise TypeError('python_callable` param must be callable')

    async def async_execute(self):
        self.log.info('Starting async operation')
        return await self.python_callable(*self.op_args, **self.op_kwargs)

    def execute(self, context):
        if not iscoroutinefunction(self.python_callable):
            raise TypeError('`python_callable` must be an async function')
        
        return asyncio.run(self.async_execute())
