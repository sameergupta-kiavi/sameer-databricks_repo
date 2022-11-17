import json
import requests
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator


class KVDatabricksHook(BaseHook):
    class BearerAuth(requests.auth.AuthBase):
        def __init__(self, token):
            self.token = token

        def __call__(self, r):
            r.headers["authorization"] = "Bearer " + self.token
            return r

    def __init__(self, databricks_connection_name, context={}, *args, **kwargs):

        self.databricks_connection_name = databricks_connection_name
        self.__databricks_host = BaseHook.get_connection(
            self.databricks_connection_name
        ).host
        self.__extra_encryption = json.loads(
            BaseHook.get_connection(self.databricks_connection_name).get_extra()
        )

    def __get_uri(self):
        """
        To get the uri
        """
        return BaseHook.get_connection(self.databricks_connection_name).get_uri()

    def __get_host(self):
        """
        To get the databricks host name
        """
        return self.__databricks_host

    def __get_token(self):
        """
        To get databricks access token
        """
        return self.__extra_encryption["token"]

    def __get_jobs_endpoint(self):
        """
        To get Databricks Jobs API Endpoint
        """
        return self.__extra_encryption["jobs_endpoint"]

    def get_job_id(self, job_name):
        """
        To get the job id of respective job name
        """
        job_id = None
        response = requests.get(
            self.__get_host() + self.__get_jobs_endpoint(),
            auth=self.BearerAuth(self.__get_token()),
        ).json()
        for i in response["jobs"]:
            if i["settings"]["name"] == job_name:
                job_id = i["job_id"]
        if job_id is not None:
            return job_id
        else:
            raise Exception(job_name + "does not exists")

    def get_run_operator(self, *args, **kwargs):
        """return the databricks run now operator"""
        job_id = self.get_job_id(kwargs["job_name"])
        return DatabricksRunNowOperator(
            task_id=kwargs["task_id"],
            dag=kwargs["dag"],
            databricks_conn_id=self.databricks_connection_name,
            job_id=job_id,
            notebook_params=kwargs["notebook_params"],
        )
