from .base import BaseDataHandler
from google.cloud import bigquery
import googleapiclient.discovery
import google.oauth2.credentials
from typing import List, Dict, Any, Optional
import pandas as pd


class GCPDataHandler(BaseDataHandler):
    """
    Handler for Google Cloud Platform (GCP) data.

    Usage:
    # Initialize handler
    handler = GCPDataHandler(cache)

    # List projects (after OAuth authentication)
    projects = handler.get_projects(credentials=oauth_credentials)
    # Returns: [{'id': 'project-id', 'name': 'My Project', ...}]

    # List datasets in a project
    datasets = handler.get_datasets(
        credentials=oauth_credentials,
        project_id='my-project-id'
    )
    # Returns: [{'id': 'dataset_id', 'name': 'My Dataset', ...}]

    # List tables in a dataset
    tables = handler.get_tables(
        credentials=oauth_credentials,
        project_id='my-project-id',
        dataset_id='my_dataset'
    )
    # Returns: [{'id': 'table_id', 'name': 'My Table', ...}]

    # Read table data
    df = handler.read_table(
        credentials=oauth_credentials,
        project_id='my-project-id',
        dataset_id='my_dataset',
        table_id='my_table',
        location='US',  # Optional: BigQuery dataset location
        limit=1000,     # Optional: limit number of rows
        custom_query='SELECT * FROM `project.dataset.table` WHERE date > "2024-01-01"'  # Optional
    )
    # Returns: pandas DataFrame

    # Get table schema
    schema = handler.get_table_schema(
        credentials=oauth_credentials,
        project_id='my-project-id',
        dataset_id='my_dataset',
        table_id='my_table',
        location='US'  # Optional: BigQuery dataset location
    )
    # Returns: [{'name': 'column_name', 'type': 'STRING', 'mode': 'NULLABLE', 'description': '...'}]
    """
    
    def __init__(self, cache):
        super().__init__(cache)
    
    @property
    def cache_timeout(self) -> int:
        return 300 #5 minutes
    
    @property
    def service_name(self) -> str:
        return 'GCP'
    
    @property
    def crm_version(self) -> str:
        """
        Version of the Cloud Resource Manager API.
        """
        return 'v1'
    
    @property
    def bq_version(self) -> str:
        """
        Version of the BigQuery API.
        """
        return 'v2'
    
    def _dict_to_credentials(self, credentials: dict) -> google.oauth2.credentials.Credentials:
        """
        Convert a dictionary to a Credentials object
        """
        return google.oauth2.credentials.Credentials(**credentials)
    
    def _credentials_to_dict(self, credentials: google.oauth2.credentials.Credentials) -> dict:
        """
        Convert a Credentials object to a dictionary for caching.
        """
        return {
            'token': credentials.token,
            'refresh_token': credentials.refresh_token,
            'token_uri': credentials.token_uri,
            'client_id': credentials.client_id,
            'client_secret': credentials.client_secret,
            'scopes': credentials.scopes
        }
        
    def get_projects(self, credentials) -> List[Dict[str, Any]]:
        """
        Get list of GCP projects accessible to the user.
        
        Args:
            credentials: OAuth2 credentials object
            
        Returns:
            List of project dictionaries
        """
        @self.cache.memoize(timeout=self.cache_timeout)
        def _get_projects(credentials) -> List[Dict[str, Any]]:
            service = googleapiclient.discovery.build('cloudresourcemanager', self.crm_version, credentials=credentials)
            request = service.projects().list()
            response = request.execute()
            projects = response.get('projects', [])
            return projects
        if isinstance(credentials, dict):
            credentials = self._dict_to_credentials(credentials)
        assert isinstance(credentials, google.oauth2.credentials.Credentials)
        return _get_projects(credentials)
    
    def get_datasets(self, credentials, project_id: str) -> List[Dict[str, Any]]:
        """
        Get list of datasets in a GCP project.

        Args:
            credentials: OAuth2 credentials object
            project_id: ID of the GCP project
            
        Returns:
            List of dataset dictionaries
        """
        @self.cache.memoize(timeout=self.cache_timeout)
        def _get_datasets(credentials, project_id: str) -> List[Dict[str, Any]]:
            service = googleapiclient.discovery.build('bigquery', self.bq_version, credentials=credentials)
            request = service.datasets().list(projectId=project_id)
            response = request.execute()
            return response.get('datasets', [])
        if isinstance(credentials, dict):
            credentials = self._dict_to_credentials(credentials)
        assert isinstance(credentials, google.oauth2.credentials.Credentials)
        return _get_datasets(credentials, project_id)
    
    def get_tables(self, credentials, project_id: str, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Get list of tables in a BigQuery dataset.
        
        Args:
            credentials: OAuth2 credentials object
            project_id: ID of the GCP project
            dataset_id: ID of the BigQuery dataset
            
        Returns:
            List of table dictionaries
        """
        @self.cache.memoize(timeout=self.cache_timeout)
        def _get_tables(credentials, project_id: str, dataset_id: str) -> List[Dict[str, Any]]:
            service = googleapiclient.discovery.build('bigquery', self.bq_version, credentials=credentials)
            request = service.tables().list(projectId=project_id, datasetId=dataset_id)
            response = request.execute()
            return response.get('tables', [])
        if isinstance(credentials, dict):
            credentials = self._dict_to_credentials(credentials)
        assert isinstance(credentials, google.oauth2.credentials.Credentials)
        return _get_tables(credentials, project_id, dataset_id)
    
    def read_table(self, 
                   credentials, 
                   project_id: str, 
                   dataset_id: str, 
                   table_id: str,
                   location: str,
                   limit: Optional[int] = None,
                   custom_query: Optional[str] = None) -> pd.DataFrame:
        """
        Read BigQuery table into a pandas DataFrame.

        Args:
            credentials: OAuth2 credentials object
            project_id: ID of the GCP project
            dataset_id: ID of the BigQuery dataset
            table_id: ID of the BigQuery table
            location: Location of the BigQuery dataset
            limit: Maximum number of rows to return (optional)
            custom_query: Custom SQL query to execute (optional)
            
        Returns:
            pandas DataFrame containing the table data
        """
        @self.cache.memoize(timeout=self.cache_timeout)
        def _read_table(credentials, project_id: str, dataset_id: str, table_id: str, location: str, limit: Optional[int] = None, custom_query: Optional[str] = None) -> pd.DataFrame:
                        
            client = bigquery.Client(credentials=credentials, project=project_id, location=location)
            
            try:
                table_ref = f'{project_id}.{dataset_id}.{table_id}'
                if custom_query:
                    query = custom_query
                else:
                    query = f'SELECT * FROM `{table_ref}`'
                    if limit:
                        query += f' LIMIT {limit}'
                query_job = client.query(query)
                result = query_job.result()
                return pd.DataFrame(result.to_dataframe())
            except Exception as e:
                raise Exception(f'Error reading table {table_ref}: {str(e)}')
        if isinstance(credentials, dict):
            credentials = self._dict_to_credentials(credentials)
        assert isinstance(credentials, google.oauth2.credentials.Credentials)
        return _read_table(credentials, project_id, dataset_id, table_id, location, limit, custom_query)
    
    def get_table_schema(self, credentials, project_id: str, dataset_id: str, table_id: str, location: str) -> List[Dict[str, Any]]:
        """
        Get the schema of a BigQuery table.

        Args:
            credentials: OAuth2 credentials object
            project_id: ID of the GCP project
            dataset_id: ID of the BigQuery dataset
            table_id: ID of the BigQuery table
            location: Location of the BigQuery dataset
            
        Returns:
            List of field dictionaries containing field information
        """
        @self.cache.memoize(timeout=self.cache_timeout)
        def _get_table_schema(credentials, project_id: str, dataset_id: str, table_id: str, location: str) -> List[Dict[str, Any]]:
            client = bigquery.Client(credentials=credentials, project=project_id, location=location)
             
            try:
                table_ref = f'{project_id}.{dataset_id}.{table_id}'
                table = client.get_table(table_ref)
                return [
                    {
                        'name': field.name,
                        'type': field.field_type,
                        'mode': field.mode,
                        'description': field.description
                    }
                    for field in table.schema
                ]
            except Exception as e:
                raise Exception(f'Error getting table schema for {table_ref}: {str(e)}')
        if isinstance(credentials, dict):
            credentials = self._dict_to_credentials(credentials)
        assert isinstance(credentials, google.oauth2.credentials.Credentials)
        return _get_table_schema(credentials, project_id, dataset_id, table_id, location)
    
    def clear_cache(self):
        """
        Clear the cache
        """
        self.cache.clear()

