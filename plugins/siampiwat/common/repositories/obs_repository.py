from typing import Dict, List, Union, Optional
import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import uuid

from siampiwat.common.repositories.base_repository import BaseRepository
from obs import ObsClient


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileHandler:


    @staticmethod
    def read_file(client: ObsClient, bucket_name: str, file_path: str, file_type: str) -> pd.DataFrame:
        """
        Reads a file from the specified bucket and converts it to a DataFrame.

        Args:
            client (ObsClient): The OBS client instance.
            bucket_name (str): The name of the bucket.
            file_path (str): The path to the file in the bucket.
            file_type (str): The type of the file ('parquet' or 'json').

        Returns:
            pd.DataFrame: The loaded data as a DataFrame, or an empty DataFrame on error.
        """
        temp_dir = f"./data/{uuid.uuid4()}/"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = os.path.join(temp_dir, f"temp_{file_type}_file")

        try:
            resp = client.getObject(bucket_name, file_path)
            with open(temp_file_path, "wb") as f:
                f.write(resp["body"].response.read())
                logger.info("File %s saved to %s", file_path, temp_file_path)
            
            if file_type == "parquet":
                df = pd.read_parquet(temp_file_path)
            elif file_type == "json":
                df = pd.read_json(temp_file_path, lines=True)
            else:
                logger.error("Unsupported file type: %s", file_type)
                return pd.DataFrame()
            
            return df
        except Exception as e:
            logger.error("Error reading %s file %s: %s", file_type, file_path, e)
            return pd.DataFrame()    
        finally:
            FileHandler.cleanup_temp_dir(temp_dir)
    
    @staticmethod
    def cleanup_temp_dir(temp_dir: str):
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                logger.info("Cleaned up temporary directory %s", temp_dir)
            else:
                logger.warning("Temporary directory %s does not exist", temp_dir)
        except Exception as e:
            logger.error("Error cleaning up temporary directory %s: %s", temp_dir, e)


class SchemaHandler:


    @staticmethod
    def get_pyarrow_schema(dataframe: pd.DataFrame, schema_mapping: Dict[str, str]) -> pa.Schema:
        """
        Generates a PyArrow schema from the provided DataFrame and schema mapping.

        Args:
            dataframe (pd.DataFrame): The DataFrame for which to generate the schema.
            schema_mapping (Dict[str, str]): A mapping of DataFrame column names to PyArrow data types.

        Returns:
            pa.Schema: The PyArrow schema for the DataFrame.
        """
        fields = [
            (column, SchemaHandler.map_dtype_to_pyarrow(dtype))
            for column, dtype in schema_mapping.items()
            if column in dataframe.columns
        ]
        return pa.schema(fields)


    @staticmethod
    def map_dtype_to_pyarrow(dtype: str) -> pa.DataType:
        """
        Maps a string representation of a type to a PyArrow DataType.

        Args:
            dtype (str): The string representation of the data type.

        Returns:
            pa.DataType: The corresponding PyArrow data type.
        """
        dtype_mappings = {
            "timestamp_ns_utc": pa.timestamp("ns", tz="UTC"),
            "int64": pa.int64(),
            "float64": pa.float64(),
            "bool": pa.bool_(),
            "string": pa.string()
        }
        return dtype_mappings.get(dtype, pa.string())


class OBSRepository(BaseRepository):
    def __init__(self, access_key_id: str, secret_access_key: str, endpoint: str) -> None:
        """
        Initializes the ObsRepository with credentials and endpoint.

        Args:
            access_key_id (str): The access key ID for the OBS client.
            secret_access_key (str): The secret access key for the OBS client.
            endpoint (str): The endpoint URL for the OBS service.
        """
        self.client = ObsClient(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            server=endpoint
        )


    def list_files(self, bucket_name: str, prefix: str, date_str: str, file_extension: str) -> List[str]:
        """
        Lists files in a specified bucket that match a given prefix and file extension.

        Args:
            bucket_name (str): The name of the bucket.
            prefix (str): The prefix to filter files.
            date_str (str): The date string to include in the file search.
            file_extension (str): The file extension to match.

        Returns:
            List[str]: A list of file paths that match the criteria.
        """
        try:
            objects = self.client.listObjects(bucket_name, prefix=prefix)
            return [
                obj["key"] for obj in objects["body"]["contents"]
                if (not date_str or f"loaddt={date_str}" in obj["key"]) and
                   (not file_extension or obj["key"].endswith(file_extension))
            ]
        except Exception as e:
            logger.error("Error listing files in %s/%s: %s", bucket_name, prefix, e)
            return []


    def read_files(self, bucket_name: str, files: List[str], file_type: str) -> pd.DataFrame:
        """
        Reads multiple files from the specified bucket and merges them into a single DataFrame.

        Args:
            bucket_name (str): The name of the bucket.
            files (List[str]): A list of file paths to read.
            file_type (str): The type of the files to read ('parquet' or 'json').

        Returns:
            pd.DataFrame: The concatenated DataFrame from all read files.
        """
        dataframes = [
            FileHandler.read_file(self.client, bucket_name, file, file_type)
            for file in files
        ]
        if not any(not df.empty for df in dataframes):
            logger.error("No files found for the specified date or all were empty.")
            return pd.DataFrame()
        return pd.concat(dataframes, ignore_index=True)


    def write_parquet_files(self, dataframe: pd.DataFrame, output_dir: str, pyarrow_schema: pa.Schema, partition_cols: List[str]):
        """
        Writes the DataFrame to parquet files in the specified directory, partitioned by the given columns.

        Args:
            dataframe (pd.DataFrame): The DataFrame to write.
            output_dir (str): The directory where to write the parquet files.
            pyarrow_schema (pa.Schema): The PyArrow schema to use for writing.
            partition_cols (List[str]): Columns to partition by.
        """
        os.makedirs(output_dir, exist_ok=True)
        table = pa.Table.from_pandas(dataframe, schema=pyarrow_schema)
        pq.write_to_dataset(table, root_path=output_dir, partition_cols=partition_cols)


    def upload_files(self, bucket_name: str, output_dir: str, prefix: str):
        """
        Uploads files from a specified directory to the specified bucket under a given prefix.

        Args:
            bucket_name (str): The name of the bucket.
            output_dir (str): The directory from which to upload files.
            prefix (str): The prefix under which to store the files in the bucket.
        """
        try:
            for root, _, files in os.walk(output_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, output_dir)
                    object_key = f"{prefix}/{relative_path}"
                    with open(local_path, "rb") as f:
                        self.client.putContent(bucket_name, object_key, f.read())
        except Exception as e:
            logger.error("Error uploading files to OBS bucket: %s", e)


    def get_pyarrow_schema(self, dataframe: pd.DataFrame, schema_mapping: Dict[str, str]) -> pa.Schema:
        """
        Retrieves a PyArrow schema based on the provided DataFrame and schema mapping.

        Args:
            dataframe (pd.DataFrame): The DataFrame for which to create the schema.
            schema_mapping (Dict[str, str]): A mapping of DataFrame column names to PyArrow data types.

        Returns:
            pa.Schema: The PyArrow schema for the DataFrame.
        """
        return SchemaHandler.get_pyarrow_schema(dataframe, schema_mapping)


    def read(self, bucket_name: str, source_prefix: str, date_str: str, file_format: str, compression: Union[str, None] = None) -> pd.DataFrame:
        """
        Reads data from OBS based on specified parameters, returning the data as a DataFrame.

        Args:
            bucket_name (str): The name of the OBS bucket.
            source_prefix (str): The prefix used to locate the files.
            date_str (str): The date string used to filter the files.
            file_format (str): The format of the files to be read.
            compression (Union[str, None]): The compression type of the files, if applicable.

        Returns:
            pd.DataFrame: The DataFrame containing the data read from the files.
        """
        file_extension = f".{file_format}" +(f".{compression}" if compression else "")
        files = self.list_files(bucket_name, source_prefix, date_str, file_extension)
        return self.read_files(bucket_name, files, file_format)


    def write(self, bucket_name: str, destiantion_prefix: str, dataframe: pd.DataFrame, schema_mapping: Dict[str, str], partition_cols: List[str]):
        """
        Writes a DataFrame to OBS, using a PyArrow schema for file format, and uploads the files.

        Args:
            bucket_name (str): The name of the OBS bucket.
            destiantion_prefix (str): The prefix under which to store the files.
            dataframe (pd.DataFrame): The DataFrame to write.
            schema_mapping (Dict[str, str]): The schema mapping for the DataFrame columns.
            partition_cols (List[str]): The list of columns to partition the data by.

        """
        temp_dir = f"./data/{uuid.uuid4()}/"
        os.makedirs(temp_dir, exist_ok=True)
        pyarrow_schema = self.get_pyarrow_schema(dataframe, schema_mapping)
        try:
            logger.info("Writing and uploading files to OBS bucket.")
            self.write_parquet_files(dataframe, temp_dir, pyarrow_schema, partition_cols)
            self.upload_files(bucket_name, temp_dir, destiantion_prefix)
        except Exception as e:
            logger.error("Error writing and uploading files to OBS bucket: %s", e)
        finally:
            FileHandler.cleanup_temp_dir(temp_dir)


    def get_latest_loaddt_per_table(self, bucket_name: str, tables: List[str], domain: str, source: str) -> Dict[str, Optional[str]]:
        """
        Retrieves the latest load date for each table from OBS.

        Args:
        -----
        :param bucket_name: The name of the OBS bucket.
        :param tables: A list of table names to filter by.
        :param domain: The domain of the tables.
        :param source: The source of the tables.

        Returns:
        --------
        A dictionary with table names as keys and the latest load date as values.
        """
        latest_loaddts = {}
        for table in tables:
            prefix = "Landing_zone/{}_{}/{}/".format(domain, source, table)
            logger.info("Checking for files in bucket %s with prefix %s", bucket_name, prefix)
            loaddt_values = self._extract_loaddt_values(bucket_name, prefix)

            if loaddt_values:
                latest_loaddts[table] = str(max(loaddt_values))
                logger.info("Found loaddt values for table %s: %s", table, latest_loaddts[table])
            else:
                latest_loaddts[table] = None
                logger.info("No loaddt values found for table %s", table)
        return latest_loaddts
    

    def _extract_loaddt_values(self, bucket_name: str, prefix: str) -> List[int]:
        """
        Extracts the load date from the file names in the specified OBS bucket and prefix.
        
        Args:
        -----
        :param bucket_name: The name of the OBS bucket.
        :param prefix: The prefix used to filter the files.

        Returns:
        --------
        A list of load dates as integers.
        """
        try:
            objects = self.client.listObjects(bucket_name, prefix=prefix)
            loaddt_values = [
                int(obj["key"].split("loaddt=")[1].split("/")[0])
                for obj in objects["body"]["contents"]
                if "loaddt=" in obj["key"]
            ]
            return loaddt_values
        except Exception as e:
            logger.error("Error extracting loaddt values for prefix %s: %s", prefix, e)
            return []
