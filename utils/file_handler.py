import os
from enum import Enum
import pandas as pd
import numpy as np
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class FileExtension(str, Enum):
    """

    Args:
        str (_type_): _description_
        Enum (_type_): _description_
    """

    XLS = "xls"
    XLSB = "xlsb"
    PARQUET = "parquet"
    TSV = "tsv"
    SQLITE = "db"


class LocalFileHandler:
    def __init__(self):
        pass

    def read_excel(self, *args, **kwargs):
        return pd.read_excel(*args, **kwargs)

    def read_csv(self, *args, **kwargs):
        return pd.read_csv(*args, **kwargs)

    def read_parquet(self, filepath):
        pass

    def write_excel(self, df_output, filepath):
        with open(filepath, "w") as file_out:
            df_output.to_excel(file_out)

    def to_csv(
        self,
        df: pd.DataFrame,
        index: bool = False,
        sep: str = "\t",
        na_rep: str = "NULL",
        *args,
        **kwargs,
    ):
        path_or_buf = kwargs.pop("path_or_buf", None)
        dirname = path_or_buf.split("/")[:-1]
        os.makedirs("/".join(dirname), exist_ok=True)
        df.to_csv(
            path_or_buf=path_or_buf,
            index=index,
            sep=sep,
            na_rep=na_rep,
            *args,
            **kwargs,
        )
        print(f"File created at: {path_or_buf}")

    def write_parquet(self, df_output, filepath):
        pass

    def delete_file(self, filepath: str) -> None:
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"The file at {filepath} has been deleted.")
        else:
            print(f"The file at {filepath} does not exist.")


class MinioFileHandler:
    def __init__(self, connection_id, bucket: str = "dsci"):
        self.connection_id = connection_id
        self.bucket = bucket
        self.minio_hook = S3Hook(aws_conn_id=self.connection_id)

    def _get_file_obj(self, file_key: str) -> bytes:
        print(f"Reading data from {file_key}")
        file_obj = self.minio_hook.get_key(bucket_name=self.bucket, key=file_key)
        file_content = file_obj.get()["Body"].read()
        return file_content

    def read_excel(self, file_name: str, *args, **kwargs) -> pd.DataFrame:
        file_content = self._get_file_obj(file_key=file_name)
        df = pd.read_excel(pd.io.common.BytesIO(file_content), *args, **kwargs)
        df = df.fillna(np.nan).replace(["", np.nan], [None, None])
        return df

    def read_csv(self, file_name: str, *args, **kwargs) -> pd.DataFrame:
        file_content = self._get_file_obj(file_key=file_name)
        df = pd.read_csv(pd.io.common.BytesIO(file_content), *args, **kwargs)
        return df

    def read_parquet(self, file_name: str, *args, **kwargs) -> pd.DataFrame:
        file_content = self._get_file_obj(file_key=file_name)
        df = pd.read_parquet(pd.io.common.BytesIO(file_content), *args, **kwargs)
        return df

    def read_file_based_on_extension(
        self, s3_filepath: str, file_extension: str
    ) -> pd.DataFrame:
        print(
            f"Reading file from {s3_filepath} with extension {file_extension} to dataframe !"
        )
        if file_extension == FileExtension.TSV.value:
            df = self.read_csv(file_name=s3_filepath, sep="\t")
        elif file_extension == FileExtension.PARQUET.value:
            df = self.read_parquet(file_name=s3_filepath)
        else:
            raise ValueError(f"Unsupported file_extension: {file_extension}")
        return df

    def download_file(self, *args, **kwargs) -> None:
        self.minio_hook.download_file(*args, **kwargs)

    def load_file_obj(self, *args, **kwargs) -> None:
        self.minio_hook.load_file_obj(*args, **kwargs)

    def load_file(self, *args, **kwargs) -> None:
        kwargs.setdefault("bucket_name", self.bucket)
        self.minio_hook.load_file(*args, **kwargs)

    def load_bytes(self, *args, **kwargs) -> None:
        print(f"Loading bytes to S3 at {kwargs['key']}")
        kwargs.setdefault("bucket_name", self.bucket)
        self.minio_hook.load_bytes(*args, **kwargs)

    def copy_object(self, *args, **kwargs) -> None:
        print(f"Copying {kwargs["source_bucket_key"]} to {kwargs["dest_bucket_key"]}")
        self.minio_hook.copy_object(*args, **kwargs)

    def delete_objects(self, *args, **kwargs) -> None:
        kwargs.setdefault("bucket", self.bucket)
        self.minio_hook.delete_objects(*args, **kwargs)

    def list_keys(self, *args, **kwargs) -> list[str]:
        kwargs.setdefault("bucket_name", self.bucket)
        return self.minio_hook.list_keys(*args, **kwargs)
