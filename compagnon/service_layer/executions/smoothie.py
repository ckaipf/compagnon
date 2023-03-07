import subprocess
import tempfile
from typing import Any, Dict
import re, os
import requests
from datameta_client import files

from compagnon.domain.model import AbstractExecution


def download_file(url: str, path: str) -> str:
    response = requests.get(url, stream=True)
    response.raise_for_status()
    headers = response.headers['content-disposition']
    filename = re.findall("filename=(.+)", headers)[0].replace('"', '').replace("'", '')
    local_filename = os.path.join(path, filename)
    with open(local_filename, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    return local_filename


class SmoothieExecution(AbstractExecution):
    execution_name = "smoothie"

    def data_parser(self, record: dict):
        return record["file_ids"]

    def command(self, file_ids: Dict[str, Dict[str, str]]):
        result = dict()
        for file_name, file_id in file_ids.items():
            response = files.download_url(file_id["site"])
            file_url = response["file_url"]

            with tempfile.TemporaryDirectory() as tmp_dir:
                file_ = download_file(url=file_url, path=tmp_dir)
                cmd = "echo -n ' puree' >> " + file_
                subprocess.run(cmd, capture_output=True, shell=True, check=False)
                proc = subprocess.run(["cat", file_], capture_output=True, check=False)
                result[file_name] = proc.stdout.decode("utf-8").strip()
        return result

    def result_parser(self, x) -> Dict[str, Any]:
        return x
