import subprocess
import tempfile
from typing import Any, Dict

import requests
from datameta_client import files

from compagnon.domain.model import ExecutionFactory


def download_file(url: str, path: str) -> str:
    local_filename = path + "/" + url.split("/")[-1]
    # TODO url not exists, path
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename


class SmoothieExecution(ExecutionFactory):
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


