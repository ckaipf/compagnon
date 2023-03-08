import subprocess
import tempfile
from typing import Any, Dict

from compagnon.domain.model import AbstractExecution
from compagnon.fetchers.fetchers import CogdatFetcher
import pathlib


class SmoothieExecution(AbstractExecution):
    execution_name = "smoothie"

    def data_parser(self, record: dict):
        return record["file_ids"]

    def command(self, file_ids: Dict[str, Dict[str, str]]):
        fetcher = CogdatFetcher()
        result = dict()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir = pathlib.Path(tmp_dir)
            
            for file in ["RawFQ1", "RawFQ2", "AssemblyFA"]:
                file_local = fetcher.get_file(self.record, lambda x: x.data["file_ids"][file]["site"], path_prefix=tmp_dir)

                cmd = "echo -n ' puree' >> " + str(file_local)
                subprocess.run(cmd, capture_output=True, shell=True, check=False)
                proc = subprocess.run(["cat", str(file_local)], capture_output=True, check=False)
                result[file] = proc.stdout.decode("utf-8").strip()
        
        return result

    def result_parser(self, x) -> Dict[str, Any]:
        return x
