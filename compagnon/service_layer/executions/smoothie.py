import pathlib
import tempfile
from typing import Any, Dict

from compagnon.domain.model import AbstractExecution
from compagnon.fetchers.fetchers import CogdatFetcher
from compagnon.service_layer.external_process import SubprocessProcess


class SmoothieExecution(AbstractExecution):
    execution_name = "smoothie"

    def command(self, **kwargs):
        fetcher = CogdatFetcher()
        result = dict()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir = pathlib.Path(tmp_dir)

            for file in ["RawFQ1", "RawFQ2", "AssemblyFA"]:
                file_local = fetcher.get_file(
                    self.record,
                    lambda x: x.data["file_ids"][file]["site"],
                    path_prefix=tmp_dir,
                )
                SubprocessProcess("echo -n ' puree' >> " + str(file_local)).run()
                result[file] = SubprocessProcess("cat " + str(file_local)).run()

        return result
