import tempfile
from typing import Any, Dict
import pathlib

import pandas

from compagnon import config as config
from compagnon.domain.model import AbstractExecution
from compagnon.fetchers.fetchers import CogdatFetcher
from compagnon.service_layer.external_process import SubprocessProcess


def parse_kraken_result_file(path: str, kwargs) -> pandas.DataFrame:
    fieldnames = [
        field["fieldname"]
        for _, field in sorted(
            kwargs["kraken_result_file_fields"].items(), key=lambda x: x[0]
        )
    ]
    dypes = {
        v["fieldname"]: v["dtype"]
        for _, v in kwargs["kraken_result_file_fields"].items()
    }
    df = pandas.read_csv(path, delimiter="\t", names=fieldnames, dtype=dypes)
    return df


class KrakenExecution(AbstractExecution):
    execution_name = "kraken"

    def data_parser(self, record: dict):
        return record["file_ids"]

    @config.add_config()
    def command(self, file_ids: Dict[str, Dict[str, str]], **kwargs):
        fetcher = CogdatFetcher()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir = pathlib.Path(tmp_dir)
            
            file_1 = fetcher.get_file(self.record, lambda x: x.data["file_ids"]["RawFQ1"]["site"], path_prefix=tmp_dir)
            file_2 = fetcher.get_file(self.record, lambda x: x.data["file_ids"]["RawFQ2"]["site"], path_prefix=tmp_dir)

            report_file = tmp_dir / "kraken2.report.txt"
            paired = True

            cmd = f"""
            {kwargs["kraken_binary"]} \
            --db {kwargs["kraken_db"]}  \
            --report {report_file} \
            {"--paired" if paired else " "} \
            {str(file_1) + " " + str(file_2) if paired else str(file_1)} \
            > /dev/null \
            """

            SubprocessProcess(cmd).run()
            df = parse_kraken_result_file(report_file, kwargs)

        #
        # prepare returned results
        #

        # no compr possible, pandas doesn't find local variables
        queries_truth = dict()
        for key, query in {
            "has_sars": 'ncbi_id in @kwargs["sars_cov_ids"] & pct > 95.00',
            "has_homo": 'ncbi_id in @kwargs["homo_sapiens_ids"] & pct > 00.00',
        }.items():
            queries_truth[key] = df.query(query).empty

        return {
            "kraken_db": kwargs["kraken_db"],
        } | queries_truth

    def result_parser(self, result) -> Dict[str, Any]:
        return result
