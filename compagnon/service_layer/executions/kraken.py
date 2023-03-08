import os
import re
import subprocess
import tempfile
from typing import Any, Dict

import pandas
import requests
from datameta_client import files

from compagnon import config as config
from compagnon.domain.model import AbstractExecution


def parse_kraken_result_file(path: str, kwargs) -> pandas.DataFrame:
    fieldnames = [
        field["fieldname"]
        for _, field in sorted(
            kwargs["kraken_result_file_fields"].items(), key=lambda x: x[0]
        )
    ]
    dypes = {
        v["fieldname"]: v["dtype"]
        for k, v in kwargs["kraken_result_file_fields"].items()
    }
    df = pandas.read_csv(path, delimiter="\t", names=fieldnames, dtype=dypes)
    return df


def download_file(url: str, path: str) -> str:
    response = requests.get(url, stream=True)
    response.raise_for_status()
    headers = response.headers["content-disposition"]
    filename = re.findall("filename=(.+)", headers)[0].replace('"', "").replace("'", "")
    local_filename = os.path.join(path, filename)
    with open(local_filename, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    return local_filename


class KrakenExecution(AbstractExecution):
    execution_name = "kraken"

    def __init__(self, *args, **kwargs):
        super(KrakenExecution, self).__init__(*args, **kwargs)

    def data_parser(self, record: dict):
        return record["file_ids"]

    @config.add_config()
    def command(self, file_ids: Dict[str, Dict[str, str]], **kwargs):
        file_urls = dict()

        for file_name, file_id in file_ids.items():
            response = files.download_url(file_id["site"])
            file_urls[file_name] = response["file_url"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir = "/tmp/test"
            file_1 = download_file(url=file_urls["RawFQ1"], path=tmp_dir)
            file_2 = download_file(url=file_urls["RawFQ2"], path=tmp_dir)
            report_file = os.path.join(tmp_dir, "kraken2.report.txt")
            paired = True

            cmd = f"""
            {kwargs["kraken_binary"]} \
            --db {kwargs["kraken_db"]}  \
            --report {report_file} \
            {"--paired" if paired else " "} \
            {file_1 + " " + file_2 if paired else file_1} \
            > /dev/null 2>&1 \
            """

            p = subprocess.run(cmd, capture_output=True, shell=True, check=False)

            # TODO: error handling, p.stderr.decode('utf-8').strip()

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
