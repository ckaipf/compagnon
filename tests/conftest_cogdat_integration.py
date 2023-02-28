import json
import os

import pytest
import yaml
from datameta_client import files, metadatasets, submissions

from compagnon.domain.model import ExecutionFactory

cogdat_fixtures_path = os.path.join("tests", "fixtures", "cogdat")
FILE_KEYS = ["RawFQ1", "RawFQ2", "AssemblyFA"]


def get_content(path: str):
    with open(path, "r") as test_file:
        yield test_file.read()


@pytest.fixture()
def example_metadatasets():
    metadatasets_path = os.path.join(cogdat_fixtures_path, "metadatasets.yml")
    with open(metadatasets_path, "r") as yaml_file:
        ms = yaml.safe_load(yaml_file)
    yield ms


@pytest.fixture()
def example_files(example_metadatasets):
    current_file_paths = []
    for metadataset in example_metadatasets.values():
        for key, value in metadataset.items():
            if key in FILE_KEYS:
                current_file_paths.append(os.path.join(cogdat_fixtures_path, value))
    yield current_file_paths


@pytest.fixture()
def ensure_metadataset_is_submitted(example_metadatasets, example_files):
    response = metadatasets.search()
    response = [record["record"]["IMS-ID"] for record in response]
    for metadataset in example_metadatasets.values():
        if metadataset["IMS-ID"] not in response:
            response = metadatasets.stage(metadata_json=json.dumps(metadataset))
            metadataset_ids = response["id"]["uuid"]

            file_ids = [files.stage(path=f)["id"]["uuid"] for f in example_files]

            valid = submissions.prevalidate(
                metadataset_ids=metadataset_ids, file_ids=file_ids, label="test"
            )
            assert valid, "Submission failed prevalidation."

            response = submissions.submit(
                metadataset_ids=metadataset_ids, file_ids=file_ids, label="test"
            )
    return True

@pytest.fixture()
def get_metadasets_site_ids(ensure_metadataset_is_submitted):
    response = metadatasets.search()
    yield [record["id"]["site"] for record in response]
