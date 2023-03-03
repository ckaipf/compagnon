import json
import os

import pytest
import requests
import validators
import yaml
from datameta_client import files, metadatasets, submissions

from compagnon.domain.model import ExecutionFactory

cogdat_fixtures_path = os.path.join("tests", "fixtures", "cogdat")
FILE_KEYS = ["RawFQ1", "RawFQ2", "AssemblyFA"]


def get_content(path: str):
    with open(path, "r") as test_file:
        yield test_file.read()


def get_file_if_url(path: str):
    if validators.url(path):
        r = requests.get(path)
        local_path = os.path.join(
            cogdat_fixtures_path, "downloads", path.split("/")[-1]
        )
        with open(local_path, "w") as f:
            f.write(r.content)
        yield local_path


@pytest.fixture()
def example_metadatasets():
    metadatasets_path = os.path.join(cogdat_fixtures_path, "metadatasets.yml")
    with open(metadatasets_path, "r") as yaml_file:
        ms = yaml.safe_load(yaml_file)
    yield ms


def file_paths_of_metadataset(metadataset):
    file_paths = []
    for key, value in metadataset.items():
        if key in FILE_KEYS:
            if validators.url(value):
                local_path = os.path.join(
                    cogdat_fixtures_path, "downloads", value.split("/")[-1]
                )
                if not os.path.isfile(local_path):
                    with open(local_path, "wb") as f:
                        f.write(requests.get(value).content)
                file_paths.append(local_path)
            else:
                file_paths.append(os.path.join(cogdat_fixtures_path, value))
    return file_paths


def replace_urls_with_file_names(metadataset):
    return {
        key: value.split("/")[-1]
        if key in FILE_KEYS and validators.url(value)
        else value
        for key, value in metadataset.items()
    }


@pytest.fixture()
def ensure_metadataset_is_submitted(example_metadatasets):
    response = metadatasets.search()
    response = [record["record"]["IMS-ID"] for record in response]
    for metadataset in example_metadatasets.values():
        if metadataset["IMS-ID"] not in response:
            response = metadatasets.stage(
                metadata_json=json.dumps(replace_urls_with_file_names(metadataset)),
                quiet=True,
            )
            metadataset_ids = response["id"]["uuid"]

            file_ids = [
                files.stage(path=f, quiet=True,)[
                    "id"
                ]["uuid"]
                for f in file_paths_of_metadataset(metadataset)
            ]

            valid = submissions.prevalidate(
                metadataset_ids=metadataset_ids,
                file_ids=file_ids,
                label="test",
                quiet=True,
            )
            assert valid, "Submission failed prevalidation."

            response = submissions.submit(
                metadataset_ids=metadataset_ids,
                file_ids=file_ids,
                label="test",
                quiet=True,
            )
    return True


@pytest.fixture()
def get_metadasets_site_ids(ensure_metadataset_is_submitted):
    response = metadatasets.search()
    yield [record["id"]["site"] for record in response]
