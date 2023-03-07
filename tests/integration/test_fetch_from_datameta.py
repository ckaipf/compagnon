import os
import unittest

import pytest

import compagnon.domain.model as model
import compagnon.service_layer.batchables as batchables
import compagnon.service_layer.services as services
from compagnon.fetchers.fetchers import CogdatFetcher
from compagnon.service_layer.executions.smoothie import SmoothieExecution
from compagnon.service_layer.executions.kraken import KrakenExecution

from compagnon.service_layer.unit_of_work import YamlUnitOfWork


@pytest.mark.skipif(
    "DATAMETA_TOKEN" and "DATAMETA_URL" not in os.environ,
    reason="Skipping test that requires CoGDat instance.",
)
def test_cogdat_instance_has_target_metadataset(ensure_metadataset_is_submitted):
    assert ensure_metadataset_is_submitted
    response = services.fetch_records(CogdatFetcher())
    target = [
        record
        for record in response
        if record.data.get("record").get("IMS-ID")
        in [
            "IMS-12345-CVDP-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXYY",
            "IMS-12345-CVDP-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXYYY",
        ]
    ]
    assert target


@unittest.skipIf(
    "DATAMETA_TOKEN" and "DATAMETA_URL" not in os.environ,
    "Skipping test that requires CoGDat instance.",
)
class CogdatIntegrationTest(unittest.TestCase):
    

    def setUp(self):
        self.ims_ids = [
            "IMS-12345-CVDP-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXYY",
        ]
        self.ims_kraken = [
            "IMS-12345-CVDP-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXYYY",
        ]
        self.yaml_file = "cogdat_test.yml"
        self.uow = YamlUnitOfWork(self.yaml_file)

    def tearDown(self):
        if os.path.isfile(self.yaml_file):
            os.remove(self.yaml_file)

    def test_fetch_from_cogdat(self):
        response = services.fetch_records(CogdatFetcher())
        retrived = [
            record
            for record in response
            if record.data.get("record").get("IMS-ID") in self.ims_ids
        ]
        assert retrived
        return retrived

    def test_fetch_and_store_records_from_cogdat(self):
        records = services.fetch_records(CogdatFetcher())
        uow = YamlUnitOfWork(self.yaml_file)
        services.add_records(records, uow)
        del uow
        uow = YamlUnitOfWork(self.yaml_file)
        with uow:
            for record in uow.records.list():
                assert record.foreign_id in (record.foreign_id for record in records)

    def test_executions_are_committed_to_db(self):
        response = services.fetch_records(CogdatFetcher())
        
        target = [
            record
            for record in response
            if record.data.get("record").get("IMS-ID")
            in self.ims_ids
            ]
        
        services.add_records(target, self.uow)
        services.add_execution_to_records(SmoothieExecution, self.uow)
        
        with self.uow:
            for record in self.uow.records.list():
                assert record.executions[-1].__class__ == SmoothieExecution
                assert not record.executions[-1].result

    def test_local_state_is_not_equal_to_remote(self):
        assert services.exist_unseen_records_in_remote(self.uow, CogdatFetcher())

    def test_add_missing_records_from_remote(self):
        assert services.exist_unseen_records_in_remote(self.uow, CogdatFetcher())
        batchables.add_missing_records_from_remote(self.uow, CogdatFetcher())
        assert not services.exist_unseen_records_in_remote(self.uow, CogdatFetcher())

    def test_batch_execution(self):
        response = services.fetch_records(CogdatFetcher())
        
        target = [
            record
            for record in response
            if record.data.get("record").get("IMS-ID")
            in self.ims_ids
            ]
        services.add_records(target, self.uow)
        
        
        services.add_execution_to_records(SmoothieExecution, self.uow)
        batchables.execute_executions(self.uow)

        with self.uow:
            for record in self.uow.records.list():
                assert record.executions[-1].result["RawFQ1"] == "potato puree"
                assert record.executions[-1].result["RawFQ2"] == "apple puree"
                assert record.executions[-1].result["AssemblyFA"] == "banana puree"
    
    
    def test_kraken_execution(self):
            response = services.fetch_records(CogdatFetcher())

            target = [
            record
            for record in response
            if record.data.get("record").get("IMS-ID")
            in self.ims_kraken
            ]

            services.add_records(target, self.uow)
            services.add_execution_to_records(KrakenExecution, self.uow)
            batchables.execute_executions(self.uow)


# def test_compare_local_and_remote(self):
#     pass

# def test_add_missing_records_from_remote(self):
#     # TODO: Be sure that no local records are overwritten
#     pass


# test no records added
# test no executions added
