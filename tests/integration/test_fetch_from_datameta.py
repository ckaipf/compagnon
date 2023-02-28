import os
import unittest

import pytest

import compagnon.service_layer.services as services
from compagnon.service_layer.executions.smoothie import SmoothieExecution
from compagnon.service_layer.unit_of_work import YamlUnitOfWork


@pytest.mark.skipif("DATAMETA_TOKEN" and "DATAMETA_URL" not in os.environ, reason="Skipping test that requires CoGDat instance.")
def test_cogdat_instance_has_target_metadataset(ensure_metadataset_is_submitted):
        assert ensure_metadataset_is_submitted
        response = services.get_records_from_datameta()    
        target = [record for record in response if record.data.get("record").get("IMS-ID") == "IMS-12345-CVDP-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXYY"]
        assert target

@unittest.skipIf("DATAMETA_TOKEN" and "DATAMETA_URL" not in os.environ, "Skipping test that requires CoGDat instance.")
class CogdatIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.yaml_file = "cogdat_test.yml"
        
    def tearDown(self):
        os.remove(self.yaml_file)

    def test_get_records_from_datameta_and_add_execution_on_files(self):
        response = services.get_records_from_datameta()    
        uow = YamlUnitOfWork(self.yaml_file)
        target = [record for record in response if record.data.get("record").get("IMS-ID") == "IMS-12345-CVDP-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXYY"]
        services.add_records(target, uow)
        services.add_execution_to_records(SmoothieExecution, uow)

        with uow:
            for record in uow.records.list():
                for execution in record.executions:
                    assert execution.result['AssemblyFA'] == 'banana puree'
                    assert execution.result['RawFQ1'] == 'potato puree'
                    assert execution.result['RawFQ2'] == 'apple puree'
