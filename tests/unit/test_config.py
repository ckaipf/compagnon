import os
import unittest

import compagnon.config as config

class TestConfig(unittest.TestCase):

    def setUp(self):
        self.test_config_path = os.path.join("tests", "fixtures", "unit", "test_configuration.yml")


    def test_get_config(self):
        c = config.get_config(self.test_config_path)
        print(c)
        assert c == {'test_config': {'a': {'b': {'c': 'd'}, 'e': 'f'}}}


    def test_add_config_params_decorator(self):
        @config.add_config(file_path=self.test_config_path)
        def f(b, key, **kwargs):
            return b, kwargs.get(key, None)
        
        assert f(0, "a") == (0, {'b': {'c': 'd'}, 'e': 'f'}) 
        assert f(0, "e") == (0, None)
