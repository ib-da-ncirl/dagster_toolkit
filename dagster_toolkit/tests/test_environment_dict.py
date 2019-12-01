from unittest import TestCase

from environ import EnvironmentDict


class TestEnvironmentDict(TestCase):
    def test_add_solid(self):
        environ = EnvironmentDict()
        environ.add_solid('new_solid')

        self.assertTrue('solids' in environ._e_dict.keys())
        self.assertTrue('new_solid' in environ._e_dict['solids'].keys())

    def test_add_solid_input(self):
        environ = EnvironmentDict()
        environ.add_solid_input('new_solid', 'solid_ip', 'ip_value')

        self.assertTrue('solids' in environ._e_dict.keys())
        self.assertTrue('new_solid' in environ._e_dict['solids'].keys())
        self.assertTrue('inputs' in environ._e_dict['solids']['new_solid'].keys())
        self.assertTrue('solid_ip' in environ._e_dict['solids']['new_solid']['inputs'].keys())
        self.assertTrue('value' in environ._e_dict['solids']['new_solid']['inputs']['solid_ip'].keys())
        self.assertEqual(environ._e_dict['solids']['new_solid']['inputs']['solid_ip']['value'], 'ip_value')

    def test_add_composite_solid_input(self):
        environ = EnvironmentDict()
        environ.add_composite_solid_input('new_solid', 'child_solid', 'solid_ip', 'ip_value')

        self.assertTrue('solids' in environ._e_dict.keys())
        self.assertTrue('new_solid' in environ._e_dict['solids'].keys())
        self.assertTrue('solids' in environ._e_dict['solids']['new_solid'].keys())
        self.assertTrue('child_solid' in environ._e_dict['solids']['new_solid']['solids'].keys())
        self.assertTrue('inputs' in environ._e_dict['solids']['new_solid']['solids']['child_solid'].keys())
        self.assertTrue('solid_ip' in environ._e_dict['solids']['new_solid']['solids']['child_solid']['inputs'].keys())
        self.assertTrue('value' in environ._e_dict['solids']['new_solid']['solids']['child_solid']['inputs']['solid_ip'].keys())
        self.assertEqual(environ._e_dict['solids']['new_solid']['solids']['child_solid']['inputs']['solid_ip']['value'], 'ip_value')

    def test_add_resource(self):
        environ = EnvironmentDict()
        environ.add_resource('new_resource', 'resource_value')

        self.assertTrue('resources' in environ._e_dict.keys())
        self.assertTrue('new_resource' in environ._e_dict['resources'].keys())
        self.assertEqual(environ._e_dict['resources']['new_resource'], 'resource_value')
