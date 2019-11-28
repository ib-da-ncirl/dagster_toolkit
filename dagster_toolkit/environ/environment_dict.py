# The MIT License (MIT)
# Copyright (c) 2019 Ian Buttimer

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


class EnvironmentDict:
    """
    Dagster environment dictionary builder
    """

    _INVALID = 0
    _EXISTS = 2
    _VALID = 3

    def __init__(self):
        """
        Initialise object
        """
        self.e_dict = {
            'solids': {},
            'resources': {}
        }

    def add_solid(self, solid_name):
        """
        Add a solid to the environment dictionary
        :param solid_name: name of solid to add
        """
        if self._solid_name_check(solid_name) == EnvironmentDict._VALID:
            self.e_dict['solids'][solid_name] = {}
        return self

    def _name_check(self, key, name):
        """
        Check a name is valid
        :param key: environment dictionary key to check
        :param name: name of solid to add
        """
        if name is not None and len(name) > 0:
            if name in self.e_dict[key].keys():
                result = EnvironmentDict._EXISTS
            else:
                result = EnvironmentDict._VALID
        else:
            result = EnvironmentDict._INVALID
            raise ValueError(f'Invalid {key} name: {name}')
        return result

    def _solid_name_check(self, solid_name):
        """
        Check a solid is valid
        :param solid_name: name of solid to add
        """
        return self._name_check('solids', solid_name)

    def _resource_name_check(self, resource_name):
        """
        Check a solid is valid
        :param resource_name: name of resource
        """
        return self._name_check('resources', resource_name)

    def add_solid_input(self, solid_name, input_name, value, is_kwargs=False):
        """
        Add an input to a solid to the environment dictionary
        :param solid_name: name of solid
        :param input_name: name of input to add to solid
        :param value: value to set for input
        """
        name_check = self._solid_name_check(solid_name)
        if name_check == EnvironmentDict._VALID or name_check == EnvironmentDict._EXISTS:
            if name_check == EnvironmentDict._VALID:
                self.e_dict['solids'][solid_name] = {'inputs': {}}

            if is_kwargs:
                self.e_dict['solids'][solid_name]['inputs'][input_name] = value
            else:
                self.e_dict['solids'][solid_name]['inputs'][input_name] = {'value': value}
        return self

    def add_resource(self, resource_name, value):
        """
        Add a resource to the environment dictionary
        :param resource_name: name of resource
        :param value: value to set
        """
        name_check = self._resource_name_check(resource_name)
        if name_check == EnvironmentDict._VALID or name_check == EnvironmentDict._EXISTS:
            self.e_dict['resources'][resource_name] = value
        return self

    def build(self):
        return self.e_dict.copy()

    def clear(self):
        return self.e_dict.clear()
