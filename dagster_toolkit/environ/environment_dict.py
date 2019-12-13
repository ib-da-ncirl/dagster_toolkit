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

import pprint


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
        self._e_dict = {
            'solids': {},
            'resources': {}
        }

    def add_solid(self, solid_name):
        """
        Add a solid to the environment dictionary
        :param solid_name: name of solid to add
        """
        return self.__add_solid(solid_name)

    def __add_solid(self, solid_name, chk_dict=None):
        """
        Add a solid to the environment dictionary
        :param solid_name: name of solid to add
        """
        if self._solid_name_check(solid_name, chk_dict=chk_dict) == EnvironmentDict._VALID:
            if chk_dict is None:
                chk_dict = self._e_dict
            chk_dict['solids'][solid_name] = {}
        return self

    def _key_check(self, key_list, chk_dict=None):
        """
        Check if a key chain exists in the dictionary
        :param key_list: list of keys ranging from top to bottom
        :return: True is full key chain exists
        """
        exists = False
        if chk_dict is None:
            chk_dict = self._e_dict
        for key in key_list:
            exists = key in chk_dict.keys()
            if exists:
                chk_dict = chk_dict[key]
            else:
                break
        return exists

    def _name_check(self, name, *args, chk_dict=None):
        """
        Check a name is valid
        :param key: environment dictionary key to check
        :param name: name of solid to add
        """
        if name is not None and len(name) > 0:
            lst = list(args)
            lst.append(name)
            if self._key_check(lst, chk_dict=chk_dict):
                result = EnvironmentDict._EXISTS
            else:
                result = EnvironmentDict._VALID
        else:
            result = EnvironmentDict._INVALID
            raise ValueError(f'Invalid name: {name}')
        return result

    def _solid_name_check(self, solid_name, chk_dict=None):
        """
        Check a solid is valid
        :param solid_name: name of solid to add
        """
        return self._name_check(solid_name, 'solids', chk_dict=chk_dict)

    def _resource_name_check(self, resource_name):
        """
        Check a solid is valid
        :param resource_name: name of resource
        """
        return self._name_check(resource_name, 'resources')

    def add_solid_input(self, solid_name, input_name, value, is_kwargs=False, chk_dict=None):
        """
        Add an input to a solid to the environment dictionary
        :param solid_name: name of solid
        :param input_name: name of input to add to solid
        :param value: value to set for input
        :param is_kwargs: flag to indicate if input is a kwargs
        :param chk_dict: environment dict to use
        """
        # add_solid_input('read_csv', 'csv_path', 'cereal.csv')
        # results in
        # environment_dict = {
        #     'solids': {
        #         'read_csv': {
        #             'inputs': {
        #                 'csv_path': {'value': 'cereal.csv'}
        #             }
        #          }
        #     }
        # }
        name_check = self._solid_name_check(solid_name, chk_dict=chk_dict)
        if name_check == EnvironmentDict._VALID or name_check == EnvironmentDict._EXISTS:
            if chk_dict is None:
                chk_dict = self._e_dict
            if name_check == EnvironmentDict._VALID:
                chk_dict['solids'][solid_name] = {'inputs': {}}
            EnvironmentDict.__add_solid_input(chk_dict['solids'][solid_name]['inputs'],
                                              input_name, value, is_kwargs=is_kwargs)
        return self

    @staticmethod
    def __add_solid_input(solid_inputs, input_name, value, is_kwargs=False):
        """
        Add an input to a solid to the environment dictionary
        :param solid_inputs: inputs dict of solid
        :param input_name: name of input to add to solid
        :param value: value to set for input
        """
        if is_kwargs:
            solid_inputs[input_name] = value
        else:
            solid_inputs[input_name] = {'value': value}

    def add_composite_solid(self, solid_name, child_solid_name):
        """
        Add a composite solid to the environment dictionary
        :param solid_name: name of solid
        :param child_solid_name: name of child solid
        """
        # add_composite_solid('load_cereals', 'read_cereals')
        # results in
        # environment_dict = {
        #     'solids': {
        #         'load_cereals': {
        #             'solids': {
        #                 'read_csv': {}
        #                 }
        #             }
        #          }
        #     }
        # }
        name_check = self._solid_name_check(solid_name)
        if name_check == EnvironmentDict._VALID or name_check == EnvironmentDict._EXISTS:
            if name_check == EnvironmentDict._VALID:
                self._e_dict['solids'][solid_name] = {'solids': {}}

            composite_dict = self._e_dict['solids'][solid_name]
            self.__add_solid(child_solid_name, chk_dict=composite_dict)
        return self

    def add_composite_solid_input(self, solid_name, child_solid_name, input_name, value, is_kwargs=False):
        """
        Add an input to a composite solid to the environment dictionary
        :param solid_name: name of solid
        :param child_solid_name: name of child solid
        :param input_name: name of input to add to solid
        :param value: value to set for input
        """
        # add_composite_solid_input('load_cereals', 'read_cereals', 'csv_path', 'cereal.csv')
        # results in
        # environment_dict = {
        #     'solids': {
        #         'load_cereals': {
        #             'solids': {
        #                 'read_cereals': {
        #                     'inputs': {
        #                         'csv_path': {'value': 'cereal.csv'}
        #                     }
        #                 }
        #             }
        #          }
        #     }
        # }
        name_check = self._solid_name_check(solid_name)
        if name_check == EnvironmentDict._VALID or name_check == EnvironmentDict._EXISTS:
            if name_check == EnvironmentDict._VALID:
                self._e_dict['solids'][solid_name] = {'solids': {}}

            composite_dict = self._e_dict['solids'][solid_name]
            self.add_solid_input(child_solid_name, input_name, value, is_kwargs=is_kwargs, chk_dict=composite_dict)
        return self

    def add_resource(self, resource_name, value):
        """
        Add a resource to the environment dictionary
        :param resource_name: name of resource
        :param value: value to set
        """
        name_check = self._resource_name_check(resource_name)
        if name_check == EnvironmentDict._VALID or name_check == EnvironmentDict._EXISTS:
            self._e_dict['resources'][resource_name] = value
        return self

    def build(self):
        return self._e_dict.copy()

    def clear(self):
        return self._e_dict.clear()

    def __str__(self):
        return pprint.pformat(self._e_dict)

