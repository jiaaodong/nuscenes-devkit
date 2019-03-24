#!/usr/bin/env python

# This script is the same with nuscenes.py in python-sdk but this file is a simplified version
# It is compatible with python 2.7
# This is modified by Jiaao Tung
from __future__ import print_function
import future_builtins

import yaml
import rosbag
import json
import sys 
import os.path as osp
import time
import numpy as np 

class data_query:

    def __init__(self, data_root, version, verbose = True):

        if version not in ['v0.1']:
            raise ValueError('Invalid DB version: {}'.format(version))
        self.version = version
        self.data_root_ = data_root
        self.table_root_ = osp.join(self.data_root_, version) 
        assert osp.exists(self.table_root_), 'Database version not found: {}'.format(self.table_root_)
        
        self.table_names = ['category', 'attribute', 'visibility', 'instance', 'sensor', 'calibrated_sensor',
                            'ego_pose', 'log', 'scene', 'sample', 'sample_data', 'sample_annotation', 'map']        
        
        start_time = time.time()
        if verbose:
            print("======\nLoading NuScenes tables for version {} ...".format(self.version))
        self.category = self.__load_table__('category')
        self.attribute = self.__load_table__('attribute')
        self.visibility = self.__load_table__('visibility')
        self.instance = self.__load_table__('instance')
        self.sensor = self.__load_table__('sensor')
        self.calibrated_sensor = self.__load_table__('calibrated_sensor')
        self.ego_pose = self.__load_table__('ego_pose')
        self.log = self.__load_table__('log')
        self.scene = self.__load_table__('scene')
        self.sample = self.__load_table__('sample')
        self.sample_data = self.__load_table__('sample_data')
        self.sample_annotation = self.__load_table__('sample_annotation')
        self.map = self.__load_table__('map')

        # # Initialize map mask for each map record.
        # for map_record in self.map:
        #     map_record['mask'] = MapMask(osp.join(self.data_root_, map_record['filename']))

        if verbose:
            for table in self.table_names:
                print("{} {},".format(len(getattr(self, table)), table))
            print("Done loading in {:.1f} seconds.\n======".format(time.time() - start_time))

        # Make reverse indexes for common lookups.
        self.__make_reverse_index__(verbose)



    def table_root(self) :
        """ Returns the folder where the tables are stored for the relevant version. """
        return osp.join(self.data_root_, self.version)

    def __load_table__(self, table_name) :
        """ Loads a table. """
        with open(osp.join(self.table_root(), '{}.json'.format(table_name))) as f:
            table = json.load(f)
        return table

    def __make_reverse_index__(self, verbose) :
        """
        De-normalizes database to create reverse indices for common cases.
        :param verbose: Whether to print outputs.
        """

        start_time = time.time()
        if verbose:
            print("Reverse indexing ...")

        # Store the mapping from token to table index for each table.
        self._token2ind = dict()
        for table in self.table_names:
            self._token2ind[table] = dict()

            for ind, member in enumerate(getattr(self, table)):
                self._token2ind[table][member['token']] = ind

        # Decorate (adds short-cut) sample_annotation table with for category name.
        for record in self.sample_annotation:
            inst = self.get('instance', record['instance_token'])
            record['category_name'] = self.get('category', inst['category_token'])['name']

        # Decorate (adds short-cut) sample_data with sensor information.
        for record in self.sample_data:
            cs_record = self.get('calibrated_sensor', record['calibrated_sensor_token'])
            sensor_record = self.get('sensor', cs_record['sensor_token'])
            record['sensor_modality'] = sensor_record['modality']
            record['channel'] = sensor_record['channel']

        # Reverse-index samples with sample_data and annotations.
        for record in self.sample:
            record['data'] = {}
            record['anns'] = []

        for record in self.sample_data:
            if record['is_key_frame']:
                sample_record = self.get('sample', record['sample_token'])
                sample_record['data'][record['channel']] = record['token']

        for ann_record in self.sample_annotation:
            sample_record = self.get('sample', ann_record['sample_token'])
            sample_record['anns'].append(ann_record['token'])

        # Add reverse indices from log records to map records.
        # for log_record in self.log:
        #     map_token = self.field2token('map', 'log_token', log_record['token'])[0]
        #     log_record['map_token'] = self.get('map', map_token)['token']

        if verbose:
            print("Done reverse indexing in {:.1f} seconds.\n======".format(time.time() - start_time))

    def get(self, table_name, token) :
        """
        Returns a record from table in constant runtime.
        :param table_name: Table name.
        :param token: Token of the record.
        :return: Table record. See README.md for record details for each table.
        """
        assert table_name in self.table_names, "Table {} not found".format(table_name)

        return getattr(self, table_name)[self.getind(table_name, token)]

    def getind(self, table_name, token) :
        """
        This returns the index of the record in a table in constant runtime.
        :param table_name: Table name.
        :param token: Token of the record.
        :return: The index of the record in table, table is an array.
        """
        return self._token2ind[table_name][token]

    def field2token(self, table_name, field, query) :
        """
        This function queries all record for a certain field value, and returns the tokens for the matching records.
        Warning: this runs in linear time.
        :param table_name: Table name.
        :param field: Field name. See README.md for details.
        :param query: Query to match against. Needs to type match the content of the query field.
        :return of tokens for the matching records.
        """
        matches = []
        for member in getattr(self, table_name):
            if member[field] == query:
                matches.append(member['token'])
        return matches


if __name__ == "__main__":
    data = data_query(data_root = '/mnt/B4CACF59CACF1690/UbuntuData/nuscenes_data/nuscenes', version = 'v0.1')
    
    pass