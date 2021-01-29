# -*- coding: utf-8 -*-
"""
Root configuration file for Pypeline
"""

import os


ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DIR = os.path.join(os.path.dirname(ROOT_DIR), 'bc2-data', 'tests')
TEST_DIR_CORE = os.path.join(TEST_DIR, 'data', 'test_core')
TEST_DIR_METADATA = os.path.join(TEST_DIR, 'data', 'test_metadata')
TEST_DIR_PARTITIONS = os.path.join(TEST_DIR, 'data', 'test_partitions')
TEST_DIR_FILE_STORE = os.path.join(TEST_DIR, 'data', 'test_file_store')
