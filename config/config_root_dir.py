# -*- coding: utf-8 -*-
"""
Root configuration file for Pypeline
"""

import os


ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DIR = os.path.join(os.path.dirname(ROOT_DIR), 'tests')
TEST_DIR_FILES_STORE = os.path.join(TEST_DIR, 'data', 'test_file_store')
TEST_DIR_FILE_DB = os.path.join(TEST_DIR, 'data', 'test_file_db')
TEST_DIR_S3 = os.path.join(TEST_DIR, 'data', 'test_s3')
TEST_DIR_ZARR = os.path.join(TEST_DIR, 'data', 'test_zarr')
