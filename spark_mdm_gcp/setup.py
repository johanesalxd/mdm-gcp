#!/usr/bin/env python3
"""
Setup script for MDM Spark dependencies wheel.

This creates a wheel package containing all required dependencies
for the MDM Spark data generator, including faker for synthetic data generation.
"""

from setuptools import find_packages
from setuptools import setup

setup(
    name='mdm_spark_dependencies',
    version='1.0.0',
    description='Dependencies package for MDM Spark data generator',
    author='MDM Team',
    author_email='mdm-team@example.com',
    packages=find_packages(),
    install_requires=[
        'faker>=19.0.0',
        # Add other dependencies if needed in the future
    ],
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
