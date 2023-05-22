from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='inditex_anomaly_detector',
    version='1.0.528',
    packages=find_packages(),  # include/exclude arguments take * as wildcard, . for any sub-package names
    install_requires=requirements
)