import io
import pathlib

from setuptools import setup, find_packages

version_data = {}
version_file = pathlib.Path(__file__).parent / 'azure_dbr_scim_sync/version.py'
with version_file.open('r') as f:
    exec(f.read(), version_data)

setup(name="azure_dbr_scim_sync",
      version=version_data['__version__'],
      packages=find_packages(exclude=["tests", "*tests.*", "*tests"]),
      python_requires=">=3.8",
      install_requires=[
          "pydantic==2.3.0", "pyyaml==6.0.1", "databricks-sdk==0.9.0", "requests", "click==8.1.7",
          "coloredlogs==15.0.1", "joblib", "fsspec==2023.9.2", "adlfs==2023.9.0", "azure-identity==1.15.0"
      ],
      extras_require={
          "dev": [
              "databricks-connect==13.3.2", "pytest==7.4.2", "pytest-cov==4.1.0", "pytest-xdist",
              "pytest-mock", "yapf", "pycodestyle", "autoflake", "isort", "wheel",
              "pytest-approvaltests==0.2.4", "pylint==2.17.5", "pyright==1.1.327"
          ],
      },
      entry_points={'console_scripts': ['azure_dbr_scim_sync=azure_dbr_scim_sync.cli:sync_cli']},
      author="Grzegorz Rusin",
      author_email="grzegorz.rusin@databricks.com",
      description="Azure Databricks SCIM Sync",
      long_description=io.open("README.md", encoding="utf-8").read(),
      long_description_content_type='text/markdown',
      keywords="Azure Databricks SCIM Sync",
      classifiers=[
          "Development Status :: 4 - Beta", "Intended Audience :: Developers",
          "Intended Audience :: Science/Research", "Intended Audience :: System Administrators",
          "License :: OSI Approved :: Apache Software License", "Programming Language :: Python :: 3.7",
          "Programming Language :: Python :: 3.8", "Programming Language :: Python :: 3.9",
          "Programming Language :: Python :: 3.10", "Programming Language :: Python :: 3.11",
          "Operating System :: OS Independent"
      ])
