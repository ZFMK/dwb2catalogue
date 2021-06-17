import os

from setuptools import setup, find_packages


requires = [
    'pymysql',
    'pyodbc',
    'pudb',
    'configparser'
    ]

setup(name='sync_dwb_webportal',
      version='0.1',
      description='Copy data from different DiversityCollection instances into the web portal of ZFMK CollectionCatalogue',
      author='Björn Quast',
      author_email='bquast@leibniz-zfmk.de',
      install_requires=requires
      )
