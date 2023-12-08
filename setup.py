"""
Build the wheel file for the library eks-ml-pipeline.
"""

from setuptools import find_packages, setup

setup(
    name='luminex',
    version='0.0.1',
    description='On the Fly ETL application',
    url='',
    license='Dish Wireless',
    packages=find_packages(include=['luminex',
                                    'luminex.data_standardization'
                                    ]),
    include_package_data=True,
    install_requires = [
        'pandas',
        'matplotlib',
        'numpy',
        'scikit_learn',
        'statsmodels'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Dish Wireless',
        ],
    )