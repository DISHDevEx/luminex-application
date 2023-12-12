# luminex_application
Luminex Application Code

~~~
luminex_application-repo/
|-- .github
|   |-- workflows
|   |   |-- create-emr-cft.yml
|   |   |-- create-emr.yml
|   |   |-- pytest-pr-check.yaml
|-- luminex/
|   |-- data-standardization
|   |   |-- __init__.py
|   |   |-- read_s3_data.py
|   |-- validation/
|   |   |-- __init__.py
|   |   |-- config.json
|   |   |-- input_validator.py
|   |   |-- stack_name_validator.py
|   |   |-- rolename_permissions_validator.py
|   |   |-- transformation_file_validation.py
|   |   |-- config.json
|   |   |-- run_all_validations.py
|   |-- __init__.py
|   |-- run_infra.py
|   |-- run_etl.py
|   |-- kill_infra.py
|-- infrastructure/
|   |-- create-emr-cft.yaml
|   |-- config_parameter.json
|-- config/
|   |-- infra_config.yaml
|   |-- aws_config.py
|-- tests/
|   |-- __init__.py
|   |-- conftest.py
|   |-- infra_tests.py
|   |-- test_add.py
|   |-- test_s3_data_loader.py
|-- README.md
|-- requirements.txt
|-- setup.py
~~~
