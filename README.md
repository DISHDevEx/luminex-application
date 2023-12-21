# luminex_application
Luminex Application Code

~~~
luminex_application-repo/
|-- .github
|   |-- workflows
|   |   |-- create-emr-cft.yml
|   |   |-- create-emr.yml
|   |   |-- pytest-pr-check.yaml
|-- config/
|   |-- __init__.py
|   |-- infra_config.json
|   |-- aws_config.py
|-- infrastructure/
|   |-- create-emr-cft.yaml
|   |-- config_parameter.json
|-- luminex/
|   |-- data-standardization
|   |   |-- __init__.py
|   |   |-- s3_data_loader_spark.py
|   |   |-- s3_data_loader.py
|   |   |-- s3_json_uploader.py
|   |-- validation/
|   |   |-- __init__.py
|   |   |-- config.json
|   |   |-- input_validator.py
|   |   |-- stack_name_validator.py
|   |   |-- rolename_permissions_validator.py
|   |   |-- validate_multiple_files.py
|   |   |-- run_all_validations.py
|   |-- __init__.py
|   |-- run_infra.py
|   |-- run_etl.py
|   |-- kill_infra.py
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
