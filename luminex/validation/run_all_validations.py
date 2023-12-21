from input_validator import InputValidator
from validate_multiple_files import ETLFileValidator
from rolename_permissions_validator import IAMRoleValidator

if __name__ == "__main__":
    # Create an instance of the InputValidator class
    input_validator = InputValidator('config.json')
    # Run the Input validation
    input_validator.run_validation()
    
    # Create an instance of the ETLFileValidator class 
    etl_validator = ETLFileValidator('config.json')
    # Run the ETL logic validation
    etl_validator.validate_files()

    # Create an instance of IAMRoleValidator and run the Permissions validator
    permissions_validator = IAMRoleValidator('config.json')
    permissions_validator.validate_roles()
