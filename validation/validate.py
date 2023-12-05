from input_validator import InputValidator
from transformation_file_validation import ETLFileValidator
from permissions_validator import IAMRoleValidator
# from permissions_validator import PermissionsValidator

if __name__ == "__main__":
    # Create an instance of the InputValidator class
    input_validator = InputValidator('config.json')
    
    # Run the validation
    input_validator.run_validation()
    
    # Create an instance of the ETLFileValidator class 
    etl_validator = ETLFileValidator('config.json')
    
    # Run the ETL logic validation
    etl_validator.validate_logic_file()

    # Create an instance of IAMRoleValidator and run the permissions validator
    permissions_validator = IAMRoleValidator('config.json')
    permissions_validator.validate_roles()
