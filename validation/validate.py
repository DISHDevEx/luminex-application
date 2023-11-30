from input_validator import InputValidator
# from permissions_validator import PermissionsValidator

if __name__ == "__main__":
    # Create an instance of the InputValidator class
    input_validator = InputValidator('config.json')
    
    # Run the validation
    input_validator.run_validation()