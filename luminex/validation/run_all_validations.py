import sys
import subprocess
from input_validator import InputValidator
from validate_multiple_files import ETLFileValidator
from rolename_permissions_validator import IAMRoleValidator

# get repo root level
root_path = subprocess.run(
    ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False
).stdout.rstrip("\n")
# add repo path to use all libraries
sys.path.append(root_path)

from configs import Config

cfg = Config('../configs/config.yaml')

if __name__ == "__main__":
    # Create an instance of the InputValidator class
    input_validator = InputValidator(cfg)
    # Run the Input validation
    input_validator.run_validation()
    
    # Create an instance of the ETLFileValidator class 
    etl_validator = ETLFileValidator(cfg)
    # Run the ETL logic validation
    etl_validator.validate_files()

    # Create an instance of IAMRoleValidator and run the Permissions validator
    permissions_validator = IAMRoleValidator(cfg)
    permissions_validator.validate_roles()
