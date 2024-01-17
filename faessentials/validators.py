import ipaddress
from email_validator import validate_email, EmailNotValidError

class Validators:
    def validate_email(self, email: str) -> bool:
        try:
            validate_email(email, check_deliverability=True,)
            return True

        except EmailNotValidError:
            return False

    def validate_ip_address(self, ip_string: str) -> bool:
        try:
            ipaddress.ip_address(ip_string)
            return True
       
        except ValueError:
            return False
