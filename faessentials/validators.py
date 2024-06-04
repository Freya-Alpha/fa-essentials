import ipaddress
import email_validator


def validate_email(email: str):
    email_validator.validate_email(email, check_deliverability=True)


def validate_ip_address(ip_string: str):
    ipaddress.ip_address(ip_string)
