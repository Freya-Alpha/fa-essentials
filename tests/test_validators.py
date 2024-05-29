from email_validator import EmailSyntaxError
import pytest
from faessentials import validators


@pytest.mark.parametrize("email", ["john.doe@gmail.com"])
def test_email_address(email):
    try:
        validators.validate_email(email)
    except EmailSyntaxError:
        pytest.fail("validate_email() raised exception unexpectedly!")    


@pytest.mark.parametrize("email", ["name@com", "@domain.com", "name@domain",
                                   "firstname.name@DOMAIN-THAT-PROBABLY-WILL-NEVER-EXIST-TEST-0123.com"])
def test_wrong_email_addresses(email):
    with pytest.raises(Exception): 
        validators.validate_email(email)


@pytest.mark.parametrize("ip_address", ["100.128.0.0"])
def test_ip_addresses(ip_address: str):
    try:
        validators.validate_ip_address(ip_address)
    except ValueError:
        pytest.fail("validate_ip_address() raised exception unexpectedly!")      


@pytest.mark.parametrize("ip_address", [
    None,
    "127 .0.0.1",
    "999.255.255.255",
    "100.128.0.0/222"])
def test_wrong_ip_addresses(ip_address: str):
    with pytest.raises(Exception): 
        validators.validate_ip_address(ip_address)
