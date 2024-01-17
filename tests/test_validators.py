import pytest
from faessentials import validators

@pytest.fixture
def validator():
    return validators.Validators()

@pytest.mark.parametrize("email", ["name@com", "@domain.com", "name@domain", "firstname.name@DOMAIN-THAT-PROBABLY-WILL-NEVER-EXIST-TEST-0123.com"])
def test_wrong_email_addresses(validator, email):
    assert validator.validate_email(email) is False

@pytest.mark.parametrize("email", ["john.doe@gmail.com"])
def test_real_email_address(validator, email):
    assert validator.validate_email(email) is True
