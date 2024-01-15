from faessentials import validators

def test_email_address_without_domain():
    email = "name@com"
    validator = validators.Validators()

    assert validator.validate_email(email) is False

def test_email_address_without_name():
    email = "@domain.com"
    validator = validators.Validators()

    assert validator.validate_email(email) is False

def test_email_address_without_tld():
    email = "name@domain"
    validator = validators.Validators()

    assert validator.validate_email(email) is False

def test_fake_email_address():
    email = "firstname.name@DOMAIN-THAT-PROBABLY-WILL-NEVER-EXIST-TEST-0123.com"
    validator = validators.Validators()

    assert validator.validate_email(email) is False

def test_real_email_address():
    email = "john.doe@gmail.com"
    validator = validators.Validators()

    assert validator.validate_email(email) is True