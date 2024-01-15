from email_validator import validate_email, EmailNotValidError

class Validators:
    def validate_email(self, email: str) -> bool:
        try:
            validate_email(email, check_deliverability=True,)
            return True

        except EmailNotValidError:
            return False
