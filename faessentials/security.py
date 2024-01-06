import os

def get_secret_key() -> str:
    """Returns the general encryption key to encrypt data."""
    key = os.getenv("ENCRYPTION_KEY")
    if not key:
        raise ValueError("ENCRYPTION_KEY environment variable is not set or empty.")
    return key

def get_JWT_secret() -> str:
    """Returns JWT secret."""
    jwt_secret = os.getenv("JWT_SECRET")
    if not jwt_secret:
        raise ValueError("JWT_SECRET environment variable is not set or empty.")
    return jwt_secret
