import os
from faessentials.constants import DEFAULT_ENCODING
from cryptography.fernet import Fernet

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

def get_fernet_secret_key() -> bytes:
    """Returns JWT secret."""
    fernet_secret = bytes(os.getenv("ENCRYPTION_FERNET_KEY"), DEFAULT_ENCODING)
    if not fernet_secret:
        raise ValueError("ENCRYPTION_FERNET_KEY environment variable is not set or empty.")
    return fernet_secret

def encrypt_text(text: str) -> str:
    """Encrypts the provided text."""
    f = Fernet(get_fernet_secret_key())
    data = bytes(text, DEFAULT_ENCODING)
    token = f.encrypt(data)
    encrypted_text = token.decode(DEFAULT_ENCODING)
    return encrypted_text

def decrypt_text_by_token(token: str) -> str:
    """Decrypts the provided text and returns its clear representation."""
    f = Fernet(get_fernet_secret_key())
    data = bytes(token, DEFAULT_ENCODING)
    decrypted_text = f.decrypt(data)
    clear_text = decrypted_text.decode(DEFAULT_ENCODING)
    return clear_text
