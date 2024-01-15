import os
from faessentials.constants import DEFAULT_ENCODING, ENCRYPTION_FERNET_KEY, ENCRYPTION_KEY, JWT_SECRET
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

class SecretsStore:
    def get_secret_key(self) -> str:
        """Returns the general encryption key to encrypt data."""
        #TODO Needs to be replaced by API interface, accessing the vault
        return ENCRYPTION_KEY

    def get_fernet_secret_key(self) -> bytes:
        """Returns the fernet encryption key to encrypt and decrypt data."""
        #TODO Needs to be replaced by API interface, accessing the vault
        return ENCRYPTION_FERNET_KEY

    def get_JWT_secret(self) -> str:
        """Returns JWT secret."""
        #TODO Needs to be replaced by API interface, accessing the vault
        return JWT_SECRET

class Encryption:
    def __init__(self, secretsStore: SecretsStore, default_encoding: str = DEFAULT_ENCODING) -> None:
        self.secretsStore = secretsStore
        self.default_encoding = default_encoding

    def encrypt_text(self, text: str) -> str:
        """Encrypts the provided text."""
        f = Fernet(self.secretsStore.get_fernet_secret_key())
        data = bytes(text, self.default_encoding)
        token = f.encrypt(data)
        encrypted_text = token.decode(self.default_encoding)
        return encrypted_text

    def decrypt_text_by_token(self, token: str) -> str:
        """Decrypts the provided text and returns its clear text presentation."""
        f = Fernet(self.secretsStore.get_fernet_secret_key())
        data = bytes(token, self.default_encoding)
        decrypted_text = f.decrypt(data)
        cleartext = decrypted_text.decode(self.default_encoding)
        return cleartext
