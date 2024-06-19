import base64
import os
from builtins import bytes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from faessentials.constants import DEFAULT_ENCODING


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


def get_AES_secret() -> bytes:
    """Returns AES secret."""
    aes_secret = bytes(os.getenv("AES_SECRET"), DEFAULT_ENCODING)
    if not aes_secret:
        raise ValueError("AES_SECRET environment variable is not set or empty.")
    return aes_secret


class Crypto:
    def __init__(self):
        self.backend = default_backend()
        self.key = base64.urlsafe_b64encode(get_AES_secret())[:32]
        self.encryptor = Cipher(algorithms.AES(self.key), modes.ECB(), self.backend).encryptor()
        self.decryptor = Cipher(algorithms.AES(self.key), modes.ECB(), self.backend).decryptor()

    def encrypt(self, value: str) -> bytes:
        byte_value = bytes(value, DEFAULT_ENCODING)
        padder = padding.PKCS7(algorithms.AES(self.key).block_size).padder()
        padded_data = padder.update(byte_value) + padder.finalize()
        encrypted_text = self.encryptor.update(padded_data) + self.encryptor.finalize()
        return base64.urlsafe_b64encode(encrypted_text)

    def encrypt_as_text(self, value) -> str:
        return str(self.encrypt(value), encoding=DEFAULT_ENCODING)

    def decrypt(self, value: str) -> bytes:
        byte_value = base64.urlsafe_b64decode(bytes(value, DEFAULT_ENCODING))
        padder = padding.PKCS7(algorithms.AES(self.key).block_size).unpadder()
        decrypted_data = self.decryptor.update(byte_value)
        unpadded = padder.update(decrypted_data) + padder.finalize()
        return unpadded

    def decrypt_as_text(self, value) -> str:
        return str(self.decrypt(value), encoding=DEFAULT_ENCODING)
