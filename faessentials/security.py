import os
import ipaddress
import uuid
import base64
import json
from builtins import bytes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
from datetime import datetime
from enum import Enum
from faessentials.constants import DEFAULT_ENCODING
from faessentials import global_logger, utils
from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator

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

class BlockedIpReasonType(str, Enum):
    ATTEMPTING_MULTIPLE_LOGINS = "Attempting multiple logins"
    EXCESSIVE_FAILED_LOGIN_ATTEMPTS = "Excessive failed login attempts"
    EXCESSIVE_REQUESTS_WITHIN_SHORT_TIME_PERIOD = "Excessive requests within short time period"
    IP_SPOOFING = "IP spoofing"
    MALWARE = "Malware"
    BLACKLISTED_IP_ADDRESS = "Blacklisted IP address"
    REPEATED_ERROR_RESPONSE_CODES = "Repeated error response codes"
    SUSPICIOUS_OPERATIONS = "Suspicious operations"
    INAPPROPRIATE_WEBSITE = "Inappropriate website"
    RULE_VIOLATION = "Rule violation"

class BlockedIp(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)

    id: str = Field(default=str(uuid.uuid4()))
    ip_address: ipaddress.IPv4Address = Field(..., strip_withspace=True)
    blocking_reason: BlockedIpReasonType = Field(...)
    timestamp: datetime = Field(default_factory=datetime.now)

    @field_validator('ip_address')
    def val_ip_address(cls, v: ipaddress.IPv4Address) -> ipaddress.IPv4Address:
        return str.strip(v.compressed)

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

class IPSecurityMiddleware:
    def __init__(self):
        self.ip_security = IPSecurity()

    async def __call__(self, request: Request):
        if self.ip_security.is_ip_blocked(request.client.host):
            raise HTTPException(status_code=403, detail="Your ip address has been blocked.")

class IPSecurity():
    def __init__(self):
        self.rc = utils.get_redis_cluster_client()
        self.db_path = "fa-tech-operations:blocked-ip"
        self.logger = global_logger.setup_custom_logger("app")

    def __get_blocked_ip_from_database(self, ip_address: str) -> str:
        blockedIP_json: str = None
        try:
            blockedIP_json = self.rc.execute_command("JSON.GET", f"{self.db_path}:{ip_address}")
        except Exception:
            self.logger.info(f"There is no blocked ip with address {ip_address} in the database.")

        return blockedIP_json

    def get_blocked_ip(self, ip_address: str) -> BlockedIp:
        """Will try to fetch a blocked ip record in the database by the provided ip address """
        blockedIp: BlockedIp = None
        blockedIP_json = self.__get_blocked_ip_from_database(ip_address)
        if blockedIP_json:
            blockedIp_dict = json.loads(blockedIP_json)
            blockedIp = BlockedIp(**blockedIp_dict)

        return blockedIp

    def is_ip_blocked(self, ip_address: str) -> bool:
        """Will try to determine if provided ip address is in the blocked ip list"""
        blockedIP_json = self.__get_blocked_ip_from_database(ip_address)
        return True if blockedIP_json is not None else False

    def block_ip(self, ip_address: str, blocking_reason: BlockedIpReasonType) -> bool:
        if self.is_ip_blocked(ip_address):
            return True

        blockedIp = BlockedIp(ip_address=ip_address, blocking_reason=blocking_reason)
        blockedIpJsonModel = blockedIp.model_dump_json()

        self.logger.debug(f"About to block ip address '{ip_address}' due to {blocking_reason}")
        response = self.rc.execute_command("JSON.SET", f"{self.db_path}:{ip_address}", ".", blockedIpJsonModel)
        return True if response == 'OK' else False

    def unblock_ip(self, ip_address) -> bool:
        self.logger.debug(f"About to unblock ip address '{ip_address}'")
        if self.is_ip_blocked(ip_address) is False:
            raise Exception("The provided ip address cannot be found in the database. Unblocking failed.")

        response = self.rc.execute_command("JSON.DEL", f"{self.db_path}:{ip_address}")
        return True if response > 0 else False
