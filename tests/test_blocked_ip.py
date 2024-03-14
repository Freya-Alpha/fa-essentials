import pytest
from faessentials.security import BlockedIp, BlockedIpReasonType

@pytest.mark.parametrize("ip_address, blocking_reason", [
    ("127.0.0.1", BlockedIpReasonType.ATTEMPTING_MULTIPLE_LOGINS),
    ("192.168.169.69", BlockedIpReasonType.BLACKLISTED_IP_ADDRESS),
    ("192.168.169.226", BlockedIpReasonType.EXCESSIVE_FAILED_LOGIN_ATTEMPTS),
    ("100.128.0.0", BlockedIpReasonType.INAPPROPRIATE_WEBSITE),
    ("101.128.0.0", BlockedIpReasonType.IP_SPOOFING),
    ("102.128.0.0", BlockedIpReasonType.MALWARE),
    ("103.128.0.0", BlockedIpReasonType.REPEATED_ERROR_RESPONSE_CODES),
    ("104.128.0.0", BlockedIpReasonType.RULE_VIOLATION)
])
def test_blocked_ip_model(ip_address: str, blocking_reason: BlockedIpReasonType):
    """Test if we can create a blocked ip model with various parameters."""
    blockedIp = BlockedIp(
        ip_address=ip_address,
        blocking_reason=blocking_reason)

    assert blockedIp.ip_address == ip_address
    assert blockedIp.blocking_reason == blocking_reason

@pytest.mark.parametrize("ip_address, blocking_reason", [
    ("127.0.0.1000", BlockedIpReasonType.ATTEMPTING_MULTIPLE_LOGINS),
    (None, BlockedIpReasonType.BLACKLISTED_IP_ADDRESS),
    ("999.255.255.255", BlockedIpReasonType.EXCESSIVE_FAILED_LOGIN_ATTEMPTS),
    ("100.128.0.0/222", BlockedIpReasonType.INAPPROPRIATE_WEBSITE),
    ("101.128.0.0", "some reason")])
def test_blocked_ip_model_for_validation_errors(ip_address: str, blocking_reason: str):
    with pytest.raises(Exception):
        BlockedIp(
            ip_address=ip_address,
            blocking_reason=blocking_reason)
