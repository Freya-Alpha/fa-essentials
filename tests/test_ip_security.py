import pytest
from faessentials.security import BlockedIpReasonType
from faessentials.security import IPSecurity

@pytest.mark.parametrize("ip_address, blocking_reason", [("155.255.152.55", BlockedIpReasonType.EXCESSIVE_FAILED_LOGIN_ATTEMPTS)])
def test_block_ip_address(ip_address: str, blocking_reason: BlockedIpReasonType):
    ipSecurity = IPSecurity()

    assert ipSecurity.block_ip(ip_address, blocking_reason) is True
    assert ipSecurity.is_ip_blocked(ip_address) is True

@pytest.mark.parametrize("ip_address, blocking_reason", [("172.255.152.55", BlockedIpReasonType.BLACKLISTED_IP_ADDRESS)])
def test_block_and_unblock_ip_address(ip_address: str, blocking_reason: BlockedIpReasonType):
    ipSecurity = IPSecurity()

    assert ipSecurity.block_ip(ip_address, blocking_reason) is True
    assert ipSecurity.is_ip_blocked(ip_address) is True
    assert ipSecurity.unblock_ip(ip_address) is True
    assert ipSecurity.is_ip_blocked(ip_address) is False

@pytest.mark.parametrize("ip_address", [("11.255.152.55")])
def test_unblock_non_blocked_ip_address(ip_address: str):
    with pytest.raises(Exception):
        IPSecurity().unblock_ip(ip_address)
