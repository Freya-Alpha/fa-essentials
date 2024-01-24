import pytest
from faessentials import security


FERNET_KEY = '2L-dPkzvR7uNR-cAW1TuSGygXZy8Wb5Zk8WAFcMG4ng='

@pytest.mark.parametrize("message", ["test", "123908234ÖÄ$ASDFdd", "äjklöèü"])
def test_encrypt_and_decrypt_text_message(monkeypatch, message):
    monkeypatch.setenv("ENCRYPTION_FERNET_KEY", FERNET_KEY)

    encrypted_message = security.encrypt_text(message)
    decrypted_message = security.decrypt_text_by_token(encrypted_message)
  
    assert decrypted_message == message

@pytest.mark.parametrize("message", ["firstname.name@domain.com"])
def test_encrypt_and_decrypt_email_address(monkeypatch, message):
    monkeypatch.setenv("ENCRYPTION_FERNET_KEY", FERNET_KEY)

    encrypted_message = security.encrypt_text(message)
    decrypted_message = security.decrypt_text_by_token(encrypted_message)

    assert decrypted_message == message

