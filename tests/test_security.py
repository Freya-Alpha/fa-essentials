import pytest
from faessentials.security import Encryption, SecretsStore

@pytest.fixture
def encryption():
    return Encryption(SecretsStore())

@pytest.mark.parametrize("message", ["test", "123908234ÖÄ$ASDFdd", "äjklöèü"])
def test_encrypt_and_decrypt_text_message(encryption, message):
    encrypted_message = encryption.encrypt_text(message)
    decrypted_message = encryption.decrypt_text_by_token(encrypted_message)
  
    assert decrypted_message == message

def test_encrypt_and_decrypt_email_address(encryption):
    message = "firstname.name@domain.com"

    encrypted_message = encryption.encrypt_text(message)
    decrypted_message = encryption.decrypt_text_by_token(encrypted_message)

    assert decrypted_message == message

