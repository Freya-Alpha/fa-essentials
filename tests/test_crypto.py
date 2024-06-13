import pytest
from faessentials.security import Crypto

AES_SECRET = "ZufDdKmoYgBv272G0DQWqz8Ng9ewM+IMGIMzkRQUoVNujiCHfdD4EULwXtn5fvwL"


@pytest.mark.parametrize("message", ["test", "123908234ÖÄ$ASDFdd", "äjklöèü"])
def test_encrypt_and_decrypt_text_message(monkeypatch, message):
    monkeypatch.setenv("AES_SECRET", AES_SECRET)
    crypto = Crypto()

    encrypted_message = crypto.encrypt_as_text(message)
    decrypted_message = crypto.decrypt_as_text(encrypted_message)

    assert decrypted_message == message


@pytest.mark.parametrize("message", ["firstname.name@domain.com"])
def test_encrypt_and_decrypt_email_address(monkeypatch, message):
    monkeypatch.setenv("AES_SECRET", AES_SECRET)
    crypto = Crypto()

    encrypted_message = crypto.encrypt_as_text(message)
    decrypted_message = crypto.decrypt_as_text(encrypted_message)

    assert decrypted_message == message
