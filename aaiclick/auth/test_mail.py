from unittest.mock import MagicMock, patch

from aaiclick.auth import config, mail


def test_send_mail_sync_drives_smtp(monkeypatch):
    settings = config.SmtpSettings(
        host="smtp.example.com", port=587, username="u", password="p", sender="noreply@example.com", starttls=True
    )
    smtp = MagicMock()
    with patch.object(mail.smtplib, "SMTP") as ctor:
        ctor.return_value.__enter__.return_value = smtp
        mail.send_mail_sync(settings, to="a@example.com", subject="hi", body="body")
    ctor.assert_called_once_with("smtp.example.com", 587, timeout=15)
    smtp.starttls.assert_called_once()
    smtp.login.assert_called_once_with("u", "p")
    message = smtp.send_message.call_args.args[0]
    assert message["To"] == "a@example.com" and message["From"] == "noreply@example.com"


def test_smtp_settings(monkeypatch):
    monkeypatch.delenv("AAICLICK_SMTP_HOST", raising=False)
    assert config.smtp_settings() is None
    monkeypatch.setenv("AAICLICK_SMTP_HOST", "mail")
    monkeypatch.setenv("AAICLICK_SMTP_USERNAME", "user@example.com")
    monkeypatch.delenv("AAICLICK_SMTP_FROM", raising=False)
    monkeypatch.setenv("AAICLICK_SMTP_STARTTLS", "0")
    settings = config.smtp_settings()
    assert settings is not None and settings.sender == "user@example.com" and settings.starttls is False
    assert settings.port == 587
