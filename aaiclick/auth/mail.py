"""Outbound mail for the password-reset flow. ``smtplib`` on a worker thread
so the async request path never blocks on the SMTP conversation."""

from __future__ import annotations

import asyncio
import smtplib
from email.message import EmailMessage

from .config import SmtpSettings


def send_mail_sync(settings: SmtpSettings, *, to: str, subject: str, body: str) -> None:
    message = EmailMessage()
    message["From"] = settings.sender
    message["To"] = to
    message["Subject"] = subject
    message.set_content(body)
    with smtplib.SMTP(settings.host, settings.port, timeout=15) as smtp:
        if settings.starttls:
            smtp.starttls()
        if settings.username and settings.password:
            smtp.login(settings.username, settings.password)
        smtp.send_message(message)


async def send_mail(settings: SmtpSettings, *, to: str, subject: str, body: str) -> None:
    await asyncio.to_thread(send_mail_sync, settings, to=to, subject=subject, body=body)
