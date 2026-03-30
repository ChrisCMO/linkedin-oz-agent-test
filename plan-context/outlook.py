"""Microsoft Graph API client for sending emails via Outlook."""

import os
import logging

import msal
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class OutlookClient:
    def __init__(self):
        self.client_id = os.environ["MICROSOFT_CLIENT_ID"]
        self.client_secret = os.environ["MICROSOFT_CLIENT_SECRET"]
        self.tenant_id = os.environ["MICROSOFT_TENANT"]
        self.sender_email = os.environ.get(
            "MICROSOFT_SENDER_EMAIL", "ai_team@yorcmo.com"
        )
        self.app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )

    def _get_token(self) -> str:
        result = self.app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )
        if "access_token" not in result:
            desc = result.get("error_description", "Unknown error")
            raise Exception(f"Token acquisition failed: {desc}")
        return result["access_token"]

    def send_email(
        self,
        to: str,
        subject: str,
        html_body: str,
        cc: str | None = None,
    ) -> dict:
        """Send an email via Microsoft Graph API.

        Requires Mail.Send application permission with admin consent.
        """
        token = self._get_token()
        url = (
            f"https://graph.microsoft.com/v1.0/users/{self.sender_email}/sendMail"
        )

        to_recipients = [{"emailAddress": {"address": to}}]
        cc_recipients = []
        if cc:
            cc_recipients = [{"emailAddress": {"address": cc}}]

        payload = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": html_body,
                },
                "toRecipients": to_recipients,
                "ccRecipients": cc_recipients,
            }
        }

        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )

        if resp.status_code == 202:
            logger.info("Email sent to %s: %s", to, subject)
            return {"success": True, "status_code": 202}

        error_body = resp.text
        logger.error(
            "Failed to send email (HTTP %d): %s", resp.status_code, error_body
        )
        raise Exception(
            f"Graph API error {resp.status_code}: {error_body}"
        )

    def test_connection(self) -> dict:
        """Verify auth works by fetching sender user profile."""
        token = self._get_token()
        url = f"https://graph.microsoft.com/v1.0/users/{self.sender_email}"
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            return {
                "success": True,
                "display_name": data.get("displayName"),
                "mail": data.get("mail"),
                "id": data.get("id"),
            }
        raise Exception(
            f"Graph API test failed (HTTP {resp.status_code}): {resp.text}"
        )
