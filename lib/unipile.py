"""Unipile API client with automatic activity logging to Supabase.

Bug fixes from plan-context version:
- account_id → linkedin_account_id (schema v3 column name)
- action_type → action (schema v3 column name)
- Added tenant_id (required NOT NULL in activity_log)
"""

import logging
from datetime import datetime, timedelta, timezone

import requests

import config
from db.connect import get_supabase

logger = logging.getLogger(__name__)


class UnipileClient:
    def __init__(self, tenant_id: str | None = None):
        self.base_url = config.UNIPILE_BASE_URL
        self.api_key = config.UNIPILE_API_KEY
        self.headers = {"X-API-KEY": self.api_key}
        self.sb = get_supabase()
        self.tenant_id = tenant_id or config.DEFAULT_TENANT_ID

    # -- Internal helpers --

    def _log(self, action, endpoint, linkedin_account_id=None,
             target_provider_id=None, request_summary=None,
             response_status=None, response_id=None, success=True,
             campaign_id=None, prospect_id=None):
        if not linkedin_account_id:
            return
        row = {
            "tenant_id": self.tenant_id,
            "linkedin_account_id": linkedin_account_id,
            "action": action,
            "endpoint": endpoint,
            "success": success,
        }
        if target_provider_id:
            row["target_provider_id"] = target_provider_id
        if response_status is not None:
            row["response_status"] = response_status
        if response_id:
            row["response_id"] = response_id
        if campaign_id:
            row["campaign_id"] = campaign_id
        if prospect_id:
            row["prospect_id"] = prospect_id
        try:
            self.sb.table("activity_log").insert(row).execute()
        except Exception as e:
            logger.warning("Failed to log activity: %s", e)

    def _request(self, method, path, action, linkedin_account_id=None,
                 target_provider_id=None, request_summary=None,
                 params=None, json_body=None,
                 campaign_id=None, prospect_id=None):
        url = f"{self.base_url}{path}"
        try:
            resp = requests.request(
                method, url, headers=self.headers,
                params=params, json=json_body, timeout=30,
            )
            success = resp.status_code < 400
            data = resp.json() if resp.content else {}
            response_id = None
            for key in ("id", "object_id", "invitation_id", "chat_id", "message_id"):
                if key in data:
                    response_id = str(data[key])
                    break

            self._log(
                action=action,
                endpoint=f"{method} {path}",
                linkedin_account_id=linkedin_account_id,
                target_provider_id=target_provider_id,
                response_status=resp.status_code,
                response_id=response_id,
                success=success,
                campaign_id=campaign_id,
                prospect_id=prospect_id,
            )

            if not success:
                error_detail = data.get("message") or data.get("error") or data
                raise Exception(
                    f"Unipile API error {resp.status_code} on {method} {path}: {error_detail}"
                )

            return data
        except requests.RequestException as e:
            self._log(
                action=action,
                endpoint=f"{method} {path}",
                linkedin_account_id=linkedin_account_id,
                target_provider_id=target_provider_id,
                response_status=0,
                success=False,
                campaign_id=campaign_id,
                prospect_id=prospect_id,
            )
            raise

    # -- Account --

    def get_accounts(self):
        return self._request("GET", "/api/v1/accounts", "health_check")

    # -- Profiles --

    def get_profile(self, provider_id, account_id):
        return self._request(
            "GET", f"/api/v1/users/{provider_id}",
            action="profile_view",
            linkedin_account_id=account_id,
            target_provider_id=provider_id,
            params={"account_id": account_id, "linkedin_sections": "*"},
        )

    # -- Connections --

    def get_relations(self, account_id, limit=50, cursor=None):
        params = {"account_id": account_id, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._request(
            "GET", "/api/v1/users/relations",
            action="relations_check",
            linkedin_account_id=account_id,
            params=params,
        )

    def send_invite(self, account_id, provider_id, campaign_id=None, prospect_id=None):
        """Send a BARE connection invite (no note — free account constraint)."""
        body = {"account_id": account_id, "provider_id": provider_id}
        return self._request(
            "POST", "/api/v1/users/invite",
            action="connection",
            linkedin_account_id=account_id,
            target_provider_id=provider_id,
            json_body=body,
            campaign_id=campaign_id,
            prospect_id=prospect_id,
        )

    # -- Messaging --

    def start_chat(self, account_id, provider_id, text, campaign_id=None, prospect_id=None):
        """Start a new chat (msg1 after acceptance)."""
        return self._request(
            "POST", "/api/v1/chats",
            action="message",
            linkedin_account_id=account_id,
            target_provider_id=provider_id,
            json_body={
                "account_id": account_id,
                "text": text,
                "attendees_ids": [provider_id],
            },
            campaign_id=campaign_id,
            prospect_id=prospect_id,
        )

    def send_followup(self, chat_id, text, account_id=None, campaign_id=None, prospect_id=None):
        """Send a follow-up message in an existing chat."""
        return self._request(
            "POST", f"/api/v1/chats/{chat_id}/messages",
            action="message",
            linkedin_account_id=account_id,
            json_body={"text": text},
            campaign_id=campaign_id,
            prospect_id=prospect_id,
        )

    def get_messages(self, chat_id, account_id=None):
        """Get messages in a chat (for reply detection)."""
        return self._request(
            "GET", f"/api/v1/chats/{chat_id}/messages",
            action="chat_check",
            linkedin_account_id=account_id,
        )

    # -- Usage helpers --

    def get_daily_count(self, account_id, action):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        result = (
            self.sb.table("activity_log")
            .select("id", count="exact")
            .eq("linkedin_account_id", account_id)
            .eq("action", action)
            .eq("success", True)
            .gte("created_at", f"{today}T00:00:00Z")
            .execute()
        )
        return result.count or 0

    def get_weekly_count(self, account_id, action):
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        result = (
            self.sb.table("activity_log")
            .select("id", count="exact")
            .eq("linkedin_account_id", account_id)
            .eq("action", action)
            .eq("success", True)
            .gte("created_at", week_ago)
            .execute()
        )
        return result.count or 0
