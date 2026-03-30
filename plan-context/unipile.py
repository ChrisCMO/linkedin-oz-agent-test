"""Unipile API client with automatic activity logging to Supabase."""

import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

from db.connect import get_supabase

load_dotenv()


class UnipileClient:
    def __init__(self):
        self.base_url = os.environ["UNIPILE_BASE_URL"]
        self.api_key = os.environ["UNIPILE_API_KEY"]
        self.headers = {"X-API-KEY": self.api_key}
        self.sb = get_supabase()

    # -- Internal helpers --

    def _log(self, action_type, endpoint, account_id=None,
             target_provider_id=None, request_summary=None,
             response_status=None, response_id=None, success=True):
        if not account_id:
            return  # Skip logging for calls without an account context
        row = {
            "account_id": account_id,
            "action_type": action_type,
            "endpoint": endpoint,
            "success": success,
        }
        if target_provider_id:
            row["target_provider_id"] = target_provider_id
        if request_summary:
            row["request_summary"] = request_summary
        if response_status is not None:
            row["response_status"] = response_status
        if response_id:
            row["response_id"] = response_id
        try:
            self.sb.table("activity_log").insert(row).execute()
        except Exception as e:
            print(f"[UnipileClient] Failed to log activity: {e}")

    def _request(self, method, path, action_type, account_id=None,
                 target_provider_id=None, request_summary=None,
                 params=None, json_body=None):
        url = f"{self.base_url}{path}"
        try:
            resp = requests.request(
                method, url, headers=self.headers,
                params=params, json=json_body, timeout=30
            )
            success = resp.status_code < 400
            data = resp.json() if resp.content else {}
            response_id = None
            for key in ("id", "object_id", "invitation_id", "chat_id", "message_id"):
                if key in data:
                    response_id = str(data[key])
                    break

            self._log(
                action_type=action_type,
                endpoint=f"{method} {path}",
                account_id=account_id,
                target_provider_id=target_provider_id,
                request_summary=request_summary,
                response_status=resp.status_code,
                response_id=response_id,
                success=success,
            )

            if not success:
                error_detail = data.get("message") or data.get("error") or data if isinstance(data, dict) else data
                raise Exception(
                    f"Unipile API error {resp.status_code} on {method} {path}: {error_detail}"
                )

            return data
        except requests.RequestException as e:
            self._log(
                action_type=action_type,
                endpoint=f"{method} {path}",
                account_id=account_id,
                target_provider_id=target_provider_id,
                request_summary=request_summary,
                response_status=0,
                success=False,
            )
            raise

    # -- Account --

    def get_accounts(self):
        return self._request("GET", "/api/v1/accounts", "ACCOUNT_CHECK")

    # -- Search --

    def search_people(self, account_id, keywords, location_ids=None):
        body = {"api": "classic", "type": "people", "keywords": keywords}
        if location_ids:
            body["location"] = location_ids
        return self._request(
            "POST", "/api/v1/linkedin/search",
            action_type="SEARCH",
            account_id=account_id,
            request_summary=f"keywords={keywords}",
            params={"account_id": account_id},
            json_body=body,
        )

    def get_search_params(self, param_type, keyword, account_id=None):
        return self._request(
            "GET", "/api/v1/linkedin/search/parameters",
            action_type="SEARCH_PARAMS",
            account_id=account_id,
            request_summary=f"type={param_type} keyword={keyword}",
            params={"type": param_type, "keyword": keyword},
        )

    # -- Profiles --

    def get_profile(self, provider_id, account_id):
        return self._request(
            "GET", f"/api/v1/users/{provider_id}",
            action_type="PROFILE_LOOKUP",
            account_id=account_id,
            target_provider_id=provider_id,
            params={"account_id": account_id, "linkedin_sections": "*"},
        )

    def get_company(self, company_id, account_id):
        return self._request(
            "GET", f"/api/v1/linkedin/company/{company_id}",
            action_type="COMPANY_LOOKUP",
            account_id=account_id,
            target_provider_id=company_id,
            params={"account_id": account_id},
        )

    # -- Connections --

    def get_relations(self, account_id, limit=50, cursor=None):
        params = {"account_id": account_id, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._request(
            "GET", "/api/v1/users/relations",
            action_type="RELATIONS",
            account_id=account_id,
            params=params,
        )

    def send_invite(self, account_id, provider_id, note=None):
        body = {"account_id": account_id, "provider_id": provider_id}
        if note:
            body["message"] = note
        summary = f"note={note[:50]}..." if note and len(note) > 50 else f"note={note}"
        return self._request(
            "POST", "/api/v1/users/invite",
            action_type="INVITE",
            account_id=account_id,
            target_provider_id=provider_id,
            request_summary=summary,
            json_body=body,
        )

    # -- Messaging --

    def send_message(self, account_id, provider_id, text):
        return self._request(
            "POST", "/api/v1/chats",
            action_type="MESSAGE_NEW",
            account_id=account_id,
            target_provider_id=provider_id,
            request_summary=f"text={text[:50]}..." if len(text) > 50 else f"text={text}",
            json_body={
                "account_id": account_id,
                "text": text,
                "attendees_ids": [provider_id],
            },
        )

    def send_followup(self, chat_id, text):
        return self._request(
            "POST", f"/api/v1/chats/{chat_id}/messages",
            action_type="MESSAGE_FOLLOWUP",
            request_summary=f"chat={chat_id} text={text[:50]}..." if len(text) > 50 else f"chat={chat_id} text={text}",
            json_body={"text": text},
        )

    def get_messages(self, chat_id):
        return self._request(
            "GET", f"/api/v1/chats/{chat_id}/messages",
            action_type="CHECK_MESSAGES",
            request_summary=f"chat={chat_id}",
        )

    def get_chats(self, account_id):
        return self._request(
            "GET", "/api/v1/chats",
            action_type="LIST_CHATS",
            account_id=account_id,
            params={"account_id": account_id},
        )

    # -- Usage helpers --

    def get_daily_count(self, account_id, action_type):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        result = (
            self.sb.table("activity_log")
            .select("id", count="exact")
            .eq("account_id", account_id)
            .eq("action_type", action_type)
            .eq("success", True)
            .gte("created_at", f"{today}T00:00:00Z")
            .execute()
        )
        return result.count or 0

    def get_weekly_count(self, account_id, action_type):
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        result = (
            self.sb.table("activity_log")
            .select("id", count="exact")
            .eq("account_id", account_id)
            .eq("action_type", action_type)
            .eq("success", True)
            .gte("created_at", week_ago)
            .execute()
        )
        return result.count or 0

    def get_usage_summary(self, account_id):
        action_types = [
            "ACCOUNT_CHECK", "SEARCH", "SEARCH_PARAMS", "PROFILE_LOOKUP",
            "COMPANY_LOOKUP", "INVITE", "RELATIONS", "MESSAGE_NEW",
            "MESSAGE_FOLLOWUP", "CHECK_MESSAGES", "LIST_CHATS",
        ]
        summary = {}
        for at in action_types:
            summary[at] = {
                "daily": self.get_daily_count(account_id, at),
                "weekly": self.get_weekly_count(account_id, at),
            }
        return summary
