"""Shared utilities for all skills — business hours, delays, event logging, rate limits."""

import logging
import random
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import config
from db.connect import get_supabase

logger = logging.getLogger(__name__)


def is_business_hours() -> bool:
    """Check if current time is within business hours (8-18 PT, weekdays)."""
    now = datetime.now(ZoneInfo(config.TIMEZONE))
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    return config.BUSINESS_HOURS[0] <= now.hour < config.BUSINESS_HOURS[1]


def random_delay(delay_range: tuple[int, int] = config.INVITE_DELAY_RANGE):
    """Sleep for a random duration within the given range (seconds)."""
    seconds = random.uniform(delay_range[0], delay_range[1])
    logger.info("Waiting %.0f seconds...", seconds)
    time.sleep(seconds)


def log_event(
    tenant_id: str,
    event_type: str,
    actor: str,
    data: dict | None = None,
    campaign_id: str | None = None,
    prospect_id: str | None = None,
):
    """Insert a row into the events table."""
    sb = get_supabase()
    row = {
        "tenant_id": tenant_id,
        "event_type": event_type,
        "actor": actor,
        "data": data or {},
    }
    if campaign_id:
        row["campaign_id"] = campaign_id
    if prospect_id:
        row["prospect_id"] = prospect_id
    try:
        sb.table("events").insert(row).execute()
    except Exception as e:
        logger.error("Failed to log event %s: %s", event_type, e)


def check_rate_limit(account_id: str, action: str) -> bool:
    """Call the DB function check_rate_limit() — returns True if action is allowed."""
    sb = get_supabase()
    result = sb.rpc("check_rate_limit", {
        "p_account_id": account_id,
        "p_action": action,
    }).execute()
    return bool(result.data)


def get_effective_limit(account_id: str, action: str) -> int | None:
    """Call the DB function get_effective_limit() — returns daily limit or None."""
    sb = get_supabase()
    result = sb.rpc("get_effective_limit", {
        "p_account_id": account_id,
        "p_action": action,
    }).execute()
    return result.data


def get_active_accounts(tenant_id: str | None = None) -> list[dict]:
    """Get all active LinkedIn accounts for a tenant."""
    sb = get_supabase()
    query = sb.table("linkedin_accounts").select("*").eq("is_active", True).eq("status", "ok")
    if tenant_id:
        query = query.eq("tenant_id", tenant_id)
    result = query.execute()
    return result.data or []


def setup_logging():
    """Configure logging for skill scripts."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
