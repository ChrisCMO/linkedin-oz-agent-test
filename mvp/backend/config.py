"""Shared config: .env loading, singletons."""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root
project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(project_root / ".env")
sys.path.insert(0, str(project_root))

from db.connect import get_supabase  # noqa: E402
from lib.unipile import UnipileClient  # noqa: E402
from lib.apollo import ApolloClient  # noqa: E402
from lib.outlook import OutlookClient  # noqa: E402

# ---------------------------------------------------------------------------
# Singletons
# ---------------------------------------------------------------------------

_unipile = None
_supabase = None
_apollo = None
_openai = None
_outlook = None


def get_unipile() -> UnipileClient:
    global _unipile
    if _unipile is None:
        _unipile = UnipileClient()
    return _unipile


def get_sb():
    global _supabase
    if _supabase is None:
        _supabase = get_supabase()
    return _supabase


def get_apollo() -> ApolloClient:
    global _apollo
    if _apollo is None:
        _apollo = ApolloClient()
    return _apollo


def get_openai():
    global _openai
    if _openai is None:
        import httpx
        from openai import OpenAI
        # Use the agent CA bundle if present (Warp sandbox has a non-standard cert path)
        _AGENT_CA = "/agent/etc/ssl/certs/ca-certificates.crt"
        ca_bundle = _AGENT_CA if os.path.exists(_AGENT_CA) else True
        _openai = OpenAI(http_client=httpx.Client(verify=ca_bundle))  # uses OPENAI_API_KEY env var
    return _openai


def get_outlook() -> OutlookClient:
    global _outlook
    if _outlook is None:
        _outlook = OutlookClient()
    return _outlook


DEFAULT_ACCOUNT_IDS = {
    "george": os.environ.get("UNIPILE_ACCOUNT_ID_GEORGE", ""),
    "laikah": os.environ.get("UNIPILE_ACCOUNT_ID_LAIKAH", ""),
}
