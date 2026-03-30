"""Supabase client singleton — uses service role key to bypass RLS."""

import httpx
from supabase import create_client, Client, ClientOptions

import config

_client: Client | None = None


def get_supabase() -> Client:
    global _client
    if _client is None:
        # Use verify=False to work around SSL cert issues in sandbox/dev environments
        _client = create_client(
            config.SUPABASE_URL,
            config.SUPABASE_SECRET_KEY,
            options=ClientOptions(httpx_client=httpx.Client(verify=False)),
        )
    return _client
