"""Central configuration — loads env vars and exposes constants."""

import os
from dotenv import load_dotenv

load_dotenv()

# Supabase
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SECRET_KEY = os.environ["SUPABASE_SECRET_KEY"]

# Default tenant (VWC CPAs)
DEFAULT_TENANT_ID = os.environ.get("DEFAULT_TENANT_ID", "")
DEFAULT_CAMPAIGN_ID = os.environ.get("DEFAULT_CAMPAIGN_ID", "")

# Unipile
UNIPILE_BASE_URL = os.environ["UNIPILE_BASE_URL"]
UNIPILE_API_KEY = os.environ["UNIPILE_API_KEY"]

# Microsoft Graph
MICROSOFT_CLIENT_ID = os.environ["MICROSOFT_CLIENT_ID"]
MICROSOFT_CLIENT_SECRET = os.environ["MICROSOFT_CLIENT_SECRET"]
MICROSOFT_TENANT = os.environ["MICROSOFT_TENANT"]
MICROSOFT_SENDER_EMAIL = os.environ.get("MICROSOFT_SENDER_EMAIL", "ai_team@yorcmo.com")

# Warp Oz
WARP_API_KEY = os.environ.get("WARP_API_KEY", "")
OZ_ENVIRONMENT_ID = os.environ.get("OZ_ENVIRONMENT_ID", "")

# Safety limits
MAX_DAILY_INVITES = 5
MAX_WEEKLY_INVITES = 20
INVITE_DELAY_RANGE = (45, 120)  # seconds
MESSAGE_DELAY_RANGE = (45, 120)
BUSINESS_HOURS = (8, 18)  # 8 AM - 6 PM
TIMEZONE = "America/Los_Angeles"
