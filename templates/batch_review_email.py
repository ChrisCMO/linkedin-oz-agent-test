"""HTML email template for batch review — sent to client with prospect cards + Approve button."""


def build_batch_review_html(
    batch_id: str,
    token: str,
    recipient_name: str,
    prospects: list[dict],
    supabase_url: str,
) -> str:
    """Build the batch review email HTML.

    Args:
        batch_id: UUID of the batch_review record
        token: Raw token for the magic link (NOT the hash)
        recipient_name: Name of the client receiving the email
        prospects: List of prospect dicts from Supabase
        supabase_url: Supabase project URL for the Edge Function link
    """
    approve_url = f"{supabase_url}/functions/v1/approve-batch?batch_id={batch_id}&token={token}"
    prospect_cards = _build_prospect_cards(prospects)
    count = len(prospects)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Prospect Review — Batch #{batch_id[:8]}</title>
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f0f4f8;">
  <div style="max-width: 680px; margin: 0 auto; background: #ffffff;">

    <!-- Header -->
    <div style="background: #1a365d; color: white; padding: 24px 32px;">
      <h1 style="margin: 0; font-size: 20px; font-weight: 600;">LinkedIn Prospect Review</h1>
      <p style="margin: 6px 0 0; font-size: 14px; color: #a0aec0;">
        {count} prospects ready for your approval, {recipient_name}
      </p>
    </div>

    <!-- Intro -->
    <div style="padding: 20px 32px; border-bottom: 1px solid #e2e8f0;">
      <p style="margin: 0; font-size: 15px; color: #4a5568; line-height: 1.6;">
        Below are the prospects we'd like to connect with on your behalf.
        Review the list and click <strong>Approve</strong> when ready.
      </p>
    </div>

    <!-- Prospect Cards -->
    {prospect_cards}

    <!-- Approve Button -->
    <div style="padding: 32px; text-align: center;">
      <a href="{approve_url}"
         style="display: inline-block; background: #38a169; color: white; padding: 14px 40px;
                border-radius: 8px; text-decoration: none; font-size: 16px; font-weight: 600;
                letter-spacing: 0.3px;">
        Approve &amp; Start Connecting
      </a>
      <p style="margin: 16px 0 0; font-size: 13px; color: #a0aec0;">
        This will send bare connection requests (no notes) — about 5 per day during business hours.
      </p>
    </div>

    <!-- Hold Instructions -->
    <div style="padding: 16px 32px; background: #fffbeb; border-top: 1px solid #fefcbf;">
      <p style="margin: 0; font-size: 13px; color: #975a16;">
        <strong>Want to hold off?</strong> Simply don't click Approve. Reply to this email
        or contact yorCMO if you want to make changes. This link expires in 14 days.
      </p>
    </div>

    <!-- Footer -->
    <div style="padding: 16px 32px; background: #f7fafc; border-top: 1px solid #e2e8f0;">
      <p style="margin: 0; font-size: 11px; color: #a0aec0;">
        VWC LinkedIn Outreach System — yorCMO
      </p>
    </div>

  </div>
</body>
</html>"""


def _build_prospect_cards(prospects: list[dict]) -> str:
    """Build HTML cards for each prospect."""
    cards = ""
    for p in prospects:
        first = p.get("first_name", "")
        last = p.get("last_name", "")
        title = p.get("title", "")
        company = p.get("company_name", "")
        location = p.get("location", "")
        linkedin_url = p.get("linkedin_url", "")
        icp_score = p.get("icp_score")
        industry = (p.get("raw_apollo_data") or {}).get("industry", "")

        # Score badge
        score_html = ""
        if icp_score is not None:
            color = "#38a169" if icp_score >= 70 else "#d69e2e" if icp_score >= 40 else "#e53e3e"
            score_html = f'<span style="background: {color}; color: white; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; margin-left: 8px;">{icp_score}</span>'

        # LinkedIn link
        li_html = ""
        if linkedin_url:
            li_html = f'<a href="{linkedin_url}" style="color: #3182ce; font-size: 12px; text-decoration: none;">View Profile</a>'

        cards += f"""
    <div style="padding: 16px 32px; border-bottom: 1px solid #e2e8f0;">
      <div style="display: flex; justify-content: space-between; align-items: center;">
        <div>
          <div style="font-size: 15px; font-weight: 600; color: #2d3748;">
            {first} {last}{score_html}
          </div>
          <div style="font-size: 13px; color: #4a5568; margin-top: 2px;">
            {title}{' at ' + company if company else ''}
          </div>
          <div style="font-size: 12px; color: #a0aec0; margin-top: 2px;">
            {location}{' · ' + industry if industry else ''}
          </div>
        </div>
        <div>{li_html}</div>
      </div>
    </div>"""

    return cards
