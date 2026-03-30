"""Email notification service — sends HITL approval emails on connection acceptance."""

import logging
from datetime import datetime, timezone

from mvp.backend.config import get_sb, get_outlook

logger = logging.getLogger(__name__)

DEFAULT_RECIPIENT = "christopher@yorcmo.com"


def send_acceptance_notification(prospect_id: str) -> dict:
    """Send an email notification when a connection request is accepted.

    1. Loads prospect + company data
    2. Generates AI messages (or uses existing)
    3. Builds HTML email
    4. Sends via Microsoft Graph
    5. Logs to email_notifications table
    """
    sb = get_sb()
    outlook = get_outlook()

    # Load prospect with company
    p_result = sb.table("prospects").select(
        "*, prospect_companies(*)"
    ).eq("id", prospect_id).single().execute()
    prospect = p_result.data

    company = prospect.get("prospect_companies") or {}

    # Find the invitation to get campaign_id and chat_id
    inv_result = sb.table("invitations").select("*").eq(
        "prospect_id", prospect_id
    ).order("created_at", desc=True).limit(1).execute()

    campaign_id = None
    chat_id = None
    linkedin_account_id = None
    if inv_result.data:
        inv = inv_result.data[0]
        campaign_id = inv.get("campaign_id")
        chat_id = inv.get("chat_id")
        linkedin_account_id = inv.get("linkedin_account_id")

    # Generate messages
    from mvp.backend.services.message_gen_svc import generate_and_store_messages

    messages = []
    try:
        messages = generate_and_store_messages(
            prospect_id, campaign_id, chat_id,
            linkedin_account_id=linkedin_account_id,
        )
    except Exception as e:
        logger.error("Message generation failed for %s: %s", prospect_id, e)
        messages = [
            {"step": 1, "text": "(Message generation failed — please write manually)"},
            {"step": 2, "text": "(Message generation failed)"},
            {"step": 3, "text": "(Message generation failed)"},
        ]

    # Build email
    first_name = prospect.get("first_name", "")
    last_name = prospect.get("last_name", "")
    title = prospect.get("title", "")
    company_name = prospect.get("company_name", company.get("name", ""))

    subject = f"VWC Approval: Messaging to {first_name} {last_name}"
    if title and company_name:
        subject += f", {title} at {company_name}"

    html_body = _build_acceptance_html(prospect, company, messages)

    # Determine tenant_id for logging
    tenant_id = None
    if campaign_id:
        c_result = sb.table("campaigns").select("tenant_id").eq(
            "id", campaign_id
        ).limit(1).execute()
        if c_result.data:
            tenant_id = c_result.data[0]["tenant_id"]

    # Send
    try:
        outlook.send_email(to=DEFAULT_RECIPIENT, subject=subject, html_body=html_body)
        status = "sent"
        error_message = None
    except Exception as e:
        status = "failed"
        error_message = str(e)
        logger.error("Email send failed: %s", e)

    # Log notification
    notification_row = {
        "prospect_id": prospect_id,
        "recipient_email": DEFAULT_RECIPIENT,
        "subject": subject,
        "html_body": html_body,
        "notification_type": "acceptance",
        "status": status,
        "error_message": error_message,
    }
    if tenant_id:
        notification_row["tenant_id"] = tenant_id
    if campaign_id:
        notification_row["campaign_id"] = campaign_id

    try:
        n_result = sb.table("email_notifications").insert(
            notification_row
        ).execute()
        notification_id = n_result.data[0]["id"] if n_result.data else None
    except Exception as e:
        logger.error("Failed to log email notification: %s", e)
        notification_id = None

    return {
        "success": status == "sent",
        "notification_id": notification_id,
        "status": status,
        "error_message": error_message,
    }


def send_test_email(recipient: str) -> dict:
    """Send a test email to verify Microsoft Graph connectivity."""
    outlook = get_outlook()
    html = """
    <div style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #1a365d;">VWC Outreach — Test Email</h2>
        <p>This is a test email from the VWC LinkedIn Outreach system.</p>
        <p>If you received this, Microsoft Graph API email sending is working correctly.</p>
        <p style="color: #718096; font-size: 12px; margin-top: 20px;">
            Sent at {timestamp}
        </p>
    </div>
    """.format(timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))

    outlook.send_email(
        to=recipient,
        subject="VWC Outreach — Test Email",
        html_body=html,
    )
    return {"success": True, "recipient": recipient}


def get_notification_history(tenant_id: str | None = None) -> list[dict]:
    """Get email notification history."""
    sb = get_sb()
    query = sb.table("email_notifications").select(
        "id, prospect_id, campaign_id, recipient_email, subject, "
        "notification_type, status, error_message, created_at, "
        "prospects(first_name, last_name, company_name)"
    ).order("created_at", desc=True).limit(50)

    if tenant_id:
        query = query.eq("tenant_id", tenant_id)

    result = query.execute()
    return result.data or []


def _build_acceptance_html(
    prospect: dict,
    company: dict,
    messages: list[dict],
) -> str:
    """Build the HTML email body for an acceptance notification."""
    first_name = prospect.get("first_name", "")
    last_name = prospect.get("last_name", "")
    title = prospect.get("title", "")
    headline = prospect.get("headline", "")
    company_name = prospect.get("company_name", company.get("name", ""))
    linkedin_url = prospect.get("linkedin_url", "")
    email = prospect.get("email", "")
    location = prospect.get("location", "")
    icp_reasoning = prospect.get("icp_reasoning", "No ICP reasoning available.")
    icp_score = prospect.get("icp_score")

    company_industry = company.get("industry", "")
    company_domain = company.get("domain", "")
    company_employees = company.get("employee_count_range", "")

    # Build message rows
    message_rows = ""
    for msg in messages:
        step = msg.get("step", "?")
        text = msg.get("text", "")
        label = {1: "Initial Message", 2: "Follow-up #1 (~2 weeks)", 3: "Follow-up #2 (~4 weeks)"}.get(step, f"Message {step}")
        message_rows += f"""
        <tr>
            <td style="padding: 12px 16px; border-bottom: 1px solid #e2e8f0;">
                <div style="font-weight: 600; color: #2d3748; margin-bottom: 4px;">{label}</div>
                <div style="color: #4a5568; white-space: pre-wrap;">{text}</div>
            </td>
        </tr>"""

    score_badge = ""
    if icp_score is not None:
        color = "#38a169" if icp_score >= 70 else "#d69e2e" if icp_score >= 40 else "#e53e3e"
        score_badge = f'<span style="background: {color}; color: white; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: 600;">{icp_score}/100</span>'

    return f"""
    <div style="font-family: Arial, Helvetica, sans-serif; max-width: 640px; margin: 0 auto; background: #ffffff;">
        <!-- Header -->
        <div style="background: #1a365d; color: white; padding: 20px 24px;">
            <h1 style="margin: 0; font-size: 18px; font-weight: 600;">New LinkedIn Connection Accepted</h1>
            <p style="margin: 4px 0 0; font-size: 13px; color: #a0aec0;">VWC LinkedIn Outreach — Approval Required</p>
        </div>

        <!-- Prospect Info -->
        <div style="padding: 20px 24px; border-bottom: 1px solid #e2e8f0;">
            <h2 style="margin: 0 0 12px; font-size: 16px; color: #2d3748;">
                {first_name} {last_name} {score_badge}
            </h2>
            <table style="width: 100%; font-size: 14px; color: #4a5568;">
                <tr>
                    <td style="padding: 3px 0; width: 100px; font-weight: 600;">Title</td>
                    <td style="padding: 3px 0;">{title}</td>
                </tr>
                <tr>
                    <td style="padding: 3px 0; font-weight: 600;">Headline</td>
                    <td style="padding: 3px 0;">{headline}</td>
                </tr>
                <tr>
                    <td style="padding: 3px 0; font-weight: 600;">Location</td>
                    <td style="padding: 3px 0;">{location}</td>
                </tr>
                {"<tr><td style='padding: 3px 0; font-weight: 600;'>Email</td><td style='padding: 3px 0;'>" + email + "</td></tr>" if email else ""}
                {"<tr><td style='padding: 3px 0; font-weight: 600;'>LinkedIn</td><td style='padding: 3px 0;'><a href='" + linkedin_url + "' style='color: #3182ce;'>" + linkedin_url + "</a></td></tr>" if linkedin_url else ""}
            </table>
        </div>

        <!-- Company Info -->
        <div style="padding: 16px 24px; border-bottom: 1px solid #e2e8f0; background: #f7fafc;">
            <h3 style="margin: 0 0 8px; font-size: 14px; color: #2d3748;">Company</h3>
            <table style="width: 100%; font-size: 14px; color: #4a5568;">
                <tr>
                    <td style="padding: 3px 0; width: 100px; font-weight: 600;">Name</td>
                    <td style="padding: 3px 0;">{company_name}</td>
                </tr>
                {"<tr><td style='padding: 3px 0; font-weight: 600;'>Industry</td><td style='padding: 3px 0;'>" + company_industry + "</td></tr>" if company_industry else ""}
                {"<tr><td style='padding: 3px 0; font-weight: 600;'>Size</td><td style='padding: 3px 0;'>" + company_employees + "</td></tr>" if company_employees else ""}
                {"<tr><td style='padding: 3px 0; font-weight: 600;'>Website</td><td style='padding: 3px 0;'><a href='https://" + company_domain + "' style='color: #3182ce;'>" + company_domain + "</a></td></tr>" if company_domain else ""}
            </table>
        </div>

        <!-- ICP Reasoning -->
        <div style="padding: 16px 24px; border-bottom: 1px solid #e2e8f0;">
            <h3 style="margin: 0 0 8px; font-size: 14px; color: #2d3748;">Why This Fits Your ICP</h3>
            <p style="margin: 0; font-size: 14px; color: #4a5568; line-height: 1.5;">{icp_reasoning}</p>
        </div>

        <!-- Proposed Messages -->
        <div style="padding: 16px 24px;">
            <h3 style="margin: 0 0 12px; font-size: 14px; color: #2d3748;">Proposed LinkedIn Messages</h3>
            <table style="width: 100%; border: 1px solid #e2e8f0; border-radius: 8px; border-collapse: collapse;">
                {message_rows}
            </table>
            <p style="margin: 12px 0 0; font-size: 12px; color: #a0aec0;">
                Reply to this email or log in to the dashboard to approve, edit, or reject these messages.
            </p>
        </div>

        <!-- Footer -->
        <div style="padding: 16px 24px; background: #f7fafc; border-top: 1px solid #e2e8f0;">
            <p style="margin: 0; font-size: 11px; color: #a0aec0;">
                VWC LinkedIn Outreach System — yorCMO
            </p>
        </div>
    </div>
    """
