"""HTML email template for acceptance notification — sent when a connection is accepted."""


def build_acceptance_html(
    prospect: dict,
    company: dict,
    messages: list[dict],
) -> str:
    """Build the HTML email body for an acceptance notification.

    Adapted from plan-context/email_svc.py _build_acceptance_html().
    """
    first_name = prospect.get("first_name", "")
    last_name = prospect.get("last_name", "")
    title = prospect.get("title", "")
    headline = prospect.get("headline", "")
    company_name = prospect.get("company_name", company.get("name", ""))
    linkedin_url = prospect.get("linkedin_url", "")
    email = prospect.get("email", "")
    location = prospect.get("location", "")
    icp_reasoning = (prospect.get("scoring") or {}).get("reasoning", "No ICP reasoning available.")
    icp_score = (prospect.get("scoring") or {}).get("score")

    company_industry = company.get("industry", "")
    company_domain = company.get("domain", "")
    company_employees = company.get("data", {}).get("employee_count_range", "") if company.get("data") else ""

    # Build message rows
    message_rows = ""
    for msg in messages:
        step = msg.get("step", "?")
        text = msg.get("original_text", msg.get("text", ""))
        label = {
            1: "Initial Message",
            2: "Follow-up #1 (~2 weeks)",
            3: "Follow-up #2 (~4 weeks)",
        }.get(step, f"Message {step}")
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

    email_row = f"<tr><td style='padding: 3px 0; font-weight: 600;'>Email</td><td style='padding: 3px 0;'>{email}</td></tr>" if email else ""
    linkedin_row = f"<tr><td style='padding: 3px 0; font-weight: 600;'>LinkedIn</td><td style='padding: 3px 0;'><a href='{linkedin_url}' style='color: #3182ce;'>{linkedin_url}</a></td></tr>" if linkedin_url else ""
    industry_row = f"<tr><td style='padding: 3px 0; font-weight: 600;'>Industry</td><td style='padding: 3px 0;'>{company_industry}</td></tr>" if company_industry else ""
    size_row = f"<tr><td style='padding: 3px 0; font-weight: 600;'>Size</td><td style='padding: 3px 0;'>{company_employees}</td></tr>" if company_employees else ""
    domain_row = f"<tr><td style='padding: 3px 0; font-weight: 600;'>Website</td><td style='padding: 3px 0;'><a href='https://{company_domain}' style='color: #3182ce;'>{company_domain}</a></td></tr>" if company_domain else ""

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
                {email_row}
                {linkedin_row}
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
                {industry_row}
                {size_row}
                {domain_row}
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
