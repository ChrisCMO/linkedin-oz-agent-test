"""Connection note and message sequence generation using OpenAI."""

import json
import logging

from mvp.backend.config import get_openai

logger = logging.getLogger(__name__)

MODEL = "gpt-4o-mini"  # Cheaper model for message generation (not scoring)


def generate_connection_note(
    prospect: dict,
    company: dict,
    sender_name: str,
) -> str:
    """Generate a LinkedIn connection note (≤200 characters).

    FROM sender TO prospect, referencing their company.
    Warm, professional, not salesy, no firm name mentioned.
    """
    first_name = prospect.get("first_name", "")
    title = prospect.get("title", "")
    company_name = company.get("name", prospect.get("company_name", ""))
    industry = company.get("industry", "")
    location = company.get("location", "")

    prompt = f"""Write a LinkedIn connection request note FROM {sender_name} TO {first_name}.

Context:
- {first_name} is {title} at {company_name}
- {company_name} is in {industry}, located in {location}
- {sender_name} works at a CPA/advisory firm (do NOT name the firm)

Rules:
- MUST be 200 characters or fewer (this is a hard LinkedIn limit)
- Address {first_name} by first name
- Reference something specific about their company or role
- Warm and professional tone, NOT salesy
- Do NOT mention the sender's firm name
- Do NOT ask for a meeting or pitch services
- Just establish a genuine connection

Return ONLY the note text, nothing else."""

    try:
        client = get_openai()
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.7,
        )
        note = resp.choices[0].message.content.strip().strip('"')

        # Enforce 200 char limit
        if len(note) > 200:
            # Retry with stricter prompt
            retry_prompt = f"This connection note is {len(note)} chars but must be ≤200. Shorten it:\n\n{note}"
            resp2 = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": retry_prompt}],
                max_tokens=80,
                temperature=0.5,
            )
            note = resp2.choices[0].message.content.strip().strip('"')
            if len(note) > 200:
                note = note[:197] + "..."

        return note
    except Exception as e:
        logger.error("Connection note generation failed: %s", e)
        return ""


def generate_messages(
    prospect: dict,
    company: dict,
    sender_name: str,
    icp_config: dict | None = None,
) -> dict:
    """Generate a 3-message follow-up sequence.

    Returns {"msg1": "...", "msg2": "...", "msg3": "..."}.
    """
    first_name = prospect.get("first_name", "")
    title = prospect.get("title", "")
    company_name = company.get("name", prospect.get("company_name", ""))
    industry = company.get("industry", "")
    location = company.get("location", "")
    li_description = company.get("li_description", company.get("description", ""))

    prompt = f"""Write a 3-message LinkedIn follow-up sequence FROM {sender_name} TO {first_name}.

Context:
- {first_name} is {title} at {company_name}
- {company_name}: {industry}, {location}
- Company description: {(li_description or '')[:200]}
- {sender_name} works at a CPA/advisory firm (do NOT name the firm)
- {first_name} has already accepted the connection request

Message timing:
- Message 1: Sent right after connection accepted. Thank them, reference their role/company.
- Message 2: ~2 weeks later. Different angle — industry insight, shared challenge, or relevant observation.
- Message 3: ~4 weeks later. Final light touch. Low pressure, leave the door open.

Rules:
- Each message: 50-150 words
- Professional but conversational tone
- Do NOT pitch services directly in messages 1-2
- Message 3 can gently mention advisory/CPA services
- Do NOT use the sender's firm name
- Reference something specific about {company_name} or {first_name}'s role
- Each message should feel different, not repetitive

Return JSON: {{"msg1": "...", "msg2": "...", "msg3": "..."}}"""

    try:
        client = get_openai()
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=800,
            temperature=0.7,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content.strip()
        messages = json.loads(content)
        return {
            "msg1": messages.get("msg1", ""),
            "msg2": messages.get("msg2", ""),
            "msg3": messages.get("msg3", ""),
        }
    except Exception as e:
        logger.error("Message generation failed: %s", e)
        return {"msg1": "", "msg2": "", "msg3": ""}


def generate_outreach_for_prospect(
    prospect: dict,
    company: dict,
    sender_names: list[str] | None = None,
) -> dict:
    """Generate connection notes + messages for one or more senders.

    Returns:
    {
        "connection_notes": {"Adrienne Nordland": "...", "Melinda Grier": "..."},
        "messages": {"Adrienne Nordland": {"msg1": ..., "msg2": ..., "msg3": ...}, ...}
    }
    """
    if sender_names is None:
        sender_names = ["Adrienne Nordland", "Melinda Grier"]

    result = {"connection_notes": {}, "messages": {}}

    for sender in sender_names:
        note = generate_connection_note(prospect, company, sender)
        msgs = generate_messages(prospect, company, sender)
        result["connection_notes"][sender] = note
        result["messages"][sender] = msgs

    return result
