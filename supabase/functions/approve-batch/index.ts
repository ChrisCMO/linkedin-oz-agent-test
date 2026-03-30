/**
 * Supabase Edge Function: approve-batch
 *
 * Called when a client clicks the "Approve & Start Connecting" button
 * in their batch review email. Marks the batch as approved in Supabase
 * and triggers an Oz cloud agent to start sending bare LinkedIn invites.
 *
 * URL: https://<project>.supabase.co/functions/v1/approve-batch?batch_id=xxx&token=yyy
 *
 * Bug fixes from plan-context version:
 * - events.type → events.event_type (schema v3 column name)
 * - events.source → events.actor (schema v3 column name)
 */

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { encode as hexEncode } from "https://deno.land/std@0.168.0/encoding/hex.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WARP_API_KEY = Deno.env.get("WARP_API_KEY") ?? "";
const OZ_ENVIRONMENT_ID = Deno.env.get("OZ_ENVIRONMENT_ID") ?? "";

async function hashToken(raw: string): Promise<string> {
  const data = new TextEncoder().encode(raw);
  const hash = await crypto.subtle.digest("SHA-256", data);
  return new TextDecoder().decode(hexEncode(new Uint8Array(hash)));
}

async function triggerOzAgent(batchId: string, batchInfo: string): Promise<boolean> {
  if (!WARP_API_KEY || !OZ_ENVIRONMENT_ID) {
    console.warn("Oz not configured — skipping agent trigger");
    return false;
  }

  try {
    const resp = await fetch("https://app.warp.dev/api/v1/agent/run", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${WARP_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        prompt:
          `Batch ${batchId} has been approved by the client. ${batchInfo} ` +
          `Send bare LinkedIn connection invites (NO notes) for all approved prospects in this batch. ` +
          `Rules: max 5 invites today, random delays 45-120s between each, ` +
          `business hours only (8-18 PT, weekdays), log every action to activity_log. ` +
          `Before each invite: verify prospect is not already FIRST_DEGREE via profile lookup.`,
        config: {
          environment_id: OZ_ENVIRONMENT_ID,
          skill_spec: "YorCMO/Linkedin-Testing:invite-sender",
        },
      }),
    });

    const result = await resp.json();
    console.log("Oz agent triggered:", result.run_id);
    return true;
  } catch (err) {
    console.error("Failed to trigger Oz agent:", err);
    return false;
  }
}

function confirmationHtml(batchInfo: {
  total: number;
  approved: number;
  sentTo: string;
}): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Batch Approved</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #f0f4f8;
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
      padding: 20px;
    }
    .card {
      background: white;
      border-radius: 16px;
      padding: 48px;
      max-width: 480px;
      text-align: center;
      box-shadow: 0 4px 24px rgba(0,0,0,0.08);
    }
    .check {
      width: 64px;
      height: 64px;
      background: #38a169;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      margin: 0 auto 24px;
    }
    .check svg { width: 32px; height: 32px; }
    h1 { font-size: 24px; color: #1a202c; margin-bottom: 12px; }
    p { font-size: 15px; color: #4a5568; line-height: 1.6; margin-bottom: 8px; }
    .detail { font-size: 13px; color: #a0aec0; margin-top: 24px; }
  </style>
</head>
<body>
  <div class="card">
    <div class="check">
      <svg fill="none" viewBox="0 0 24 24" stroke="white" stroke-width="3">
        <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/>
      </svg>
    </div>
    <h1>Batch Approved</h1>
    <p>${batchInfo.approved} connection requests will be sent over the next few days.</p>
    <p>Bare invites only (no notes). You'll receive an email with proposed messages once someone accepts your connection.</p>
    <p class="detail">~5 invites per day during business hours.</p>
  </div>
</body>
</html>`;
}

function errorHtml(message: string, status: number): Response {
  const html = `<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Error</title>
<style>
  body { font-family: -apple-system, sans-serif; background: #f0f4f8; display: flex; align-items: center; justify-content: center; min-height: 100vh; }
  .card { background: white; border-radius: 16px; padding: 48px; max-width: 480px; text-align: center; box-shadow: 0 4px 24px rgba(0,0,0,0.08); }
  h1 { color: #e53e3e; margin-bottom: 12px; }
  p { color: #4a5568; }
</style></head>
<body><div class="card"><h1>Oops</h1><p>${message}</p></div></body></html>`;

  return new Response(html, {
    status,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

serve(async (req: Request) => {
  const url = new URL(req.url);
  const batchId = url.searchParams.get("batch_id");
  const rawToken = url.searchParams.get("token");

  if (!batchId || !rawToken) {
    return errorHtml("Missing batch_id or token parameter.", 400);
  }

  const tokenHash = await hashToken(rawToken);
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

  // 1. Validate token and load batch
  const { data: batch, error } = await sb
    .from("batch_reviews")
    .select("*")
    .eq("id", batchId)
    .eq("token_hash", tokenHash)
    .single();

  if (error || !batch) {
    return errorHtml("Invalid or expired approval link.", 403);
  }

  // 2. Check expiration
  if (new Date(batch.expires_at) < new Date()) {
    return errorHtml("This approval link has expired. Please contact yorCMO for a new one.", 410);
  }

  // 3. Idempotent — already completed
  if (batch.completed_at) {
    return new Response(
      confirmationHtml({
        total: batch.total_count,
        approved: batch.approved_count || batch.total_count,
        sentTo: batch.sent_to_email,
      }),
      { status: 200, headers: { "Content-Type": "text/html; charset=utf-8" } }
    );
  }

  // 4. Mark batch as approved
  const now = new Date().toISOString();
  await sb
    .from("batch_reviews")
    .update({
      approved_count: batch.total_count,
      completed_at: now,
      last_accessed_at: now,
    })
    .eq("id", batchId);

  // 5. Update all prospects in batch to 'approved'
  const prospectIds: string[] = batch.prospect_ids || [];
  if (prospectIds.length > 0) {
    await sb
      .from("prospects")
      .update({ status: "approved", status_changed_at: now })
      .in("id", prospectIds)
      .in("status", ["scored", "queued"]);
  }

  // 6. Log approval event (FIXED: type→event_type, source→actor)
  await sb.from("events").insert({
    tenant_id: batch.tenant_id,
    event_type: "batch_approved",
    actor: "edge_function",
    data: {
      batch_id: batchId,
      prospect_count: prospectIds.length,
      approved_by: batch.sent_to_email,
    },
  });

  // 7. Trigger Oz agent (fire-and-forget)
  const batchInfo = `${prospectIds.length} prospects for campaign ${batch.campaign_id}.`;
  triggerOzAgent(batchId, batchInfo);

  // 8. Return confirmation page
  return new Response(
    confirmationHtml({
      total: batch.total_count,
      approved: batch.total_count,
      sentTo: batch.sent_to_email,
    }),
    { status: 200, headers: { "Content-Type": "text/html; charset=utf-8" } }
  );
});
