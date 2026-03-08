// ═══════════════════════════════════════════════════
// EDGE FUNCTION: run-sync-batch
// Called by pg_cron every 30 seconds
// Picks ONE pending window per active user
// Fetches Gmail → classifies → saves to gmail_emails
// Updates sync_jobs status
// ═══════════════════════════════════════════════════

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const SUPABASE_URL     = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE = Deno.env.get('SERVICE_ROLE_KEY')!;
const GOOGLE_CLIENT_ID = Deno.env.get('GOOGLE_CLIENT_ID')!;
const GOOGLE_SECRET    = Deno.env.get('GOOGLE_CLIENT_SECRET')!;
const ANTHROPIC_KEY    = Deno.env.get('ANTHROPIC_API_KEY')!;
const HAIKU            = 'claude-haiku-4-5-20251001';

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE);

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'authorization, content-type',
};

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: CORS });

  try {
    // 1. Find all users with pending sync jobs
    const { data: pendingJobs } = await sb
      .from('sync_jobs')
      .select('google_sub')
      .eq('status', 'pending')
      .order('window_index', { ascending: true });

    if (!pendingJobs || pendingJobs.length === 0) {
      return json({ message: 'No pending sync jobs' });
    }

    // Get unique users with pending jobs
    const users = [...new Set(pendingJobs.map((j: any) => j.google_sub))];
    const results = [];

    // Process ONE window per user per invocation (safe, no overload)
    for (const google_sub of users) {
      const result = await processNextWindow(google_sub);
      results.push({ google_sub, ...result });
    }

    return json({ processed: results });

  } catch (e: any) {
    console.error('run-sync-batch top error:', e);
    return json({ error: e.message }, 500);
  }
});

async function processNextWindow(google_sub: string) {
  // 1. Claim next pending window (oldest pending = lowest index)
  const { data: jobs } = await sb
    .from('sync_jobs')
    .select('*')
    .eq('google_sub', google_sub)
    .eq('status', 'pending')
    .order('window_index', { ascending: true })
    .limit(1);

  const job = jobs?.[0];
  if (!job) return { status: 'no_pending' };

  // 2. Mark as running (prevents duplicate processing)
  await sb.from('sync_jobs').update({
    status:     'running',
    started_at: new Date().toISOString(),
  }).eq('id', job.id);

  try {
    // 3. Get fresh access token
    const accessToken = await getAccessToken(google_sub);
    if (!accessToken) {
      await markJob(job.id, 'failed', 'Could not refresh access token');
      return { status: 'failed', reason: 'no_token' };
    }

    // 4. Fetch email IDs for this window from Gmail
    const emailIds = await fetchWindowIds(accessToken, job.after_ts, job.before_ts);

    if (emailIds.length === 0) {
      await markJob(job.id, 'done', null, 0, 0);
      return { status: 'done', window: job.window_label, emails: 0 };
    }

    // 5. Check which IDs are already cached
    const cached = await getCachedIds(emailIds);
    const cachedSet = new Set(cached);
    const uncachedIds = emailIds.filter((id: string) => !cachedSet.has(id));

    // 6. Fetch metadata for uncached — concurrency 3
    const metaRaw = await fetchMetadataConcurrent(accessToken, uncachedIds, 3);

    // 7. Parse + client-side auto-classify
    const parsed     = metaRaw.map(parseMeta);
    const autoEmails = parsed
      .filter(isAutoEmail)
      .map(e => ({ ...e, priority: 'fyi', category: 'newsletter',
                   summary: 'Newsletter / notification', context: [] }));
    const toAI = parsed
      .filter(e => !isAutoEmail(e))
      .map(e => ({ ...e, priority: 'normal', category: 'info',
                   summary: e.snippet, context: [] }));

    // 8. AI classify in batches of 5 (Haiku — fast + cheap)
    const classified = [];
    const aiBatch = 5;
    for (let i = 0; i < toAI.length; i += aiBatch) {
      const batch  = toAI.slice(i, i + aiBatch);
      const result = await Promise.all(batch.map(classifyEmail));
      classified.push(...result);
      // Small pause between AI batches
      if (i + aiBatch < toAI.length) {
        await sleep(300);
      }
    }

    const allEmails = [...classified, ...autoEmails];

    // 9. Save to gmail_emails
    if (allEmails.length > 0) {
      await saveEmails(google_sub, allEmails);
    }

    // 10. Mark job done
    await markJob(job.id, 'done', null, emailIds.length, allEmails.length);

    return {
      status:  'done',
      window:  job.window_label,
      found:   emailIds.length,
      cached:  cached.length,
      saved:   allEmails.length,
    };

  } catch (e: any) {
    console.error(`Window ${job.window_label} failed:`, e);
    await markJob(job.id, 'failed', e.message);
    return { status: 'failed', window: job.window_label, error: e.message };
  }
}

// ─── TOKEN MANAGEMENT ────────────────────────────────────────
async function getAccessToken(google_sub: string): Promise<string | null> {
  const { data } = await sb
    .from('user_tokens')
    .select('access_token, token_expiry, refresh_token')
    .eq('google_sub', google_sub)
    .single();

  if (!data) return null;

  // Return cached token if still valid (5 min buffer)
  if (data.access_token && data.token_expiry > Date.now() + 300_000) {
    return data.access_token;
  }

  // Refresh using refresh_token
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id:     GOOGLE_CLIENT_ID,
      client_secret: GOOGLE_SECRET,
      refresh_token: data.refresh_token,
      grant_type:    'refresh_token',
    }),
  });

  const tokens = await res.json();
  if (!res.ok || !tokens.access_token) {
    console.error('Token refresh failed:', tokens);
    return null;
  }

  // Update cached token
  await sb.from('user_tokens').update({
    access_token: tokens.access_token,
    token_expiry: Date.now() + (tokens.expires_in * 1000),
    updated_at:   new Date().toISOString(),
  }).eq('google_sub', google_sub);

  return tokens.access_token;
}

// ─── GMAIL API ────────────────────────────────────────────────
async function fetchWindowIds(token: string, after: number, before: number): Promise<string[]> {
  const ids: string[] = [];
  let pageToken: string | null = null;

  do {
    const q   = encodeURIComponent(`after:${after} before:${before}`);
    let url = `https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults=500&labelIds=INBOX&q=${q}`;
    if (pageToken) url += `&pageToken=${pageToken}`;

    const res  = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (res.status === 429) {
      await sleep(5000); // back off on rate limit
      continue;
    }
    if (!res.ok) break;

    const data = await res.json();
    (data.messages || []).forEach((m: any) => ids.push(m.id));
    pageToken = data.nextPageToken || null;
  } while (pageToken);

  return ids;
}

async function fetchMetadataConcurrent(
  token: string, ids: string[], concurrency: number
): Promise<any[]> {
  const results: any[] = new Array(ids.length).fill(null);
  let idx = 0;

  async function worker() {
    while (idx < ids.length) {
      const i  = idx++;
      const id = ids[i];
      try {
        const res = await fetch(
          `https://gmail.googleapis.com/gmail/v1/users/me/messages/${id}` +
          `?format=metadata&metadataHeaders=From&metadataHeaders=Subject` +
          `&metadataHeaders=To&metadataHeaders=Date`,
          { headers: { Authorization: `Bearer ${token}` } }
        );
        if (res.status === 429) { await sleep(3000); idx--; continue; } // retry
        if (res.ok) results[i] = await res.json();
      } catch (e) { results[i] = null; }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, worker));
  return results.filter(Boolean);
}

// ─── PARSE ────────────────────────────────────────────────────
function parseMeta(msg: any) {
  const h: Record<string, string> = {};
  (msg.payload?.headers || []).forEach((hh: any) => h[hh.name.toLowerCase()] = hh.value);
  const fromRaw = h.from || '';
  return {
    id:        msg.id,
    threadId:  msg.threadId,
    subject:   h.subject   || '(no subject)',
    from:      fromRaw,
    fromName:  fromRaw.replace(/<.*>/, '').replace(/"/g, '').trim() || fromRaw,
    fromEmail: fromRaw.match(/<(.+)>/)?.[1] || fromRaw,
    to:        h.to        || '',
    date:      parseInt(msg.internalDate || '0'),
    snippet:   msg.snippet || '',
    labelIds:  msg.labelIds || [],
    unread:    (msg.labelIds || []).includes('UNREAD'),
  };
}

function isAutoEmail(e: any): boolean {
  const f = (e.from || '').toLowerCase();
  const s = (e.subject || '').toLowerCase();
  return ['noreply', 'no-reply', 'donotreply', 'notifications@', 'alerts@',
          'updates@', 'mailer@', 'postmaster@'].some(p => f.includes(p))
      || ['unsubscribe', 'your receipt', 'order confirmed', 'invoice #']
           .some(p => s.includes(p));
}

// ─── AI CLASSIFY ──────────────────────────────────────────────
async function classifyEmail(email: any) {
  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type':      'application/json',
        'x-api-key':         ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model:      HAIKU,
        max_tokens: 120,
        system:     'Email classifier. Return ONLY valid JSON, no markdown.',
        messages: [{
          role:    'user',
          content: `{"priority":"urgent|high|normal|fyi","category":"action|meeting|info|newsletter","summary":"max 10 words"}\n\nFrom:${email.from}\nSubject:${email.subject}\nSnippet:${email.snippet}`,
        }],
      }),
    });

    const data = await res.json();
    const obj  = JSON.parse(
      (data.content?.[0]?.text || '{}').replace(/```json|```/g, '').trim()
    );
    return {
      ...email,
      priority: obj.priority || 'normal',
      category: obj.category || 'info',
      summary:  obj.summary  || email.snippet.slice(0, 80),
      context:  [],
    };
  } catch (e) {
    return { ...email, priority: 'normal', category: 'info',
             summary: email.snippet.slice(0, 80), context: [] };
  }
}

// ─── SUPABASE ─────────────────────────────────────────────────
async function getCachedIds(ids: string[]): Promise<string[]> {
  if (!ids.length) return [];
  const chunk = 200;
  const found: string[] = [];
  for (let i = 0; i < ids.length; i += chunk) {
    const slice = ids.slice(i, i + chunk);
    const { data } = await sb
      .from('gmail_emails')
      .select('gmail_id')
      .in('gmail_id', slice);
    (data || []).forEach((r: any) => found.push(r.gmail_id));
  }
  return found;
}

async function saveEmails(google_sub: string, emails: any[]) {
  const rows = emails.map(e => ({
    gmail_id:        e.id,
    thread_id:       e.threadId,
    subject:         e.subject,
    from_addr:       e.from,
    from_name:       e.fromName,
    from_email:      e.fromEmail,
    email_date:      e.date,
    snippet:         e.snippet,
    unread:          e.unread,
    priority:        e.priority,
    category:        e.category,
    ai_summary:      e.summary,
    context_signals: e.context || [],
  }));

  const chunk = 50;
  for (let i = 0; i < rows.length; i += chunk) {
    await sb.from('gmail_emails').upsert(
      rows.slice(i, i + chunk),
      { onConflict: 'gmail_id' }
    );
  }
}

async function markJob(
  id: number, status: string, error: string | null = null,
  found = 0, saved = 0
) {
  await sb.from('sync_jobs').update({
    status,
    emails_found:  found,
    emails_cached: saved,
    error_msg:     error,
    completed_at:  new Date().toISOString(),
  }).eq('id', id);
}

// ─── UTILS ────────────────────────────────────────────────────
function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json' },
  });
}
