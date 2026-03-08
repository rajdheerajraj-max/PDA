// ═══════════════════════════════════════════════════
// EDGE FUNCTION: start-sync
// Called by frontend after login
// Creates 73 sync_job rows (5-day windows, newest first)
// Skips windows already created for this user
// ═══════════════════════════════════════════════════

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const SUPABASE_URL     = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE = Deno.env.get('SERVICE_ROLE_KEY')!;

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE);

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'authorization, content-type',
};

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: CORS });
  }

  try {
    const body = await req.json();
    const { google_sub } = body;

    if (!google_sub) {
      return json({ error: 'google_sub required' }, 400);
    }

    // Check how many windows already exist for this user
    const { data: existing } = await sb
      .from('sync_jobs')
      .select('window_index, status')
      .eq('google_sub', google_sub);

    const existingMap: Record<number, string> = {};
    (existing || []).forEach((r: any) => {
      existingMap[r.window_index] = r.status;
    });

    // Build 73 windows — newest first (index 0 = most recent)
    const windows = build5DayWindows();
    const toInsert = [];

    for (const win of windows) {
      // Skip if already done or running
      const existingStatus = existingMap[win.index];
      if (existingStatus === 'done' || existingStatus === 'running') continue;

      toInsert.push({
        google_sub,
        window_index: win.index,
        window_label: win.label,
        after_ts:     win.after,
        before_ts:    win.before,
        status:       'pending',
      });
    }

    if (toInsert.length === 0) {
      // Check if all done
      const doneCount = Object.values(existingMap).filter(s => s === 'done').length;
      return json({
        message: 'Sync jobs already exist',
        total: windows.length,
        done: doneCount,
        pending: windows.length - doneCount,
      });
    }

    // Upsert — safe to call multiple times
    const { error } = await sb
      .from('sync_jobs')
      .upsert(toInsert, { onConflict: 'google_sub,window_index' });

    if (error) {
      console.error('start-sync insert error:', error);
      return json({ error: error.message }, 500);
    }

    return json({
      message: 'Sync jobs created',
      total: windows.length,
      created: toInsert.length,
      skipped: windows.length - toInsert.length,
    });

  } catch (e: any) {
    console.error('start-sync error:', e);
    return json({ error: e.message }, 500);
  }
});

// Build 73 windows of 5 days each, newest first
function build5DayWindows() {
  const windows = [];
  const now     = Math.floor(Date.now() / 1000);
  const ONE_YEAR  = 365 * 24 * 60 * 60;
  const FIVE_DAYS =   5 * 24 * 60 * 60;
  const startEpoch = now - ONE_YEAR;

  let index = 0;
  for (let end = now; end > startEpoch; end -= FIVE_DAYS) {
    const wStart = Math.max(end - FIVE_DAYS, startEpoch);
    const startDate = new Date(wStart * 1000);
    const endDate   = new Date(end * 1000);
    windows.push({
      index,
      after:  wStart,
      before: end,
      label:  `${fmt(startDate)} – ${fmt(endDate)}`,
    });
    index++;
  }
  return windows;
}

function fmt(d: Date) {
  return d.toLocaleDateString('en-IN', { day: 'numeric', month: 'short' });
}

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json' },
  });
}
