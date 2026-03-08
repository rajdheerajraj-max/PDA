// ═══════════════════════════════════════════════════
// EDGE FUNCTION: oauth-callback
// Receives auth code from Google → exchanges for
// access_token + refresh_token → stores in user_tokens
// Redirects user back to frontend
// ═══════════════════════════════════════════════════

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const SUPABASE_URL     = Deno.env.get('SUPABASE_URL')!;
const SUPABASE_SERVICE = Deno.env.get('SERVICE_ROLE_KEY')!;
const GOOGLE_CLIENT_ID = Deno.env.get('GOOGLE_CLIENT_ID')!;
const GOOGLE_SECRET    = Deno.env.get('GOOGLE_CLIENT_SECRET')!;
const FRONTEND_URL     = 'https://rajdheerajraj-max.github.io/PDA/gmail-tool/';
const REDIRECT_URI     = `${Deno.env.get('SUPABASE_URL')}/functions/v1/oauth-callback`;

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE);

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  const code  = url.searchParams.get('code');
  const error = url.searchParams.get('error');

  // Google denied access
  if (error) {
    return redirect(`${FRONTEND_URL}?auth_error=${encodeURIComponent(error)}`);
  }

  if (!code) {
    return redirect(`${FRONTEND_URL}?auth_error=no_code`);
  }

  try {
    // 1. Exchange code for tokens
    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        code,
        client_id:     GOOGLE_CLIENT_ID,
        client_secret: GOOGLE_SECRET,
        redirect_uri:  REDIRECT_URI,
        grant_type:    'authorization_code',
      }),
    });

    const tokens = await tokenRes.json();
    if (!tokenRes.ok || !tokens.refresh_token) {
      console.error('Token exchange failed:', tokens);
      return redirect(`${FRONTEND_URL}?auth_error=token_exchange_failed`);
    }

    // 2. Get Google user info
    const userRes = await fetch('https://www.googleapis.com/oauth2/v3/userinfo', {
      headers: { Authorization: `Bearer ${tokens.access_token}` },
    });
    const user = await userRes.json();

    // 3. Upsert into user_tokens
    const { error: dbErr } = await sb.from('user_tokens').upsert({
      google_sub:    user.sub,
      email:         user.email,
      refresh_token: tokens.refresh_token,
      access_token:  tokens.access_token,
      token_expiry:  Date.now() + (tokens.expires_in * 1000),
      updated_at:    new Date().toISOString(),
    }, { onConflict: 'google_sub' });

    if (dbErr) {
      console.error('DB upsert error:', dbErr);
      return redirect(`${FRONTEND_URL}?auth_error=db_error`);
    }

    // 4. Redirect to frontend with access_token + sub
    // Frontend uses access_token for immediate API calls
    // Backend uses refresh_token for long-running sync
    const params = new URLSearchParams({
      access_token: tokens.access_token,
      expires_in:   String(tokens.expires_in),
      google_sub:   user.sub,
      email:        user.email,
      name:         user.name || '',
    });

    return redirect(`${FRONTEND_URL}#${params.toString()}`);

  } catch (e) {
    console.error('oauth-callback error:', e);
    return redirect(`${FRONTEND_URL}?auth_error=server_error`);
  }
});

function redirect(url: string) {
  return new Response(null, {
    status: 302,
    headers: { Location: url },
  });
}
