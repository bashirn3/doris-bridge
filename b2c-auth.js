require('dotenv').config();
const crypto = require('crypto');
const axios = require('axios');

const AUTHORITY = process.env.B2C_AUTHORITY;
const SPA_CLIENT_ID = process.env.B2C_SPA_CLIENT_ID;
const KAPA_CLIENT_ID = process.env.B2C_KAPA_CLIENT_ID;
const USERNAME = process.env.DORIS_USERNAME;
const PASSWORD = process.env.DORIS_PASSWORD;
const KAPA_BASE = process.env.KAPA_BASE_URL;

const SPA_REDIRECT = 'https://doris.idealinspect.fi';
const KAPA_REDIRECT = `${KAPA_BASE}/az/auth`;
const KAPA_SCOPE = `openid offline_access https://ykldorisprod.onmicrosoft.com/${SPA_CLIENT_ID}/Doris.Api`;

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

function generatePKCE() {
  const verifier = crypto.randomBytes(32).toString('base64url');
  const challenge = crypto.createHash('sha256').update(verifier).digest('base64url');
  return { verifier, challenge };
}

function extractSettings(html) {
  const m = html.match(/var\s+SETTINGS\s*=\s*(\{[\s\S]*?\});/);
  if (!m) throw new Error('Could not find SETTINGS in B2C login page');
  return JSON.parse(m[1]);
}

function extractCodeFromRedirect(location) {
  const hashIdx = location.indexOf('#');
  if (hashIdx < 0) {
    const url = new URL(location);
    return url.searchParams.get('code');
  }
  const fragment = location.slice(hashIdx + 1);
  const params = new URLSearchParams(fragment);
  return params.get('code');
}

class B2CAuth {
  constructor() {
    this.spaToken = null;
    this.spaRefreshToken = null;
    this.spaTokenExpiry = 0;

    this.kapaSessionCookie = null;
    this.kapaSessionExpiry = 0;

    this.http = axios.create({
      maxRedirects: 0,
      validateStatus: s => s < 500,
      headers: { 'User-Agent': UA },
      timeout: 30000,
    });
  }

  async getSpaToken() {
    const now = Date.now();
    if (this.spaToken && now < this.spaTokenExpiry - 60_000) {
      return this.spaToken;
    }
    if (this.spaRefreshToken) {
      try {
        await this._refreshSpaToken();
        return this.spaToken;
      } catch (e) {
        console.warn('[B2C] Refresh failed, doing full login:', e.message);
      }
    }
    await this._spaLogin();
    return this.spaToken;
  }

  async getKapaSession() {
    const now = Date.now();
    if (this.kapaSessionCookie && now < this.kapaSessionExpiry - 60_000) {
      return this.kapaSessionCookie;
    }
    await this._kapaLogin();
    return this.kapaSessionCookie;
  }

  async _spaLogin() {
    console.log('[B2C] SPA login starting...');
    const { verifier, challenge } = generatePKCE();
    const state = crypto.randomBytes(16).toString('hex');
    const nonce = crypto.randomBytes(16).toString('hex');

    const authorizeUrl = `${AUTHORITY}/oauth2/v2.0/authorize?` + new URLSearchParams({
      client_id: SPA_CLIENT_ID,
      redirect_uri: SPA_REDIRECT,
      response_type: 'code',
      scope: 'openid profile email offline_access',
      response_mode: 'fragment',
      state,
      nonce,
      code_challenge: challenge,
      code_challenge_method: 'S256',
    }).toString();

    const loginPage = await this.http.get(authorizeUrl);
    if (loginPage.status !== 200) {
      throw new Error(`B2C authorize returned ${loginPage.status}`);
    }

    const cookies = this._extractCookies(loginPage.headers['set-cookie']);
    const settings = extractSettings(loginPage.data);
    const csrf = settings.csrf;
    const transId = settings.transId;
    console.log('[B2C] Got login page, transId:', transId?.slice(0, 30) + '...');

    const selfAssertedUrl = `${AUTHORITY}/SelfAsserted?tx=${encodeURIComponent(transId)}&p=B2C_1A_SIGNIN`;
    const credRes = await this.http.post(selfAssertedUrl,
      new URLSearchParams({
        signInName: USERNAME,
        password: PASSWORD,
        request_type: 'RESPONSE',
      }).toString(),
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'X-CSRF-TOKEN': csrf,
          Cookie: cookies,
          Referer: authorizeUrl,
        },
      }
    );

    const credBody = typeof credRes.data === 'string' ? credRes.data : JSON.stringify(credRes.data);
    if (!credBody.includes('"status":"200"')) {
      throw new Error(`B2C credential POST failed: ${credBody.slice(0, 200)}`);
    }
    console.log('[B2C] Credentials accepted');

    const allCookies = this._mergeCookieStrings(
      cookies,
      this._extractCookies(credRes.headers['set-cookie'])
    );

    const confirmUrl = `${AUTHORITY}/api/CombinedSigninAndSignup/confirmed?` + new URLSearchParams({
      rememberMe: 'false',
      csrf_token: csrf,
      tx: transId,
      p: 'B2C_1A_SIGNIN',
    }).toString();

    const confirmRes = await this.http.get(confirmUrl, {
      headers: { Cookie: allCookies, Referer: authorizeUrl },
    });

    if (confirmRes.status !== 302) {
      throw new Error(`B2C confirm expected 302, got ${confirmRes.status}`);
    }

    const redirectLocation = confirmRes.headers.location;
    const code = extractCodeFromRedirect(redirectLocation);
    if (!code) {
      throw new Error('No auth code in redirect: ' + redirectLocation?.slice(0, 200));
    }
    console.log('[B2C] Got auth code');

    const tokenUrl = `${AUTHORITY}/oauth2/v2.0/token`;
    const tokenRes = await this.http.post(tokenUrl,
      new URLSearchParams({
        grant_type: 'authorization_code',
        client_id: SPA_CLIENT_ID,
        code,
        redirect_uri: SPA_REDIRECT,
        scope: 'openid profile email offline_access',
        code_verifier: verifier,
      }).toString(),
      { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
    );

    if (tokenRes.status !== 200 || !tokenRes.data.id_token) {
      throw new Error(`Token exchange failed: ${JSON.stringify(tokenRes.data).slice(0, 300)}`);
    }

    this.spaToken = tokenRes.data.id_token;
    this.spaRefreshToken = tokenRes.data.refresh_token || null;
    const expiresIn = tokenRes.data.id_token_expires_in || tokenRes.data.expires_in || 3600;
    this.spaTokenExpiry = Date.now() + expiresIn * 1000;

    console.log('[B2C] SPA token acquired, expires in', expiresIn, 'seconds');
  }

  async _refreshSpaToken() {
    console.log('[B2C] Refreshing SPA token...');
    const tokenUrl = `${AUTHORITY}/oauth2/v2.0/token`;
    const res = await this.http.post(tokenUrl,
      new URLSearchParams({
        grant_type: 'refresh_token',
        client_id: SPA_CLIENT_ID,
        refresh_token: this.spaRefreshToken,
        scope: 'openid profile email offline_access',
      }).toString(),
      { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
    );

    if (res.status !== 200 || !res.data.id_token) {
      throw new Error(`Refresh failed: ${JSON.stringify(res.data).slice(0, 200)}`);
    }

    this.spaToken = res.data.id_token;
    this.spaRefreshToken = res.data.refresh_token || this.spaRefreshToken;
    const expiresIn = res.data.id_token_expires_in || res.data.expires_in || 3600;
    this.spaTokenExpiry = Date.now() + expiresIn * 1000;
    console.log('[B2C] SPA token refreshed');
  }

  async _kapaLogin() {
    console.log('[B2C] KAPA login starting (full redirect chain)...');

    const step1 = await this.http.get(`${KAPA_BASE}/customer-register`, {
      headers: { Accept: 'text/html' },
    });

    let b2cUrl;
    if (step1.status === 302) {
      b2cUrl = step1.headers.location;
    } else if (step1.status === 200 && typeof step1.data === 'string') {
      const metaMatch = step1.data.match(/url=([^"]+)/i);
      if (metaMatch) b2cUrl = metaMatch[1];
    }

    if (!b2cUrl) {
      throw new Error(`KAPA did not redirect to B2C. Status: ${step1.status}`);
    }

    const kapaCookies = this._extractCookies(step1.headers['set-cookie']);
    console.log('[B2C] KAPA redirected to B2C');

    if (!b2cUrl.startsWith('http')) {
      b2cUrl = `${KAPA_BASE}${b2cUrl}`;
    }

    let authorizeUrl = b2cUrl;
    const kapaBounce = await this.http.get(b2cUrl);
    if (kapaBounce.status === 302) {
      authorizeUrl = kapaBounce.headers.location;
    }

    const loginPage = await this.http.get(authorizeUrl);
    if (loginPage.status !== 200) {
      throw new Error(`B2C KAPA authorize returned ${loginPage.status}`);
    }

    const b2cCookies = this._extractCookies(loginPage.headers['set-cookie']);
    const settings = extractSettings(loginPage.data);
    const csrf = settings.csrf;
    const transId = settings.transId;
    console.log('[B2C] Got KAPA B2C login page');

    const selfAssertedUrl = `${AUTHORITY}/SelfAsserted?tx=${encodeURIComponent(transId)}&p=B2C_1A_SIGNIN`;
    const credRes = await this.http.post(selfAssertedUrl,
      new URLSearchParams({
        signInName: USERNAME,
        password: PASSWORD,
        request_type: 'RESPONSE',
      }).toString(),
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'X-CSRF-TOKEN': csrf,
          Cookie: b2cCookies,
          Referer: authorizeUrl,
        },
      }
    );

    const credBody = typeof credRes.data === 'string' ? credRes.data : JSON.stringify(credRes.data);
    if (!credBody.includes('"status":"200"')) {
      throw new Error(`B2C KAPA cred POST failed: ${credBody.slice(0, 200)}`);
    }
    console.log('[B2C] KAPA credentials accepted');

    const confirmUrl = `${AUTHORITY}/api/CombinedSigninAndSignup/confirmed?` + new URLSearchParams({
      rememberMe: 'false',
      csrf_token: csrf,
      tx: transId,
      p: 'B2C_1A_SIGNIN',
    }).toString();

    const allB2cCookies = this._mergeCookieStrings(
      b2cCookies,
      this._extractCookies(credRes.headers['set-cookie'])
    );

    const confirmRes = await this.http.get(confirmUrl, {
      headers: { Cookie: allB2cCookies, Referer: authorizeUrl },
    });

    if (confirmRes.status !== 302) {
      throw new Error(`B2C KAPA confirm expected 302, got ${confirmRes.status}`);
    }

    let callbackUrl = confirmRes.headers.location;
    console.log('[B2C] B2C redirecting to KAPA callback');

    const callbackRes = await this.http.get(callbackUrl, {
      headers: { Cookie: kapaCookies },
    });

    let finalCookies = this._extractCookies(callbackRes.headers['set-cookie']);

    if (callbackRes.status === 302) {
      let nextUrl = callbackRes.headers.location;
      if (!nextUrl.startsWith('http')) nextUrl = `${KAPA_BASE}${nextUrl}`;
      const mergedKapaCookies = this._mergeCookieStrings(kapaCookies, finalCookies);

      const finalRes = await this.http.get(nextUrl, {
        headers: { Cookie: mergedKapaCookies },
      });
      finalCookies = this._mergeCookieStrings(mergedKapaCookies, this._extractCookies(finalRes.headers['set-cookie']));
    }

    const phpsessid = this._getCookieValue(finalCookies, 'PHPSESSID');
    if (!phpsessid) {
      throw new Error('KAPA login succeeded but no PHPSESSID found');
    }

    this.kapaSessionCookie = `PHPSESSID=${phpsessid}`;
    this.kapaSessionExpiry = Date.now() + 55 * 60 * 1000;
    console.log('[B2C] KAPA session acquired (PHPSESSID)');
  }

  _extractCookies(setCookieHeaders) {
    if (!setCookieHeaders) return '';
    const arr = Array.isArray(setCookieHeaders) ? setCookieHeaders : [setCookieHeaders];
    const pairs = arr.map(raw => raw.split(';')[0].trim());
    return pairs.join('; ');
  }

  _mergeCookieStrings(...strings) {
    const map = new Map();
    for (const s of strings) {
      if (!s) continue;
      for (const pair of s.split(';')) {
        const eq = pair.indexOf('=');
        if (eq > 0) {
          map.set(pair.slice(0, eq).trim(), pair.slice(eq + 1).trim());
        }
      }
    }
    return [...map.entries()].map(([k, v]) => `${k}=${v}`).join('; ');
  }

  _getCookieValue(cookieStr, name) {
    if (!cookieStr) return null;
    for (const pair of cookieStr.split(';')) {
      const eq = pair.indexOf('=');
      if (eq > 0 && pair.slice(0, eq).trim() === name) {
        return pair.slice(eq + 1).trim();
      }
    }
    return null;
  }
}

module.exports = { B2CAuth };
