// Bearer-token support for network-exposed servers.
//
// When the backend is started with SRUNX_WEB_TOKEN set (required by
// `assert_safe_bind` for a non-loopback `srunx ui --host ...`), every /api/*
// request must carry `Authorization: Bearer <token>`. The default local
// (127.0.0.1, no token) deployment needs none of this — the interceptor is a
// no-op when no token is known and the server never returns 401.
//
// The token is obtained from a `?token=...` URL parameter (then persisted and
// stripped from the URL) or from localStorage, and the user is prompted once
// on the first 401.

const TOKEN_KEY = "srunx_web_token";

let token: string | null = null;

function loadToken(): string | null {
  try {
    const url = new URL(window.location.href);
    const fromUrl = url.searchParams.get("token");
    if (fromUrl) {
      localStorage.setItem(TOKEN_KEY, fromUrl);
      url.searchParams.delete("token");
      window.history.replaceState({}, "", url.toString());
      return fromUrl;
    }
    return localStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

function setToken(value: string): void {
  token = value;
  try {
    localStorage.setItem(TOKEN_KEY, value);
  } catch {
    /* ignore storage failures (private mode etc.) */
  }
}

function requestUrl(input: RequestInfo | URL): string {
  if (typeof input === "string") return input;
  if (input instanceof URL) return input.toString();
  return input.url;
}

function isApiRequest(url: string): boolean {
  // Every same-origin API call in this app is a relative path rooted at /api/.
  return url.startsWith("/api/");
}

/**
 * Wrap window.fetch so /api/* requests carry the bearer token (when known)
 * and a 401 triggers a one-time token prompt + retry. Idempotent.
 */
export function installAuthFetch(): void {
  const w = window as typeof window & { __srunxAuthInstalled?: boolean };
  if (w.__srunxAuthInstalled) return;
  w.__srunxAuthInstalled = true;

  token = loadToken();
  const original = window.fetch.bind(window);

  const withAuth = (
    init: RequestInit | undefined,
    value: string,
  ): RequestInit => {
    const headers = new Headers(init?.headers);
    if (!headers.has("Authorization"))
      headers.set("Authorization", `Bearer ${value}`);
    return { ...init, headers };
  };

  window.fetch = async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    const isApi = isApiRequest(requestUrl(input));
    const opts = isApi && token ? withAuth(init, token) : init;
    let res = await original(input, opts);

    if (isApi && res.status === 401) {
      const entered = window.prompt(
        "This srunx server requires an access token (SRUNX_WEB_TOKEN):",
        "",
      );
      if (entered) {
        setToken(entered);
        res = await original(input, withAuth(init, entered));
      }
    }
    return res;
  };
}
