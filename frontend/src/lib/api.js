const API_BASE_URL = (import.meta.env.VITE_API_URL || '').replace(/\/$/, '');

// Signed login session (minted by POST /api/auth/login). Persisted so the
// chosen role survives a page refresh; sent as a Bearer token on every call.
const SESSION_KEY = 'jn_auth_session';

export function getAuthSession() {
  try {
    const raw = localStorage.getItem(SESSION_KEY);
    const session = raw ? JSON.parse(raw) : null;
    return session && session.token && session.role ? session : null;
  } catch {
    return null;
  }
}

export function setAuthSession(session) {
  if (session && session.token) {
    localStorage.setItem(SESSION_KEY, JSON.stringify(session));
  } else {
    localStorage.removeItem(SESSION_KEY);
  }
}

export function apiFetch(path, options = {}) {
  const session = getAuthSession();
  const headers = { ...(options.headers || {}) };
  if (session?.token) {
    headers.Authorization = `Bearer ${session.token}`;
  }
  return fetch(`${API_BASE_URL}${path}`, { ...options, headers });
}
