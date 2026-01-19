/**
 * config.js
 * Stores configuration, constants, and generic utility functions used by every other file.
 */

// ============================================================================
// API ENDPOINTS
// ============================================================================
// Always use the custom domain for proper cookie handling
export const BACKEND = window.TIMRX_3D_API_BASE || 'https://3d.timrx.live';
export const CHAT_API = window.TIMRX_API_BASE || 'https://timrx-chat-1.onrender.com';

console.log('[Config] BACKEND:', BACKEND, 'hostname:', window.location.hostname);

// ============================================================================
// STORAGE KEYS
// ============================================================================
export const ACTIVE_JOBS_STORAGE_KEY = 'activeJobs_v1';
export const PENDING_JOBS_STORAGE_KEY = 'pendingJobs_v1';

// ============================================================================
// UI CONSTANTS
// ============================================================================
export const HISTORY_MENU_EDGE_PAD = 12;
export const HISTORY_SUBMENU_GAP = 10;

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Console logging with prefix
 */
export const log = (...args) => console.log('[TimrX]', ...args);

/**
 * Shorthand for getElementById
 */
export const byId = (id) => document.getElementById(id);

/**
 * Safely execute a function only if element exists
 */
export function safe(el, fn) {
  if (el) fn();
}

/**
 * POST JSON to a URL and return parsed response
 */
export async function postJSON(url, data) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

/**
 * Convert a File object to a data URL
 */
export function fileToDataURL(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

/**
 * Normalize any timestamp-ish value to epoch milliseconds
 */
export function normalizeEpochMs(input) {
  if (input == null) return Date.now();

  // If it's a numeric-looking string, make it a number
  if (typeof input === 'string' && /^\d+$/.test(input)) input = Number(input);

  // ISO date string?
  if (typeof input === 'string') {
    const t = Date.parse(input);
    return Number.isNaN(t) ? Date.now() : t;
  }

  // Number -> decide seconds vs milliseconds
  if (typeof input === 'number') {
    if (input > 1e15) {
      return Math.floor(input / 1000);
    }
    if (input < 1e12) {
      // looks like seconds
      return input * 1000;
    }
    return input; // already ms
  }

  return Date.now();
}

/**
 * Create a unique batch group ID
 */
export function createBatchGroupId(prefix = 'batch') {
  try {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
      return crypto.randomUUID();
    }
  } catch (_) {
    /* noop */
  }
  const rand = Math.floor(Math.random() * 1e6);
  return `${prefix}-${Date.now()}-${rand}`;
}

/**
 * Format a timestamp as DD/MM/YYYY
 */
export function dateLabel(ts) {
  try {
    const ms = normalizeEpochMs(ts);
    const d = new Date(ms);

    const y = d.getFullYear();
    if (y < 2000 || y > 2099) {
      const now = new Date();
      const dd = String(now.getDate()).padStart(2, '0');
      const mm = String(now.getMonth() + 1).padStart(2, '0');
      const yyyy = now.getFullYear();
      return `${dd}/${mm}/${yyyy}`;
    }

    const dd = String(d.getDate()).padStart(2, '0');
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    return `${dd}/${mm}/${y}`;
  } catch {
    return '';
  }
}

/**
 * Wait for Three.js to be ready before executing callback
 */
export function onThreeReady(cb) {
  if (window.THREE && THREE.GLTFLoader && THREE.OrbitControls) {
    cb();
  } else {
    window.addEventListener('three-ready', () => cb(), { once: true });
  }
}
