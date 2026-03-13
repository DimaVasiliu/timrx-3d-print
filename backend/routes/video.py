/**
 * workspace-credits.js
 * Manages credits/wallet state for the 3dprint.html workspace.
 * Fetches wallet balance and action costs on load, provides helpers for credit checks.
 */

import { BACKEND, log, apiFetch, updateSessionInfo, readWalletCache, writeWalletCache, clearWalletCache } from './config.js';

// ============================================================================
// CONSTANTS
// ============================================================================

const CREDITS_CACHE_KEY = 'timrx_credits_last';
const VIDEO_CREDITS_CACHE_KEY = 'timrx_video_credits_last';

// ============================================================================
// SINGLE-FLIGHT GUARD
// ============================================================================

// Track in-flight fetch promises to prevent duplicate requests
let walletFetchInFlight = null;
let refreshInFlight = null;
let pendingRetry = false; // Flag for window.focus retry
let lastRefreshTime = 0; // Track last refresh for visibility/focus throttling
const MIN_REFRESH_INTERVAL_MS = 5000; // Don't refresh more than once per 5s

// ============================================================================
// STATE
// ============================================================================

const creditsState = {
  wallet: {
    balance: 0,
    reserved: 0,
    available: 0,
    // Video credits (separate pool)
    videoBalance: 0,
    videoReserved: 0,
    videoAvailable: 0,
  },
  identityId: null,
  email: null,  // User's email (null if not attached)
  emailVerified: false,
  actionCosts: {},
  loaded: false,
  loading: false,
  error: null,
  // Optimistic updates tracking
  pendingDeductions: [],  // Array of { id, amount, action, timestamp }
  lastServerBalance: null,
  // Reservation tracking (credits held during generation)
  reservations: new Map(),  // Map<jobId, { amount, action, timestamp }>
  totalReserved: 0,  // Sum of all active reservations
};

// Idempotency: track job IDs that have already been charged
// Prevents duplicate deductions from double-clicks or retries
const chargedJobs = new Set();

// ============================================================================
// EARLY RENDER (for perceived performance)
// ============================================================================

/**
 * Check if we should force a fresh fetch (e.g., after purchase redirect).
 * URL params: ?refresh=1 or referrer from hub after purchase
 */
function shouldForceRefresh() {
  const params = new URLSearchParams(window.location.search);
  // Force refresh if ?refresh=1 is in URL (set by hub after purchase)
  if (params.get('refresh') === '1') {
    // Clear the param from URL to avoid repeated refreshes on reload
    const url = new URL(window.location.href);
    url.searchParams.delete('refresh');
    window.history.replaceState({}, '', url.toString());
    log('[Credits] Force refresh requested via URL param');
    return true;
  }
  // Force refresh if coming from hub (different origin purchase flow)
  if (document.referrer && document.referrer.includes('timrx.live') && !document.referrer.includes('3d.timrx.live')) {
    log('[Credits] Force refresh: navigated from hub');
    return true;
  }
  return false;
}

// Track if force refresh was requested (checked at module load time)
const FORCE_REFRESH = shouldForceRefresh();

/**
 * Render cached credits immediately on page load (before async fetch).
 * This provides instant visual feedback using the last known balance.
 * Call this as early as possible - even before DOM ready if elements exist.
 */
function renderCachedCreditsEarly() {
  const creditsPill = document.getElementById('workspaceCredits');
  const creditsValue = document.getElementById('workspaceCreditsValue');
  const creditsGroup = document.getElementById('workspaceCreditsGroup');

  // If UI elements don't exist yet, try again after DOM ready
  if (!creditsValue) {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', renderCachedCreditsEarly, { once: true });
    }
    return;
  }

  // Skip cache if force refresh was requested (show syncing immediately)
  if (FORCE_REFRESH) {
    log('[Credits] Skipping cache render due to force refresh');
    creditsValue.textContent = '—';
    if (creditsGroup) creditsGroup.classList.add('syncing');
    if (creditsPill) {
      creditsPill.classList.add('syncing');
      creditsPill.setAttribute('title', 'Syncing credits...');
    }
    return;
  }

  // Priority 1: Check cross-page wallet cache (fresher, from hub after purchase)
  const walletCache = readWalletCache();
  let displayValue = '—';
  let cacheSource = null;

  if (walletCache && typeof walletCache.available_credits === 'number') {
    displayValue = walletCache.available_credits.toLocaleString();
    // Pre-populate state so hasCreditsFor() works with cached value
    creditsState.wallet.available = walletCache.available_credits;
    creditsState.wallet.balance = walletCache.available_credits;
    creditsState.identityId = walletCache.identity_id || null;
    cacheSource = 'cross-page';
    log('[Credits] Early render from cross-page cache:', walletCache.available_credits);
  } else {
    // Priority 2: Fall back to local credits cache
    const cached = localStorage.getItem(CREDITS_CACHE_KEY);
    if (cached !== null) {
      const cachedBalance = parseInt(cached, 10);
      if (Number.isFinite(cachedBalance) && cachedBalance >= 0) {
        displayValue = cachedBalance.toLocaleString();
        // Pre-populate state so hasCreditsFor() works with cached value
        creditsState.wallet.available = cachedBalance;
        creditsState.wallet.balance = cachedBalance;
        cacheSource = 'local';
        log('[Credits] Early render from local cache:', cachedBalance);
      }
    }
  }

  // Also restore cached video credits (separate pool)
  const cachedVideo = localStorage.getItem(VIDEO_CREDITS_CACHE_KEY);
  if (cachedVideo !== null) {
    const cachedVideoBalance = parseInt(cachedVideo, 10);
    if (Number.isFinite(cachedVideoBalance) && cachedVideoBalance >= 0) {
      creditsState.wallet.videoAvailable = cachedVideoBalance;
      creditsState.wallet.videoBalance = cachedVideoBalance;
    }
  }

  if (!cacheSource) {
    log('[Credits] No cached balance, showing syncing placeholder');
  }

  // Render immediately
  creditsValue.textContent = displayValue;

  // Add syncing indicator
  if (creditsGroup) {
    creditsGroup.classList.add('syncing');
  }
  if (creditsPill) {
    creditsPill.classList.add('syncing');
    creditsPill.setAttribute('title', 'Syncing credits...');
  }
}

/**
 * Save credits balance to localStorage for next page load
 */
function cacheCreditsBalance(balance, videoBalance) {
  if (typeof balance === 'number' && Number.isFinite(balance) && balance >= 0) {
    localStorage.setItem(CREDITS_CACHE_KEY, balance.toString());
    log('[Credits] Cached balance to localStorage:', balance);
  }
  if (typeof videoBalance === 'number' && Number.isFinite(videoBalance) && videoBalance >= 0) {
    localStorage.setItem(VIDEO_CREDITS_CACHE_KEY, videoBalance.toString());
  }
}

// ============================================================================
// API FETCHING
// ============================================================================

/**
 * Fetch wallet balance from /api/me
 * Single-flight: returns existing promise if already in flight
 * Response format:
 * {
 *   ok: true,
 *   identity_id: "uuid",
 *   balance_credits: 100,
 *   reserved_credits: 0,
 *   available_credits: 100,
 *   ...
 * }
 */
export async function fetchWallet() {
  // Single-flight guard: return existing promise if already fetching
  if (walletFetchInFlight) {
    log('[Credits] fetchWallet already in flight, returning existing promise');
    return walletFetchInFlight;
  }

  const url = `${BACKEND}/api/me`;
  log('[Credits] Fetching wallet from:', url);

  // Create the fetch promise with single-flight tracking
  walletFetchInFlight = (async () => {
    try {
      const result = await apiFetch('/api/me', {
        cache: 'no-store',
        keepalive: true,
      });

      if (!result.ok) {
        // Not authenticated or error - log details
        log('[Credits] Wallet fetch failed:', result.status, result.error);
        creditsState.wallet = { balance: 0, reserved: 0, available: 0, videoBalance: 0, videoReserved: 0, videoAvailable: 0 };
        pendingRetry = true; // Schedule retry on window.focus
        return creditsState.wallet;
      }

      const data = result.data;
      log('[Credits] /api/me response:', {
        ok: data.ok,
        identity_id: data.identity_id,
        balance_credits: data.balance_credits,
        reserved_credits: data.reserved_credits,
        available_credits: data.available_credits,
      });

      if (data.ok) {
        // Read credits from top-level fields (new format) with fallback to nested wallet object
        const balance = data.balance_credits ?? data.wallet?.balance ?? 0;
        const reserved = data.reserved_credits ?? data.wallet?.reserved ?? 0;
        const available = data.available_credits ?? data.wallet?.available ?? Math.max(0, balance - reserved);
        const serverIdentityId = data.identity_id || null;

        // Video credits (separate pool)
        const videoBalance = data.video_credits_balance ?? data.video_balance_credits ?? 0;
        const videoReserved = data.video_reserved_credits ?? 0;
        const videoAvailable = data.video_available_credits ?? Math.max(0, videoBalance - videoReserved);

        // Check if identity differs from cross-page cache - if so, discard cache
        const walletCache = readWalletCache();
        if (walletCache && walletCache.identity_id && serverIdentityId && walletCache.identity_id !== serverIdentityId) {
          log('[Credits] Identity mismatch - clearing cross-page cache');
          log('[Credits]   Cached:', walletCache.identity_id?.slice(0, 8) + '...');
          log('[Credits]   Server:', serverIdentityId?.slice(0, 8) + '...');
          clearWalletCache();
        }

        creditsState.wallet = {
          balance,
          reserved,
          available,
          // Video credits
          videoBalance,
          videoReserved,
          videoAvailable,
        };
        creditsState.identityId = serverIdentityId;
        creditsState.email = data.email || null;
        creditsState.emailVerified = data.email_verified || false;

        // Server's available already accounts for backend reservations.
        // Clear client-side reservations to avoid double-counting.
        creditsState.reservations.clear();
        creditsState.totalReserved = 0;

        // Update email beacon visibility
        updateEmailBeaconUI();

        // Cache balance for next page load (perceived performance)
        cacheCreditsBalance(available, videoAvailable);

        // Also write to cross-page wallet cache
        if (serverIdentityId) {
          writeWalletCache(serverIdentityId, available);
        }

        pendingRetry = false; // Clear retry flag on success
        lastRefreshTime = Date.now(); // Track for visibility throttling

        log('[Credits] Wallet loaded:', creditsState.wallet);

        // Update global session info for debugging
        updateSessionInfo(data, 'workspace');
      } else {
        log('[Credits] /api/me returned ok:false');
        creditsState.wallet = { balance: 0, reserved: 0, available: 0, videoBalance: 0, videoReserved: 0, videoAvailable: 0 };
        pendingRetry = true;
      }

      return creditsState.wallet;
    } catch (err) {
      log('[Credits] Wallet fetch error:', err.message);
      // Keep cached balance on timeout, schedule retry
      pendingRetry = true;
      creditsState.error = err.message;
      return creditsState.wallet;
    } finally {
      walletFetchInFlight = null; // Clear single-flight guard
    }
  })();

  return walletFetchInFlight;
}

/**
 * Fetch action costs from /api/billing/action-costs
 * Response format: { ok: true, action_costs: [{ action_key: "...", credits: N }, ...] }
 */
export async function fetchActionCosts() {
  try {
    const result = await apiFetch('/api/billing/action-costs');

    if (!result.ok) {
      log('[Credits] Action costs fetch failed:', result.status);
      creditsState.actionCosts = getDefaultActionCosts();
      return creditsState.actionCosts;
    }

    const data = result.data;

    // Handle array format from backend: { action_costs: [{ action_key, credits }, ...] }
    if (data.ok && Array.isArray(data.action_costs)) {
      const costsMap = {};
      data.action_costs.forEach(item => {
        if (item.action_key && typeof item.credits === 'number') {
          costsMap[item.action_key] = item.credits;
        }
      });

      // Add legacy aliases for backward compatibility
      // Backend now returns canonical keys; we add aliases for any code still using old keys
      // Canonical -> Legacy aliases
      if (costsMap['text_to_3d_generate']) {
        costsMap['text-to-3d'] = costsMap['text_to_3d_generate'];
        costsMap['preview'] = costsMap['text_to_3d_generate'];
      }
      if (costsMap['image_to_3d_generate']) {
        costsMap['image-to-3d'] = costsMap['image_to_3d_generate'];
      }
      if (costsMap['image_generate']) {
        costsMap['text-to-image'] = costsMap['image_generate'];
        costsMap['image_studio_generate'] = costsMap['image_generate'];
      }
      if (costsMap['refine']) {
        costsMap['upscale'] = costsMap['refine'];
      }
      if (costsMap['retexture']) {
        costsMap['texture'] = costsMap['retexture'];
      }
      if (costsMap['video_generate']) {
        costsMap['video'] = costsMap['video_generate'];
      }
      if (costsMap['video_text_generate']) {
        costsMap['text2video'] = costsMap['video_text_generate'];
      }
      if (costsMap['video_image_animate']) {
        costsMap['image2video'] = costsMap['video_image_animate'];
      }

      // If no costs were parsed, use defaults
      if (Object.keys(costsMap).length === 0) {
        log('[Credits] API returned empty action_costs array, using defaults');
        creditsState.actionCosts = getDefaultActionCosts();
      } else {
        // Merge: defaults as fallback, backend values take priority
        creditsState.actionCosts = { ...getDefaultActionCosts(), ...costsMap };
        log('[Credits] Action costs loaded:', Object.keys(costsMap).length, 'keys from backend +', Object.keys(creditsState.actionCosts).length, 'total with defaults');
      }
    } else if (data.costs && Object.keys(data.costs).length > 0) {
      // Handle old object format (backward compatibility)
      creditsState.actionCosts = data.costs;
      log('[Credits] Action costs from legacy format:', Object.keys(data.costs).length, 'keys');
    } else {
      // Fallback to defaults if API returns empty or unexpected format
      creditsState.actionCosts = getDefaultActionCosts();
      log('[Credits] Using default action costs (API returned empty or unexpected format)');
      log('[Credits] API response was:', data);
    }

    return creditsState.actionCosts;
  } catch (err) {
    log('[Credits] Action costs fetch error:', err);
    creditsState.actionCosts = getDefaultActionCosts();
    creditsState.error = err.message;
    return creditsState.actionCosts;
  }
}

/**
 * Default action costs (fallback if API unavailable)
 *
 * CANONICAL ACTION KEYS (use these in new code):
 * - image_generate       (10c) - Standard AI image
 * - image_generate_2k    (15c) - 2K resolution AI image
 * - image_generate_4k    (20c) - 4K resolution AI image
 * - text_to_3d_generate  (20c) - Text to 3D preview generation
 * - image_to_3d_generate (30c) - Image to 3D conversion
 * - refine               (10c) - Refine/upscale 3D model
 * - remesh               (10c) - Remesh 3D model (same cost as refine)
 * - retexture            (15c) - Apply new texture to 3D model
 * - video_generate       (75c) - Generic video generation (minimum, varies by duration/resolution)
 * - video_text_generate  (75c) - Text-to-video generation (minimum)
 * - video_image_animate  (110c) - Image-to-video animation (minimum)
 *
 * VIDEO PRICING (DB-driven via video_credit_rules):
 * - 720p:  4s=75, 6s=100, 8s=125
 * - 1080p: 8s=150 (requires 8s duration)
 * - 4K:    8s=200 (requires 8s duration)
 */
function getDefaultActionCosts() {
  return {
    // === CANONICAL ACTION KEYS ===
    'image_generate': 10,         // Standard AI image
    'image_generate_2k': 15,      // 2K resolution
    'image_generate_4k': 20,      // 4K resolution
    'text_to_3d_generate': 20,    // Text to 3D preview
    'image_to_3d_generate': 30,   // Image to 3D
    'refine': 10,                 // Refine 3D model
    'remesh': 10,                 // Remesh 3D model
    'retexture': 15,              // Retexture 3D model
    'video_generate': 75,         // Video generation (minimum - actual cost from video_credit_rules)
    'video_text_generate': 75,    // Text to video (minimum)
    'video_image_animate': 110,   // Image to video (minimum)

    // === LEGACY ALIASES (backwards compatibility) ===
    // Hyphenated variants
    'text-to-3d': 20,
    'image-to-3d': 30,
    'text-to-image': 10,

    // Old naming
    'preview': 20,                // -> text_to_3d_generate
    'texture': 15,                // -> retexture
    'upscale': 10,                // -> refine
    'video': 75,                  // -> video_generate (minimum)
    'image_studio_generate': 10,  // -> image_generate

    // Backend DB action codes (for direct lookups)
    'MESHY_TEXT_TO_3D': 20,
    'MESHY_IMAGE_TO_3D': 30,
    'MESHY_RETEXTURE': 15,
    'MESHY_REFINE': 10,
    'OPENAI_IMAGE': 10,
    'OPENAI_IMAGE_2K': 15,
    'OPENAI_IMAGE_4K': 20,
    'GEMINI_IMAGE': 10,
    'GEMINI_IMAGE_2K': 15,
    'GEMINI_IMAGE_4K': 20,
    'VIDEO_GENERATE': 75,         // Minimum video cost
    'VIDEO_TEXT_GENERATE': 75,
    'VIDEO_IMAGE_ANIMATE': 110,
  };
}

/**
 * Initialize credits - fetch wallet and action costs
 * Idempotent: safe to call multiple times, will only run once
 */
export async function initCredits() {
  // Guard: already initialized
  if (creditsState.loaded) {
    log('[Credits] Already initialized, skipping...');
    return;
  }

  // Guard: currently loading (prevent concurrent calls)
  if (creditsState.loading) {
    log('[Credits] Already loading, skipping...');
    return;
  }

  creditsState.loading = true;
  creditsState.error = null;

  try {
    // Fetch both in parallel
    await Promise.all([
      fetchWallet(),
      fetchActionCosts(),
    ]);

    creditsState.loaded = true;
    log('[Credits] Initialization complete');

    // Update any UI elements
    updateCreditsUI();

    // Setup batch count listeners for dynamic cost updates
    setupBatchCountListeners();

  } catch (err) {
    log('[Credits] Initialization error:', err);
    creditsState.error = err.message;
  } finally {
    creditsState.loading = false;
  }
}

// ============================================================================
// CREDIT CHECKS
// ============================================================================

// Track which actions we've already warned about (avoid log spam)
const _warnedActions = new Set();

/**
 * Resolve cost for an action key, trying multiple aliases.
 * Returns null if action is not found (distinct from 0 which means free).
 *
 * @param {string} action - The action key (e.g., 'text-to-3d', 'refine')
 * @returns {number|null} - Cost in credits, or null if unknown
 */
export function resolveCost(action) {
  if (!action) return null;

  // Direct lookup
  if (action in creditsState.actionCosts) {
    return creditsState.actionCosts[action];
  }

  // Try common aliases (hyphen <-> underscore)
  const underscore = action.replace(/-/g, '_');
  const hyphen = action.replace(/_/g, '-');

  if (underscore !== action && underscore in creditsState.actionCosts) {
    return creditsState.actionCosts[underscore];
  }
  if (hyphen !== action && hyphen in creditsState.actionCosts) {
    return creditsState.actionCosts[hyphen];
  }

  // Try with common suffixes
  const withGenerate = `${action}_generate`;
  if (withGenerate in creditsState.actionCosts) {
    return creditsState.actionCosts[withGenerate];
  }

  // Not found - log warning once per action key
  if (!_warnedActions.has(action) && creditsState.loaded) {
    _warnedActions.add(action);
    console.warn(`[Credits] Unknown action: "${action}". Available keys:`, Object.keys(creditsState.actionCosts).join(', '));
  }

  return null;
}

/**
 * Get cost for a specific action.
 * Returns 0 for unknown actions (backward compatible) - use resolveCost() for nullable result.
 *
 * @param {string} action - The action key
 * @returns {number} - Cost in credits (0 if unknown)
 */
export function getActionCost(action) {
  const cost = resolveCost(action);
  return cost !== null ? cost : 0;
}

/**
 * Check if user has enough credits for an action
 */
export function hasCreditsFor(action) {
  const cost = getActionCost(action);
  return creditsState.wallet.available >= cost;
}

/**
 * Get available credits
 */
export function getAvailableCredits() {
  return creditsState.wallet.available;
}

/**
 * Get wallet state (includes both general and video credits)
 */
export function getWallet() {
  return { ...creditsState.wallet };
}

/**
 * Get general credits wallet only (without video credits)
 */
export function getGeneralWallet() {
  return {
    balance: creditsState.wallet.balance,
    reserved: creditsState.wallet.reserved,
    available: creditsState.wallet.available,
  };
}

/**
 * Get all action costs
 */
export function getActionCosts() {
  return { ...creditsState.actionCosts };
}

/**
 * Check if credits system is loaded
 */
export function isLoaded() {
  return creditsState.loaded;
}

// ============================================================================
// VIDEO CREDITS - Separate pool for video generation
// ============================================================================

/**
 * Get available video credits
 */
export function getVideoCredits() {
  return creditsState.wallet.videoAvailable;
}

/**
 * Get video wallet state
 */
export function getVideoWallet() {
  return {
    balance: creditsState.wallet.videoBalance,
    reserved: creditsState.wallet.videoReserved,
    available: creditsState.wallet.videoAvailable,
  };
}

/**
 * Check if user has enough video credits for a specific cost
 * @param {number} cost - Required video credits
 * @returns {boolean}
 */
export function hasVideoCredits(cost) {
  return creditsState.wallet.videoAvailable >= cost;
}

/**
 * Check if an action is a video action (uses video credits pool)
 * @param {string} action - Action key
 * @returns {boolean}
 */
export function isVideoAction(action) {
  if (!action) return false;
  const normalizedAction = action.toLowerCase().replace(/-/g, '_');
  return normalizedAction.includes('video') ||
         normalizedAction === 'text2video' ||
         normalizedAction === 'image2video';
}

/**
 * Build video action code from task, duration, and resolution.
 * Format: VIDEO_TEXT_GENERATE_4S_720P or VIDEO_IMAGE_ANIMATE_8S_4K
 * @param {string} task - "text2video" or "image2video"
 * @param {number} durationSeconds - 4, 6, or 8
 * @param {string} resolution - "720p", "1080p", or "4k"
 * @returns {string} Action code
 */
export function getVideoActionCode(task, durationSeconds, resolution, provider) {
  // Use lowercase snake_case as canonical format
  const taskPart = task === 'text2video' ? 'text_generate' : 'image_animate';
  const durationPart = `${durationSeconds}s`;

  // fal Seedance: fal_seedance_{task}_{duration}s
  if (provider === 'fal_seedance') {
    return `fal_seedance_${taskPart}_${durationPart}`;
  }

  // PiAPI Seedance: handled by caller with tier prefix (seedance_{tier}_{task}_{dur}s)
  // Vertex/Veo: video_{task}_{dur}s_{res}
  const resPart = resolution.toLowerCase();
  return `video_${taskPart}_${durationPart}_${resPart}`;
}

/**
 * Get video credit cost by duration and resolution.
 * Looks up from backend-fetched action costs, falls back to hardcoded defaults.
 * @param {string} task - "text2video" or "image2video"
 * @param {number} durationSeconds - 4, 6, or 8
 * @param {string} resolution - "720p", "1080p", or "4k"
 * @returns {number} Credit cost
 */
export function getVideoCreditCost(task, durationSeconds, resolution) {
  // Build the action code
  const actionCode = getVideoActionCode(task, durationSeconds, resolution);

  // Try to find in action costs (both uppercase and lowercase)
  const cost = resolveCost(actionCode) || resolveCost(actionCode.toLowerCase());

  if (cost !== null && cost > 0) {
    return cost;
  }

  // Fallback to hardcoded defaults (must match backend pricing_service.py)
  const FALLBACK_TEXT = {
    '720p': { 4: 75, 6: 100, 8: 125 },
    '1080p': { 8: 150 },
    '4k': { 8: 200 },
  };
  const FALLBACK_IMAGE = {
    '720p': { 4: 110, 6: 140, 8: 170 },
    '1080p': { 8: 200 },
    '4k': { 8: 250 },
  };

  const isImageTask = task && task.toLowerCase() !== 'text2video';
  const fallback = isImageTask ? FALLBACK_IMAGE : FALLBACK_TEXT;
  const resLower = resolution.toLowerCase();
  const dur = parseInt(durationSeconds, 10);

  if (fallback[resLower] && fallback[resLower][dur] !== undefined) {
    console.warn(`[Credits] Using fallback cost for ${actionCode}: ${fallback[resLower][dur]}`);
    return fallback[resLower][dur];
  }

  // Ultimate fallback
  console.warn(`[Credits] No cost found for ${actionCode}, defaulting to ${isImageTask ? 110 : 75}`);
  return isImageTask ? 110 : 75;
}

/**
 * Show insufficient video credits modal
 * @param {number} required - Credits required
 * @param {number} available - Credits available (optional, uses current state if not provided)
 */
export function showInsufficientVideoCreditsMessage(required, available = null) {
  const actualAvailable = available !== null ? available : creditsState.wallet.videoAvailable;
  const needed = Math.max(0, required - actualAvailable);

  log('[Credits] Insufficient video credits:', { required, available: actualAvailable, needed });

  // Create modal HTML
  const modalId = 'insufficient-video-credits-modal';

  // Remove existing modal if any
  const existingModal = document.getElementById(modalId);
  if (existingModal) {
    existingModal.remove();
  }

  const modal = document.createElement('div');
  modal.id = modalId;
  modal.className = 'modal show';
  modal.style.cssText = 'position:fixed;inset:0;z-index:999999;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.6);opacity:1;visibility:visible;';

  modal.innerHTML = `
    <div class="modal-backdrop" style="position:absolute;inset:0;cursor:pointer;"></div>
    <div class="modal-dialog" style="position:relative;z-index:1;background:var(--surface-elevated, #1e1e2e);border-radius:12px;padding:24px;max-width:400px;width:90%;box-shadow:0 8px 32px rgba(0,0,0,0.4);">
      <div class="modal-header" style="margin-bottom:16px;">
        <h3 style="margin:0;color:var(--text-primary, #fff);font-size:1.25rem;display:flex;align-items:center;gap:8px;">
          <i class="fa-solid fa-video" style="color:var(--accent-warning, #f59e0b);"></i>
          Video Credits Required
        </h3>
      </div>
      <div class="modal-body" style="color:var(--text-secondary, #a0a0b0);margin-bottom:20px;">
        <p style="margin:0 0 12px 0;">
          Video generation uses <strong style="color:var(--accent-warning, #f59e0b);">video credits</strong>,
          which are separate from your general credits.
        </p>
        <div style="background:var(--surface-base, #14141f);border-radius:8px;padding:12px;margin:12px 0;">
          <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
            <span>Required:</span>
            <span style="color:var(--text-primary, #fff);font-weight:600;">${required} video credits</span>
          </div>
          <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
            <span>Available:</span>
            <span style="color:var(--text-primary, #fff);">${actualAvailable} video credits</span>
          </div>
          <div style="display:flex;justify-content:space-between;border-top:1px solid var(--border-subtle, #2a2a3a);padding-top:8px;margin-top:8px;">
            <span>Need:</span>
            <span style="color:var(--accent-error, #ef4444);font-weight:600;">${needed} more</span>
          </div>
        </div>
      </div>
      <div class="modal-footer" style="display:flex;gap:12px;justify-content:flex-end;">
        <button class="btn btn-secondary" id="video-credits-modal-cancel" style="padding:10px 20px;border-radius:8px;border:1px solid var(--border-default, #3a3a4a);background:transparent;color:var(--text-primary, #fff);cursor:pointer;">
          Cancel
        </button>
        <button class="btn btn-primary" id="video-credits-modal-buy" style="padding:10px 20px;border-radius:8px;border:none;background:linear-gradient(135deg, #8b5cf6, #6366f1);color:#fff;cursor:pointer;font-weight:600;">
          <i class="fa-solid fa-coins" style="margin-right:6px;"></i>
          Buy Video Credits
        </button>
      </div>
    </div>
  `;

  document.body.appendChild(modal);

  // Event handlers
  const closeModal = () => modal.remove();

  modal.querySelector('.modal-backdrop').addEventListener('click', closeModal);
  modal.querySelector('#video-credits-modal-cancel').addEventListener('click', closeModal);
  modal.querySelector('#video-credits-modal-buy').addEventListener('click', () => {
    closeModal();
    // Navigate to hub pricing section for video credits
    window.location.href = 'hub.html#pricing';
  });

  // Close on Escape key
  const handleEscape = (e) => {
    if (e.key === 'Escape') {
      closeModal();
      document.removeEventListener('keydown', handleEscape);
    }
  };
  document.addEventListener('keydown', handleEscape);
}

// ============================================================================
// OPTIMISTIC UPDATES
// ============================================================================

/**
 * Generate unique ID for tracking pending deductions
 */
function generateDeductionId() {
  return `deduct_${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;
}

/**
 * Deduct credits optimistically (immediately update UI before API call)
 *
 * @param {string} action - The action key (e.g., 'image_to_3d', 'text_to_3d')
 * @param {number} count - Number of items (default 1, for batch operations)
 * @returns {object} - { id, amount } to use for reconcile/rollback
 */
export function deductOptimistic(action, count = 1) {
  const costPerItem = getActionCost(action);
  const totalCost = costPerItem * count;

  if (totalCost === 0) {
    log('[Credits] Optimistic deduct: action has no cost', action);
    return { id: null, amount: 0 };
  }

  const deductionId = generateDeductionId();
  const deduction = {
    id: deductionId,
    amount: totalCost,
    action,
    count,
    timestamp: Date.now(),
  };

  // Track this pending deduction
  creditsState.pendingDeductions.push(deduction);

  // Store current balance as "last server balance" if not already set
  if (creditsState.lastServerBalance === null) {
    creditsState.lastServerBalance = creditsState.wallet.available;
  }

  // Optimistically reduce available credits
  creditsState.wallet.available = Math.max(0, creditsState.wallet.available - totalCost);
  creditsState.wallet.balance = Math.max(0, creditsState.wallet.balance - totalCost);

  log('[Credits] Optimistic deduct:', {
    id: deductionId,
    action,
    cost: totalCost,
    newAvailable: creditsState.wallet.available,
  });

  // Update UI immediately
  updateCreditsUI();

  return { id: deductionId, amount: totalCost };
}

/**
 * Reconcile local state with server balance (call after API response)
 *
 * @param {number} serverBalance - The actual balance from server response
 * @param {string} deductionId - Optional: specific deduction to clear
 */
export function reconcile(serverBalance, deductionId = null) {
  log('[Credits] Reconciling with server balance:', serverBalance);

  // Clear specific deduction or all pending deductions
  if (deductionId) {
    creditsState.pendingDeductions = creditsState.pendingDeductions.filter(
      d => d.id !== deductionId
    );
  } else {
    // Clear all pending deductions on full reconcile
    creditsState.pendingDeductions = [];
  }

  // Update to server truth
  creditsState.wallet.available = serverBalance;
  creditsState.wallet.balance = serverBalance;
  creditsState.lastServerBalance = serverBalance;

  // Cache for next page load
  cacheCreditsBalance(serverBalance);

  log('[Credits] Reconciled:', {
    balance: serverBalance,
    pendingCount: creditsState.pendingDeductions.length,
  });

  // Update UI with server truth
  updateCreditsUI();
}

/**
 * Rollback a pending deduction (call if API call fails)
 *
 * @param {string} deductionId - The deduction ID from deductOptimistic
 */
export function rollback(deductionId) {
  const deductionIndex = creditsState.pendingDeductions.findIndex(
    d => d.id === deductionId
  );

  if (deductionIndex === -1) {
    log('[Credits] Rollback: deduction not found', deductionId);
    return;
  }

  const deduction = creditsState.pendingDeductions[deductionIndex];

  // Remove from pending
  creditsState.pendingDeductions.splice(deductionIndex, 1);

  // Restore credits
  creditsState.wallet.available += deduction.amount;
  creditsState.wallet.balance += deduction.amount;

  log('[Credits] Rolled back:', {
    id: deductionId,
    amount: deduction.amount,
    newAvailable: creditsState.wallet.available,
  });

  // Update UI
  updateCreditsUI();
}

/**
 * Clear all pending deductions (useful on page refresh or error recovery)
 */
export function clearPending() {
  creditsState.pendingDeductions = [];
  log('[Credits] Cleared all pending deductions');
}

/**
 * Get total pending deductions amount
 */
export function getPendingAmount() {
  return creditsState.pendingDeductions.reduce((sum, d) => sum + d.amount, 0);
}

// ============================================================================
// CREDIT RESERVATIONS (hold credits during generation)
// ============================================================================

/**
 * Reserve credits for a pending operation.
 * Shows "Reserving credits..." state and immediately reduces available.
 *
 * @param {string} action - The action key (e.g., 'text-to-3d', 'image-to-3d')
 * @param {number} count - Number of items (default 1, for batch operations)
 * @returns {{ reservationId: string, amount: number }} Reservation info
 */
export function reserveCredits(action, count = 1) {
  const costPerItem = getActionCost(action);
  const totalCost = costPerItem * count;

  if (totalCost === 0) {
    log('[Credits] Reserve: action has no cost', action);
    log('[Credits] Available action costs:', Object.keys(creditsState.actionCosts).join(', ') || '(empty)');
    log('[Credits] Action costs state:', creditsState.actionCosts);
    return { reservationId: null, amount: 0 };
  }

  // Check if enough credits available (accounting for existing reservations)
  const available = Number(creditsState.wallet.available) || 0;
  const reserved = Number(creditsState.totalReserved) || 0;
  const effectiveAvailable = available - reserved;
  const missing = Math.max(0, totalCost - effectiveAvailable);
  const shouldBlock = missing > 0;

  // Detailed logging for debugging credit issues
  console.log(`[CREDITS] ========================================`);
  console.log(`[CREDITS] RESERVE CREDITS CHECK (action-based)`);
  console.log(`[CREDITS] action=${action}`);
  console.log(`[CREDITS] costPerItem=${costPerItem}, count=${count}, totalCost=${totalCost}`);
  console.log(`[CREDITS] available=${available}`);
  console.log(`[CREDITS] reserved=${reserved}`);
  console.log(`[CREDITS] effectiveAvailable=${effectiveAvailable}`);
  console.log(`[CREDITS] missing=${missing}`);
  console.log(`[CREDITS] shouldBlock=${shouldBlock}`);
  console.log(`[CREDITS] ========================================`);

  if (shouldBlock) {
    log('[Credits] Reserve failed: insufficient credits', {
      action,
      cost: totalCost,
      available,
      reserved,
      effectiveAvailable,
      missing,
    });
    return { reservationId: null, amount: 0, insufficient: true, required: totalCost, available: effectiveAvailable, missing };
  }

  const reservationId = `res_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
  const reservation = {
    amount: totalCost,
    action,
    count,
    timestamp: Date.now(),
  };

  // Track reservation
  creditsState.reservations.set(reservationId, reservation);
  creditsState.totalReserved += totalCost;

  log('[Credits] Reserved:', {
    reservationId,
    action,
    amount: totalCost,
    totalReserved: creditsState.totalReserved,
    effectiveAvailable: creditsState.wallet.available - creditsState.totalReserved,
  });

  // Update UI to show reservation
  updateCreditsUI();

  return { reservationId, amount: totalCost };
}

/**
 * Reserve an EXACT amount of credits (use for pre-computed costs like video)
 * Unlike reserveCredits(action, count), this does NOT multiply by action cost.
 *
 * @param {object} params
 * @param {string} params.action - The action type (for logging/tracking)
 * @param {number} params.amount - Exact credits amount to reserve
 * @param {object} params.meta - Optional metadata
 * @returns {{ reservationId: string, amount: number, insufficient?: boolean }}
 */
export function reserveAmount({ action, amount, meta = {} }) {
  const numAmount = Number(amount) || 0;

  if (numAmount <= 0) {
    log('[Credits] reserveAmount: invalid amount', { action, amount });
    return { reservationId: null, amount: 0 };
  }

  // Check if enough credits available (accounting for existing reservations)
  const available = Number(creditsState.wallet.available) || 0;
  const reserved = Number(creditsState.totalReserved) || 0;
  const effectiveAvailable = available - reserved;
  const missing = Math.max(0, numAmount - effectiveAvailable);
  const shouldBlock = missing > 0;

  // Detailed logging for debugging credit issues
  console.log(`[CREDITS] ========================================`);
  console.log(`[CREDITS] RESERVE AMOUNT CHECK`);
  console.log(`[CREDITS] action=${action}`);
  console.log(`[CREDITS] cost=${numAmount}`);
  console.log(`[CREDITS] available=${available}`);
  console.log(`[CREDITS] reserved=${reserved}`);
  console.log(`[CREDITS] effectiveAvailable=${effectiveAvailable}`);
  console.log(`[CREDITS] missing=${missing}`);
  console.log(`[CREDITS] shouldBlock=${shouldBlock}`);
  console.log(`[CREDITS] ========================================`);

  if (shouldBlock) {
    log('[Credits] reserveAmount failed: insufficient credits', {
      action,
      cost: numAmount,
      available,
      reserved,
      effectiveAvailable,
      missing,
    });
    return { reservationId: null, amount: 0, insufficient: true, required: numAmount, available: effectiveAvailable, missing };
  }

  const reservationId = `res_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
  const reservation = {
    amount: numAmount,
    action,
    meta,
    timestamp: Date.now(),
  };

  // Track reservation
  creditsState.reservations.set(reservationId, reservation);
  creditsState.totalReserved += numAmount;

  log('[Credits] reserveAmount succeeded:', {
    reservationId,
    action,
    amount: numAmount,
    totalReserved: creditsState.totalReserved,
    effectiveAvailable: available - creditsState.totalReserved,
  });

  // Update UI to show reservation
  updateCreditsUI();

  return { reservationId, amount: numAmount };
}

/**
 * Confirm a reservation (job started successfully).
 * Converts reservation to actual deduction.
 *
 * @param {string} reservationId - The reservation ID from reserveCredits
 * @param {string} jobId - The actual job ID from backend
 */
export function confirmReservation(reservationId, jobId) {
  const reservation = creditsState.reservations.get(reservationId);
  if (!reservation) {
    log('[Credits] confirmReservation: not found', reservationId);
    return;
  }

  // Remove from reservations
  creditsState.reservations.delete(reservationId);
  creditsState.totalReserved -= reservation.amount;

  // Apply actual deduction
  applyDelta(-reservation.amount, reservation.action, jobId);

  log('[Credits] Reservation confirmed:', {
    reservationId,
    jobId,
    amount: reservation.amount,
    newBalance: creditsState.wallet.available,
  });
}

/**
 * Release a reservation (job failed to start or was cancelled).
 * Returns credits to available.
 *
 * @param {string} reservationId - The reservation ID from reserveCredits
 */
export function releaseReservation(reservationId) {
  const reservation = creditsState.reservations.get(reservationId);
  if (!reservation) {
    log('[Credits] releaseReservation: not found', reservationId);
    return;
  }

  // Remove from reservations
  creditsState.reservations.delete(reservationId);
  creditsState.totalReserved -= reservation.amount;

  log('[Credits] Reservation released:', {
    reservationId,
    amount: reservation.amount,
    totalReserved: creditsState.totalReserved,
  });

  // Update UI
  updateCreditsUI();
}

/**
 * Get total currently reserved credits
 */
export function getTotalReserved() {
  return creditsState.totalReserved;
}

/**
 * Get effective available credits (available minus reserved)
 */
export function getEffectiveAvailable() {
  return Math.max(0, creditsState.wallet.available - creditsState.totalReserved);
}

/**
 * Check if enough credits for action (accounting for reservations)
 */
export function hasEffectiveCreditsFor(action, count = 1) {
  const cost = getActionCost(action) * count;
  return getEffectiveAvailable() >= cost;
}

// ============================================================================
// WALLET STATE MANAGEMENT
// ============================================================================

/**
 * Update wallet after a successful operation (e.g., after job completion)
 */
export function updateWallet(wallet) {
  if (wallet) {
    const available = wallet.available ?? Math.max(0, (wallet.balance || 0) - (wallet.reserved || 0));
    creditsState.wallet = {
      balance: wallet.balance || 0,
      reserved: wallet.reserved || 0,
      available,
    };
    // Cache for next page load
    cacheCreditsBalance(available);
    updateCreditsUI();
    log('[Credits] Wallet updated:', creditsState.wallet);
  }
}

// ============================================================================
// EMAIL BEACON - Navbar beacon prompt to add email
// ============================================================================

/**
 * Update email beacon visibility based on email state
 * Shows beacon if: no email attached
 */
function updateEmailBeaconUI() {
  const emailBeacon = document.getElementById('emailBeacon');
  if (!emailBeacon) return;

  const shouldShow = !creditsState.email;

  if (shouldShow) {
    emailBeacon.classList.remove('hidden');
    log('[Credits] Email beacon shown - no email attached');
  } else {
    emailBeacon.classList.add('hidden');
    log('[Credits] Email beacon hidden - email attached');
  }
}

/**
 * Handle beacon click - navigate to hub secure credits section
 */
function handleBeaconClick() {
  window.location.href = 'hub.html#secure-credits';
}

// Setup email beacon event listeners on DOM ready
function setupEmailBeaconListeners() {
  const emailBeacon = document.getElementById('emailBeacon');
  emailBeacon?.addEventListener('click', handleBeaconClick);
}

// Run setup when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', setupEmailBeaconListeners);
} else {
  setupEmailBeaconListeners();
}

// ============================================================================
// UI UPDATES
// ============================================================================

/**
 * Update credits display in the workspace UI
 */
export function updateCreditsUI() {
  // Update credits pill if it exists
  const creditsPill = document.getElementById('workspaceCredits');
  const creditsValue = document.getElementById('workspaceCreditsValue');
  const creditsGroup = document.getElementById('workspaceCreditsGroup');
  const reservedIndicator = document.getElementById('workspaceCreditsReserved');

  // Calculate effective available (balance - reserved)
  const effectiveAvailable = getEffectiveAvailable();
  const hasReservations = creditsState.totalReserved > 0;

  if (creditsValue) {
    creditsValue.textContent = effectiveAvailable.toLocaleString();
  }

  // Update hover tooltip with pool breakdown
  const tooltipGeneral = document.getElementById('tooltipGeneral');
  const tooltipVideo = document.getElementById('tooltipVideo');
  if (tooltipGeneral) {
    tooltipGeneral.textContent = effectiveAvailable.toLocaleString();
  }
  if (tooltipVideo) {
    tooltipVideo.textContent = creditsState.wallet.videoAvailable.toLocaleString();
  }

  // Show/hide reserved indicator
  if (reservedIndicator) {
    if (hasReservations) {
      reservedIndicator.textContent = `(${creditsState.totalReserved} reserved)`;
      reservedIndicator.classList.remove('hidden');
    } else {
      reservedIndicator.classList.add('hidden');
    }
  }

  if (creditsPill) {
    creditsPill.classList.toggle('low', effectiveAvailable < 30 && effectiveAvailable > 0);
    creditsPill.classList.toggle('empty', effectiveAvailable === 0);
    creditsPill.classList.toggle('has-credits', effectiveAvailable > 0);
    creditsPill.classList.toggle('has-reservations', hasReservations);

    // Make credits pill clickable to show buy options
    if (!creditsPill.dataset.clickWired) {
      creditsPill.dataset.clickWired = 'true';
      creditsPill.style.cursor = 'pointer';
      creditsPill.addEventListener('click', () => {
        // Redirect to pricing section
        window.location.href = 'hub.html#pricing';
      });
    }

    // Native title removed — hover tooltip shows pool breakdown instead
    creditsPill.removeAttribute('title');
  }

  // Toggle syncing class on group and pill
  const isSyncing = creditsState.loading && !creditsState.loaded;
  const wasSyncing = creditsPill?.classList.contains('syncing');

  if (creditsGroup) {
    creditsGroup.classList.toggle('syncing', isSyncing);
  }
  if (creditsPill) {
    creditsPill.classList.toggle('syncing', isSyncing);

    // Brief "just-synced" flash when syncing completes
    if (wasSyncing && !isSyncing && creditsState.loaded) {
      creditsPill.classList.add('just-synced');
      setTimeout(() => creditsPill.classList.remove('just-synced'), 1200);
    }
  }

  // Update generate buttons with cost indicators
  updateGenerateButtonCosts();
}

/**
 * Button to action mapping with associated batch count inputs.
 *
 * CANONICAL ACTION KEYS (use these):
 * - image_generate       (10c) - All 2D image providers
 * - text_to_3d_generate  (20c) - Text to 3D preview
 * - image_to_3d_generate (30c) - Image to 3D
 * - refine               (10c) - Refine 3D model
 * - remesh               (10c) - Remesh 3D model
 * - retexture            (15c) - Retexture 3D model
 * - video_generate       (75-250c) - Video generation (varies by duration/resolution)
 * - video_text_generate  (75-200c) - Text to video
 * - video_image_animate  (110-250c) - Image to video
 */
const BUTTON_CONFIG = {
  // Core generation buttons (canonical keys)
  'generateModelBtn': { action: 'text_to_3d_generate', batchInput: 'modelBatchCount' },
  'generateImageBtn': { action: 'image_generate', batchInput: null },
  'imageTo3dBtn': { action: 'image_to_3d_generate', batchInput: null },
  // Post-processing buttons (canonical keys)
  'generateTextureBtn': { action: 'retexture', batchInput: null },
  'applyRemeshBtn': { action: 'remesh', batchInput: null },
  'applyRefineBtn': { action: 'refine', batchInput: null },
  'applyUpscaleBtn': { action: 'refine', batchInput: null },  // Upscale uses refine cost
  'generateVideoBtn': { action: 'video_generate', batchInput: null },
};

/**
 * Get batch count for a button (from associated input or default 1)
 */
function getBatchCountForButton(btnId) {
  const config = BUTTON_CONFIG[btnId];
  if (!config?.batchInput) return 1;

  const input = document.getElementById(config.batchInput);
  if (!input) return 1;

  const val = parseInt(input.value, 10);
  return Number.isFinite(val) && val > 0 ? Math.min(val, 4) : 1;
}

/**
 * Update generate buttons to show credit costs
 * Maps button IDs to action keys for cost lookup
 * Uses resolveCost() to show "—" for unknown costs instead of "0"
 */
function updateGenerateButtonCosts() {
  // Use effective available (accounting for reservations)
  const effectiveAvailable = getEffectiveAvailable();

  Object.entries(BUTTON_CONFIG).forEach(([btnId, config]) => {
    const btn = document.getElementById(btnId);
    if (!btn) return;

    // Check for dynamic action override (e.g., when switching between text-to-3d and image-to-3d tabs)
    const action = btn.dataset.currentAction || config.action;
    const batchCount = getBatchCountForButton(btnId);

    // Check for dynamic cost override from UI (e.g., video panel with duration/resolution/audio options)
    // The data-base-credits attribute is set by panel-specific JS when options change
    const dynamicCost = btn.dataset.baseCredits ? parseInt(btn.dataset.baseCredits, 10) : null;

    // Use dynamic cost if available, otherwise fall back to static action cost
    const costPerItem = dynamicCost !== null && !isNaN(dynamicCost) ? dynamicCost : resolveCost(action);
    const isUnknown = costPerItem === null;
    const totalCost = isUnknown ? 0 : costPerItem * batchCount;
    const hasCreds = isUnknown ? false : effectiveAvailable >= totalCost;

    // Find the .gen-credits span in the same footer card
    const footerCard = btn.closest('.gen-footer-card');
    if (footerCard) {
      const creditsSpan = footerCard.querySelector('.gen-credits');
      if (creditsSpan) {
        // Show "—" for unknown costs, otherwise show the cost
        if (isUnknown) {
          creditsSpan.textContent = '—';
          creditsSpan.title = `Cost unknown for action: ${action}`;
        } else {
          // Show batch multiplier if > 1
          const costText = batchCount > 1
            ? `${costPerItem} × ${batchCount} = ${totalCost}`
            : `${totalCost}`;
          creditsSpan.innerHTML = `<i class="fa-solid fa-coins"></i> ${costText}`;
          creditsSpan.classList.toggle('insufficient', !hasCreds);
        }
      }
    }

    // Add/update insufficient state on button
    btn.classList.toggle('insufficient-credits', !hasCreds);

    // Disable button when insufficient credits
    // Only manage the disabled state for credits - don't override other reasons
    const currentlyDisabledForCredits = btn.getAttribute('data-disabled-reason') === 'insufficient-credits';
    const hasOtherDisabledReason = btn.disabled && !currentlyDisabledForCredits;

    if (!hasCreds) {
      btn.setAttribute('data-disabled-reason', 'insufficient-credits');
      btn.disabled = true;
    } else if (currentlyDisabledForCredits) {
      // Only re-enable if we were the ones who disabled it
      btn.removeAttribute('data-disabled-reason');
      if (!hasOtherDisabledReason) {
        btn.disabled = false;
      }
    }

    // Update tooltip with clear message about required credits
    btn.setAttribute('data-credits', isUnknown ? '' : totalCost);
    if (isUnknown) {
      btn.setAttribute('title', `Cost unknown for action: ${action}`);
    } else if (!hasCreds) {
      // Ensure missing is never negative
      const missing = Math.max(0, totalCost - effectiveAvailable);
      btn.setAttribute('title', `You need ${totalCost} credits to generate this. (${missing} more needed)`);
    } else {
      btn.setAttribute('title', `${totalCost} credits`);
    }

    // Add cost badge to button (show "—" for unknown, cost for known)
    let costBadge = btn.querySelector('.btn-cost-badge');
    if (isUnknown || totalCost > 0) {
      if (!costBadge) {
        costBadge = document.createElement('span');
        costBadge.className = 'btn-cost-badge';
        btn.appendChild(costBadge);
      }
      if (isUnknown) {
        costBadge.textContent = '—';
        costBadge.classList.add('unknown');
        costBadge.classList.remove('insufficient', 'has-batch');
      } else {
        // Show batch multiplier in badge if > 1
        costBadge.textContent = batchCount > 1 ? `${totalCost}` : totalCost;
        costBadge.classList.toggle('insufficient', !hasCreds);
        costBadge.classList.toggle('has-batch', batchCount > 1);
        costBadge.classList.remove('unknown');
      }
    } else if (costBadge) {
      costBadge.remove();
    }
  });
}

/**
 * Setup batch count input listeners to update costs dynamically
 */
function setupBatchCountListeners() {
  // Find all batch count inputs
  const batchInputIds = [...new Set(
    Object.values(BUTTON_CONFIG)
      .map(c => c.batchInput)
      .filter(Boolean)
  )];

  batchInputIds.forEach(inputId => {
    const input = document.getElementById(inputId);
    if (!input) return;

    // Update costs when value changes
    const updateHandler = () => {
      log('[Credits] Batch count changed:', inputId, input.value);
      updateGenerateButtonCosts();
    };

    input.addEventListener('input', updateHandler);
    input.addEventListener('change', updateHandler);

    // Note: Stepper buttons are handled by 3dprint-app.js which dispatches
    // change events that we listen to above. No duplicate handlers needed.
  });

  log('[Credits] Batch count listeners setup for:', batchInputIds);
}

/**
 * Show insufficient credits message and redirect to pricing
 */
export function showInsufficientCreditsMessage(action) {
  const cost = getActionCost(action);
  const available = creditsState.wallet.available;
  const needed = Math.max(0, cost - available);

  log('[Credits] Insufficient credits:', { action, cost, available, needed });

  // Check if we're on hub.html with the buy modal
  const hubBuyModal = document.getElementById('buyCreditsModal');
  if (hubBuyModal && window.TimrXCredits?.openModal) {
    window.TimrXCredits.openModal();
    return;
  }

  // Simple confirm dialog and redirect to pricing
  const msg = `Insufficient credits.\n\nYou need ${cost} credits for this action but only have ${available} available.\nYou need ${needed} more credits.`;
  if (confirm(msg + '\n\nWould you like to buy more credits?')) {
    window.location.href = 'hub.html#pricing';
  }
}

// ============================================================================
// SIMPLE CLIENT API (credits-client interface)
// ============================================================================

/**
 * Initialize credits UI - loads current credits from backend and renders
 * Alias for initCredits() with a clearer name
 */
export async function initCreditsUI() {
  return initCredits();
}

/**
 * Get current cached numeric balance
 * @returns {number} Current available credits
 */
export function getCredits() {
  return creditsState.wallet.available;
}

/**
 * Set credits balance directly and update UI
 * @param {number} n - New balance to set
 */
export function setCredits(n) {
  const balance = Math.max(0, Math.floor(n));
  creditsState.wallet.available = balance;
  creditsState.wallet.balance = balance;
  creditsState.lastServerBalance = balance;
  // Cache for next page load
  cacheCreditsBalance(balance);
  log('[Credits] setCredits:', balance);
  updateCreditsUI();
}

/**
 * Apply a delta (positive or negative) to credits with tracking
 * Used for optimistic updates with reason/job tracking
 *
 * @param {number} delta - Amount to add (positive) or subtract (negative)
 * @param {string} reason - Reason for the change (e.g., 'text_to_3d', 'purchase')
 * @param {string} jobId - Optional job ID for idempotency tracking
 * @returns {{ id: string, previousBalance: number, newBalance: number }}
 */
export function applyDelta(delta, reason = 'unknown', jobId = null) {
  // Idempotency: skip if this jobId was already charged (prevents double-click/retry duplicates)
  if (jobId && delta < 0 && chargedJobs.has(jobId)) {
    log('[Credits] applyDelta: skipping duplicate charge for jobId:', jobId);
    return {
      id: jobId,
      previousBalance: creditsState.wallet.available,
      newBalance: creditsState.wallet.available,
      skipped: true,
    };
  }

  const previousBalance = creditsState.wallet.available;
  const deductionId = jobId || `delta_${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;

  // Track this change
  const change = {
    id: deductionId,
    amount: Math.abs(delta),
    delta,
    reason,
    jobId,
    timestamp: Date.now(),
  };

  if (delta < 0) {
    // Deduction - track as pending
    creditsState.pendingDeductions.push(change);
    // Mark this jobId as charged for idempotency
    if (jobId) {
      chargedJobs.add(jobId);
    }
  }

  // Store last server balance if not set
  if (creditsState.lastServerBalance === null) {
    creditsState.lastServerBalance = previousBalance;
  }

  // Apply delta
  const newBalance = Math.max(0, previousBalance + delta);
  creditsState.wallet.available = newBalance;
  creditsState.wallet.balance = newBalance;

  log('[Credits] applyDelta:', {
    id: deductionId,
    delta,
    reason,
    jobId,
    balance: `${previousBalance} → ${newBalance}`,
  });

  // Update UI immediately
  updateCreditsUI();

  return {
    id: deductionId,
    previousBalance,
    newBalance,
  };
}

/**
 * Refresh credits from server - calls GET /api/credits/wallet
 * Single-flight: returns existing promise if already in flight
 * Sets exact server balance, clearing any optimistic state
 * @returns {Promise<number>} The server balance
 */
export async function refreshCredits() {
  // Single-flight guard: return existing promise if already refreshing
  if (refreshInFlight) {
    log('[Credits] refreshCredits already in flight, returning existing promise');
    return refreshInFlight;
  }

  const url = `${BACKEND}/api/credits/wallet`;
  log('[Credits] Refreshing from:', url);

  // Show syncing indicator
  creditsState.loading = true;
  updateCreditsUI();

  refreshInFlight = (async () => {
    try {
      const result = await apiFetch('/api/credits/wallet', {
        cache: 'no-store',
        keepalive: true,
      });

      if (!result.ok) {
        log('[Credits] refreshCredits failed:', result.status, result.error);
        pendingRetry = true;
        // Fall back to /api/me
        return fetchWallet().then(() => creditsState.wallet.available);
      }

      const data = result.data;
      log('[Credits] /api/credits/wallet response:', data);

      if (data.ok && typeof data.credits_balance === 'number') {
        const serverBalance = data.credits_balance;
        const serverReserved = data.reserved_credits || 0;
        const serverAvailable = typeof data.available_credits === 'number'
          ? data.available_credits
          : Math.max(0, serverBalance - serverReserved);

        // Video credits (separate pool)
        const videoBalance = data.video_credits_balance ?? 0;
        const videoReserved = data.video_reserved_credits ?? 0;
        const videoAvailable = typeof data.video_available_credits === 'number'
          ? data.video_available_credits
          : Math.max(0, videoBalance - videoReserved);

        // Server is truth - use server's available (accounts for backend reservations)
        creditsState.pendingDeductions = [];
        creditsState.wallet.balance = serverBalance;
        creditsState.wallet.reserved = serverReserved;
        creditsState.wallet.available = serverAvailable;
        creditsState.wallet.videoBalance = videoBalance;
        creditsState.wallet.videoReserved = videoReserved;
        creditsState.wallet.videoAvailable = videoAvailable;
        creditsState.lastServerBalance = serverBalance;

        // Server's available_credits already accounts for all backend reservations.
        // Clear client-side reservations to avoid double-counting.
        creditsState.reservations.clear();
        creditsState.totalReserved = 0;

        if (data.identity_id) {
          creditsState.identityId = data.identity_id;
        }

        log('[Credits] Video credits: balance=%d, reserved=%d, available=%d',
            videoBalance, videoReserved, videoAvailable);

        // Cache available for next page load (not raw balance)
        cacheCreditsBalance(serverAvailable, videoAvailable);

        // Also write to cross-page wallet cache
        if (data.identity_id) {
          writeWalletCache(data.identity_id, serverAvailable);
        }

        pendingRetry = false; // Clear retry flag on success
        lastRefreshTime = Date.now(); // Track for visibility throttling

        // Update global session info
        updateSessionInfo({ ok: true, identity_id: data.identity_id, available_credits: serverAvailable }, 'workspace');

        log('[Credits] Refreshed from server: balance=%d, reserved=%d, available=%d',
            serverBalance, serverReserved, serverAvailable);
        updateCreditsUI();
        return serverAvailable;
      }

      // Fallback to /api/me if response format unexpected
      return fetchWallet().then(() => creditsState.wallet.available);
    } catch (err) {
      log('[Credits] refreshCredits error:', err.message);
      // Keep cached balance on timeout, schedule retry on focus
      pendingRetry = true;
      return creditsState.wallet.available;
    } finally {
      // Hide syncing indicator and clear single-flight guard
      creditsState.loading = false;
      refreshInFlight = null;
      updateCreditsUI();
    }
  })();

  return refreshInFlight;
}

// ============================================================================
// BACKEND SYNC HELPERS
// ============================================================================

/**
 * Force sync with backend - ALWAYS trusts backend over local state.
 * Use this after job completion (success or failure) to ensure UI matches DB.
 *
 * If backend returns a different balance than local, backend wins.
 * This prevents "snap back" issues where optimistic updates diverge from reality.
 *
 * @returns {Promise<number>} The authoritative server balance
 */
export async function syncWithBackend() {
  log('[Credits] syncWithBackend: Forcing reconciliation with backend (backend is truth)');

  // Clear any pending deductions - we're about to get authoritative balance
  creditsState.pendingDeductions = [];

  // Refresh from server - refreshCredits already treats server as truth
  const serverBalance = await refreshCredits();

  log('[Credits] syncWithBackend: Authoritative balance from backend:', serverBalance);
  return serverBalance;
}

/**
 * Apply backend balance immediately if returned in API response.
 * Call this whenever an API response includes new_balance.
 *
 * @param {number} newBalance - The new_balance from backend response
 * @param {string} source - Where this balance came from (for logging)
 */
export function applyBackendBalance(newBalance, source = 'api_response') {
  if (typeof newBalance !== 'number' || isNaN(newBalance)) {
    log('[Credits] applyBackendBalance: Invalid balance, ignoring:', newBalance);
    return;
  }

  const previousBalance = creditsState.wallet.available;
  const balance = Math.max(0, Math.floor(newBalance));

  // Clear pending deductions and client-side reservations - backend balance is authoritative
  creditsState.pendingDeductions = [];
  creditsState.reservations.clear();
  creditsState.totalReserved = 0;

  // Apply backend balance
  creditsState.wallet.available = balance;
  creditsState.wallet.balance = balance;
  creditsState.lastServerBalance = balance;

  // Cache for next page load
  cacheCreditsBalance(balance);

  log(`[Credits] applyBackendBalance (${source}): ${previousBalance} → ${balance} (backend is truth)`);
  updateCreditsUI();
}

// ============================================================================
// IDEMPOTENCY HELPERS
// ============================================================================

/**
 * Clear charged jobs set (useful for testing or session reset)
 */
export function clearChargedJobs() {
  chargedJobs.clear();
  log('[Credits] Cleared chargedJobs set');
}

/**
 * Check if a job ID has already been charged
 */
export function isJobCharged(jobId) {
  return chargedJobs.has(jobId);
}

// ============================================================================
// EXPORTS FOR GLOBAL ACCESS
// ============================================================================

/**
 * Get the current identity ID (for debugging)
 */
export function getIdentityId() {
  return creditsState.identityId;
}

/**
 * Check if the current user can download assets.
 * Requires: wallet loaded AND (general credits > 0 OR video credits > 0).
 * Unauthenticated/zero-credit users are blocked.
 */
export function canDownloadAssets() {
  if (!creditsState.loaded) return false;
  const totalAvailable = (creditsState.wallet.available || 0)
    + (creditsState.wallet.videoAvailable || 0);
  return totalAvailable > 0;
}

// Expose globally for backward compatibility and cross-module access
window.WorkspaceCredits = {
  // Original API
  init: initCredits,
  refresh: fetchWallet,
  getWallet,
  getAvailableCredits,
  getActionCost,
  resolveCost,  // New: returns null for unknown actions (vs 0)
  getActionCosts,
  hasCreditsFor,
  updateWallet,
  updateUI: updateCreditsUI,
  updateButtonCosts: updateGenerateButtonCosts,
  setupBatchListeners: setupBatchCountListeners,
  showInsufficientCreditsMessage,
  isLoaded,
  getIdentityId,
  canDownloadAssets,
  // Video credits API (separate pool)
  getVideoCredits,
  getVideoWallet,
  hasVideoCredits,
  isVideoAction,
  showInsufficientVideoCreditsMessage,
  // Video variant costs (backend-driven)
  getVideoActionCode,
  getVideoCreditCost,
  // Optimistic update functions
  deductOptimistic,
  reconcile,
  rollback,
  clearPending,
  getPendingAmount,
  // Reservation functions (hold credits during generation)
  reserveCredits,
  reserveAmount,
  confirmReservation,
  releaseReservation,
  getTotalReserved,
  getEffectiveAvailable,
  hasEffectiveCreditsFor,
  // Simple client API (credits-client interface)
  initCreditsUI,
  getCredits,
  setCredits,
  applyDelta,
  refreshCredits,
  // Backend sync (force reconciliation - backend is truth)
  syncWithBackend,
  applyBackendBalance,
  // Idempotency helpers
  clearChargedJobs,
  isJobCharged,
  // Early render (for external use if needed)
  renderCachedCreditsEarly,
};

// Standardized ready flag for diagnostics (workspace page)
window.__TIMRX_CREDITS_READY__ = true;
window.__TIMRX_CREDITS_PAGE__ = 'workspace';
console.log('[Credits] Workspace credits module ready');

// ============================================================================
// IMMEDIATE EXECUTION: Render cached credits ASAP
// ============================================================================

// Run early render immediately when module loads - don't wait for initCredits()
// This provides instant visual feedback using the last known balance
renderCachedCreditsEarly();

// ============================================================================
// VISIBILITY & FOCUS: Refresh credits when tab becomes visible/focused
// ============================================================================

/**
 * Refresh credits if enough time has passed since last refresh
 * Used for focus/visibility events to catch up after payments or generation
 */
function maybeRefreshOnVisibility() {
  const now = Date.now();
  const timeSinceLastRefresh = now - lastRefreshTime;

  // Skip if already refreshing or too soon
  if (refreshInFlight || walletFetchInFlight) {
    log('[Credits] Skipping visibility refresh - already in flight');
    return;
  }

  // Refresh if pending retry OR enough time has passed (to catch payments in other tabs)
  if (pendingRetry || timeSinceLastRefresh > MIN_REFRESH_INTERVAL_MS) {
    log('[Credits] Visibility/focus refresh triggered');
    pendingRetry = false;
    lastRefreshTime = now;
    refreshCredits().catch(err => {
      log('[Credits] Visibility refresh failed:', err.message);
      pendingRetry = true;
    });
  }
}

// Refresh on window focus
window.addEventListener('focus', maybeRefreshOnVisibility);

// Refresh on visibility change (tab becomes visible)
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    maybeRefreshOnVisibility();
  }
});

// ============================================================================
// CROSS-TAB SYNC: Detect wallet cache changes from hub purchases
// ============================================================================

/**
 * Listen for localStorage changes from other tabs.
 * When hub completes a purchase and writes to timrx_last_wallet,
 * this tab will detect it and refresh credits immediately.
 */
window.addEventListener('storage', (event) => {
  // Only react to wallet cache changes
  if (event.key !== 'timrx_last_wallet') return;

  log('[Credits] Cross-tab storage event detected');

  // Parse the new value
  if (event.newValue) {
    try {
      const newCache = JSON.parse(event.newValue);
      if (newCache && typeof newCache.available_credits === 'number') {
        const newCredits = newCache.available_credits;
        const currentCredits = creditsState.wallet.available;

        log('[Credits] Cross-tab wallet update:', currentCredits, '→', newCredits);

        // If credits increased (purchase in another tab), update immediately
        if (newCredits > currentCredits) {
          creditsState.wallet.available = newCredits;
          creditsState.wallet.balance = newCredits;

          // Update identity if provided
          if (newCache.identity_id) {
            creditsState.identityId = newCache.identity_id;
          }

          // Cache locally
          cacheCreditsBalance(newCredits);

          // Update UI immediately
          updateCreditsUI();

          log('[Credits] Cross-tab sync complete: credits now', newCredits);

          // Also verify with server in background (non-blocking)
          refreshCredits().catch(err => {
            log('[Credits] Background refresh after cross-tab sync failed:', err.message);
          });
        }
      }
    } catch (e) {
      log('[Credits] Failed to parse cross-tab storage value:', e.message);
    }
  }
});
