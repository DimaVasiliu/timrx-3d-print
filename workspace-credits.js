/**
 * workspace-credits.js
 * Manages credits/wallet state for the 3dprint.html workspace.
 * Fetches wallet balance and action costs on load, provides helpers for credit checks.
 */

import { BACKEND, log } from './config.js';

// ============================================================================
// STATE
// ============================================================================

const creditsState = {
  wallet: {
    balance: 0,
    reserved: 0,
    available: 0,
  },
  identityId: null,
  actionCosts: {},
  loaded: false,
  loading: false,
  error: null,
};

// ============================================================================
// API FETCHING
// ============================================================================

/**
 * Fetch wallet balance from /api/me
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
  const url = `${BACKEND}/api/me`;
  log('[Credits] Fetching wallet from:', url);

  try {
    const res = await fetch(url, {
      method: 'GET',
      credentials: 'include',
      headers: { 'Accept': 'application/json' },
    });

    if (!res.ok) {
      // Not authenticated or error - log details
      const text = await res.text().catch(() => '');
      log('[Credits] Wallet fetch failed:', res.status, text.slice(0, 200));
      creditsState.wallet = { balance: 0, reserved: 0, available: 0 };
      return creditsState.wallet;
    }

    const data = await res.json();
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

      creditsState.wallet = { balance, reserved, available };
      creditsState.identityId = data.identity_id || null;

      log('[Credits] Wallet loaded:', creditsState.wallet);
    } else {
      log('[Credits] /api/me returned ok:false');
      creditsState.wallet = { balance: 0, reserved: 0, available: 0 };
    }

    return creditsState.wallet;
  } catch (err) {
    log('[Credits] Wallet fetch error:', err);
    creditsState.wallet = { balance: 0, reserved: 0, available: 0 };
    creditsState.error = err.message;
    return creditsState.wallet;
  }
}

/**
 * Fetch action costs from /api/billing/action-costs
 */
export async function fetchActionCosts() {
  try {
    const res = await fetch(`${BACKEND}/api/billing/action-costs`, {
      method: 'GET',
      credentials: 'include',
      headers: { 'Accept': 'application/json' },
    });

    if (!res.ok) {
      log('[Credits] Action costs fetch failed:', res.status);
      // Use default costs if API fails
      creditsState.actionCosts = getDefaultActionCosts();
      return creditsState.actionCosts;
    }

    const data = await res.json();
    if (data.ok && data.costs) {
      creditsState.actionCosts = data.costs;
      log('[Credits] Action costs loaded:', creditsState.actionCosts);
    } else if (data.costs) {
      // Handle case where response is just { costs: {...} }
      creditsState.actionCosts = data.costs;
    } else {
      creditsState.actionCosts = getDefaultActionCosts();
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
 */
function getDefaultActionCosts() {
  return {
    'text-to-3d': 20,
    'image-to-3d': 30,
    'text-to-image': 12,
    'refine': 10,
    'texture': 10,
    'remesh': 5,
    'rig': 10,
  };
}

/**
 * Initialize credits - fetch wallet and action costs
 */
export async function initCredits() {
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

/**
 * Get cost for a specific action
 */
export function getActionCost(action) {
  return creditsState.actionCosts[action] || 0;
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
 * Get wallet state
 */
export function getWallet() {
  return { ...creditsState.wallet };
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

/**
 * Update wallet after a successful operation (e.g., after job completion)
 */
export function updateWallet(wallet) {
  if (wallet) {
    creditsState.wallet = {
      balance: wallet.balance || 0,
      reserved: wallet.reserved || 0,
      available: wallet.available ?? Math.max(0, (wallet.balance || 0) - (wallet.reserved || 0)),
    };
    updateCreditsUI();
    log('[Credits] Wallet updated:', creditsState.wallet);
  }
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

  if (creditsValue) {
    creditsValue.textContent = creditsState.wallet.available.toLocaleString();
  }

  if (creditsPill) {
    const available = creditsState.wallet.available;
    creditsPill.classList.toggle('low', available < 30 && available > 0);
    creditsPill.classList.toggle('empty', available === 0);
    creditsPill.classList.toggle('has-credits', available > 0);
  }

  // Update generate buttons with cost indicators
  updateGenerateButtonCosts();
}

/**
 * Update generate buttons to show credit costs
 */
function updateGenerateButtonCosts() {
  const buttonCostMap = {
    'generateModelBtn': 'text-to-3d',
    'generateImageBtn': 'text-to-image',
    'generateTextureBtn': 'texture',
    'applyRemeshBtn': 'remesh',
    'applyRigBtn': 'rig',
  };

  Object.entries(buttonCostMap).forEach(([btnId, action]) => {
    const btn = document.getElementById(btnId);
    if (!btn) return;

    const cost = getActionCost(action);
    const hasCreds = hasCreditsFor(action);

    // Find the .gen-credits span in the same footer card
    const footerCard = btn.closest('.gen-footer-card');
    if (footerCard) {
      const creditsSpan = footerCard.querySelector('.gen-credits');
      if (creditsSpan) {
        // Update with coin icon and cost
        creditsSpan.innerHTML = `<i class="fa-solid fa-coins"></i> ${cost}`;
        creditsSpan.classList.toggle('insufficient', !hasCreds);
      }
    }

    // Add/update insufficient state on button
    btn.classList.toggle('insufficient-credits', !hasCreds);

    // Add cost badge to button if cost > 0
    let costBadge = btn.querySelector('.btn-cost-badge');
    if (cost > 0 && !costBadge) {
      costBadge = document.createElement('span');
      costBadge.className = 'btn-cost-badge';
      btn.appendChild(costBadge);
    }
    if (costBadge) {
      costBadge.textContent = cost;
      costBadge.classList.toggle('insufficient', !hasCreds);
    }
  });
}

/**
 * Show insufficient credits message
 */
export function showInsufficientCreditsMessage(action) {
  const cost = getActionCost(action);
  const available = creditsState.wallet.available;
  const needed = cost - available;

  log('[Credits] Insufficient credits:', { action, cost, available, needed });

  // Check if we're on hub.html with the buy modal
  const hubBuyModal = document.getElementById('buyCreditsModal');
  if (hubBuyModal && window.TimrXCredits?.openModal) {
    window.TimrXCredits.openModal();
    return;
  }

  // Simple confirm dialog
  const msg = `Insufficient credits.\n\nYou need ${cost} credits for this action but only have ${available} available.\nYou need ${needed} more credits.`;
  if (confirm(msg + '\n\nWould you like to buy more credits?')) {
    window.location.href = 'hub.html#pricing';
  }
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

// Expose globally for backward compatibility and cross-module access
window.WorkspaceCredits = {
  init: initCredits,
  refresh: fetchWallet,
  getWallet,
  getAvailableCredits,
  getActionCost,
  getActionCosts,
  hasCreditsFor,
  updateWallet,
  updateUI: updateCreditsUI,
  showInsufficientCreditsMessage,
  isLoaded,
  getIdentityId,
};
