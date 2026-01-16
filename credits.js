/**
 * credits.js
 * Handles credits display, wallet fetching, and buy credits modal for hub.html
 */

(function() {
  'use strict';

  // API endpoint - prefer same-origin for cookie handling
  // Use relative path if on deployed domain, otherwise use explicit backend URL
  const DEPLOYED_DOMAINS = ['3d.timrx.live', 'timrx-3d-print-1.onrender.com'];
  const isDeployedDomain = DEPLOYED_DOMAINS.some(d => window.location.hostname.includes(d));
  const API_BASE = isDeployedDomain ? '' : (window.TIMRX_3D_API_BASE || 'https://timrx-3d-print-1.onrender.com');

  console.log('[Credits] Init - API_BASE:', API_BASE || '(same-origin)', 'hostname:', window.location.hostname);

  // Plan definitions (must match DB: starter_80, creator_300, studio_600)
  const PLANS = {
    starter_80: { name: 'Starter', credits: 80, price: 7.99 },
    creator_300: { name: 'Creator', credits: 300, price: 19.99 },
    studio_600: { name: 'Studio', credits: 600, price: 34.99 }
  };

  // DOM elements
  const creditsPill = document.getElementById('creditsPill');
  const creditsValue = document.getElementById('creditsValue');
  const buyCreditsBtn = document.getElementById('buyCreditsBtn');
  const buyCreditsModal = document.getElementById('buyCreditsModal');
  const buyCreditsClose = document.getElementById('buyCreditsClose');
  const planCards = document.querySelectorAll('.plan-card');
  const pricingCtaButtons = document.querySelectorAll('.pricing-cta');

  // Checkout section elements
  const checkoutSection = document.getElementById('checkoutSection');
  const selectedPlanDisplay = document.getElementById('selectedPlanDisplay');
  const selectedPlanName = document.getElementById('selectedPlanName');
  const selectedPlanPrice = document.getElementById('selectedPlanPrice');
  const checkoutEmail = document.getElementById('checkoutEmail');
  const checkoutError = document.getElementById('checkoutError');
  const checkoutBtn = document.getElementById('checkoutBtn');

  // Success modal elements
  const successModal = document.getElementById('paymentSuccessModal');
  const successCreditsValue = document.getElementById('successCreditsValue');
  const successCloseBtn = document.getElementById('successCloseBtn');

  // State
  let walletBalance = 0;
  let walletReserved = 0;
  let walletAvailable = 0;
  let userEmail = '';
  let identityId = null;
  let selectedPlan = null;

  // ─────────────────────────────────────────────────────────────
  // Wallet API
  // ─────────────────────────────────────────────────────────────

  /**
   * Fetch wallet/session info from /api/me
   * Response format:
   * {
   *   ok: true,
   *   identity_id: "uuid",
   *   email: null | "email@example.com",
   *   balance_credits: 100,
   *   reserved_credits: 0,
   *   available_credits: 100,
   *   ...
   * }
   */
  async function fetchWallet() {
    const url = `${API_BASE}/api/me`;
    console.log('[Credits] Fetching wallet from:', url);

    try {
      const res = await fetch(url, {
        method: 'GET',
        credentials: 'include',
        headers: { 'Accept': 'application/json' }
      });

      if (!res.ok) {
        // Log error details
        const text = await res.text().catch(() => '');
        console.warn('[Credits] Wallet fetch failed:', res.status, text.slice(0, 200));
        updateCreditsDisplay(0, 0, 0);
        return null;
      }

      const data = await res.json();
      console.log('[Credits] /api/me response:', {
        ok: data.ok,
        identity_id: data.identity_id,
        balance_credits: data.balance_credits,
        reserved_credits: data.reserved_credits,
        available_credits: data.available_credits,
        email: data.email
      });

      if (data.ok) {
        // Read credits from top-level fields (new format)
        walletBalance = data.balance_credits ?? data.wallet?.balance ?? 0;
        walletReserved = data.reserved_credits ?? data.wallet?.reserved ?? 0;
        walletAvailable = data.available_credits ?? data.wallet?.available ?? Math.max(0, walletBalance - walletReserved);
        identityId = data.identity_id || null;

        updateCreditsDisplay(walletAvailable, walletBalance, walletReserved);

        // Store email if available
        if (data.email) {
          userEmail = data.email;
          if (checkoutEmail && !checkoutEmail.value) {
            checkoutEmail.value = userEmail;
            validateCheckoutForm();
          }
        }

        return { balance: walletBalance, reserved: walletReserved, available: walletAvailable };
      } else {
        console.warn('[Credits] /api/me returned ok:false');
        updateCreditsDisplay(0, 0, 0);
        return null;
      }
    } catch (err) {
      console.error('[Credits] Failed to fetch wallet:', err);
      updateCreditsDisplay(0, 0, 0);
      return null;
    }
  }

  /**
   * Refresh credits - alias for fetchWallet with return value
   */
  async function refreshCredits() {
    const wallet = await fetchWallet();
    return wallet ? wallet.available : 0;
  }

  function updateCreditsDisplay(available, total, reserved) {
    if (!creditsValue) return;

    // Show available credits
    creditsValue.textContent = available.toLocaleString();

    // Add visual indicator if credits are low
    if (creditsPill) {
      creditsPill.classList.toggle('low', available < 30 && available > 0);
      creditsPill.classList.toggle('empty', available === 0);
      // Hide plus icon when user has credits
      creditsPill.classList.toggle('has-credits', available > 0);
    }

    console.log('[Credits] UI updated: available=' + available + ', total=' + total + ', reserved=' + reserved);
  }

  // ─────────────────────────────────────────────────────────────
  // Plan Selection
  // ─────────────────────────────────────────────────────────────

  function selectPlan(planId) {
    const plan = PLANS[planId];
    if (!plan) return;

    selectedPlan = { id: planId, ...plan };

    // Update UI - highlight selected plan card
    planCards.forEach(card => {
      const cardPlan = card.dataset.plan;
      card.classList.toggle('selected', cardPlan === planId);
    });

    // Update selected plan display
    if (selectedPlanName) selectedPlanName.textContent = plan.name;
    if (selectedPlanPrice) selectedPlanPrice.textContent = `£${plan.price.toFixed(2)}`;

    // Show checkout section
    if (checkoutSection) {
      checkoutSection.classList.add('visible');
    }

    // Focus email input if empty
    if (checkoutEmail && !checkoutEmail.value) {
      checkoutEmail.focus();
    }

    // Validate form
    validateCheckoutForm();
  }

  function clearPlanSelection() {
    selectedPlan = null;
    planCards.forEach(card => card.classList.remove('selected'));
    if (checkoutSection) checkoutSection.classList.remove('visible');
    if (selectedPlanName) selectedPlanName.textContent = '-';
    if (selectedPlanPrice) selectedPlanPrice.textContent = '-';
    if (checkoutBtn) checkoutBtn.disabled = true;
    clearCheckoutError();
  }

  // ─────────────────────────────────────────────────────────────
  // Form Validation
  // ─────���───────────────────────────────────────────────────────

  function isValidEmail(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  }

  function validateCheckoutForm() {
    const email = checkoutEmail?.value?.trim() || '';
    const isValid = selectedPlan && isValidEmail(email);

    if (checkoutBtn) {
      checkoutBtn.disabled = !isValid;
    }

    return isValid;
  }

  function showCheckoutError(message) {
    if (checkoutError) {
      checkoutError.textContent = message;
      checkoutError.style.display = 'block';
    }
  }

  function clearCheckoutError() {
    if (checkoutError) {
      checkoutError.textContent = '';
      checkoutError.style.display = 'none';
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Modal Management
  // ─────────────────────────────────────────────────────────────

  function openBuyCreditsModal(preselectedPlan = null) {
    if (!buyCreditsModal) return;

    // Reset state
    clearPlanSelection();
    clearCheckoutError();

    buyCreditsModal.classList.add('open');
    buyCreditsModal.setAttribute('aria-hidden', 'false');

    // Preselect plan if specified
    if (preselectedPlan && PLANS[preselectedPlan]) {
      selectPlan(preselectedPlan);
    }

    // Pre-fill email if we have it
    if (checkoutEmail && userEmail && !checkoutEmail.value) {
      checkoutEmail.value = userEmail;
      validateCheckoutForm();
    }

    // Focus first plan card if no preselection, otherwise focus email
    if (!preselectedPlan) {
      const firstPlan = buyCreditsModal.querySelector('.plan-card');
      if (firstPlan) firstPlan.focus();
    }
  }

  function closeBuyCreditsModal() {
    if (!buyCreditsModal) return;
    buyCreditsModal.classList.remove('open');
    buyCreditsModal.setAttribute('aria-hidden', 'true');

    // Reset state
    clearPlanSelection();
    clearCheckoutError();

    // Return focus to buy button
    if (buyCreditsBtn) buyCreditsBtn.focus();
  }

  // ─────────────────────────────────────────────────────────────
  // Success Modal
  // ─────────────────────────────────────────────────────────────

  function openSuccessModal(credits) {
    if (!successModal) return;

    // Update credits display
    if (successCreditsValue) {
      successCreditsValue.textContent = credits.toLocaleString();
    }

    successModal.classList.add('open');
    successModal.setAttribute('aria-hidden', 'false');
  }

  function closeSuccessModal() {
    if (!successModal) return;
    successModal.classList.remove('open');
    successModal.setAttribute('aria-hidden', 'true');
  }

  // ─────────────────────────────────────────────────────────────
  // Checkout Flow
  // ─────────────────────────────────────────────────────────────

  async function startCheckout() {
    if (!validateCheckoutForm()) {
      showCheckoutError('Please select a plan and enter a valid email.');
      return;
    }

    const email = checkoutEmail.value.trim();

    // Show loading state
    setCheckoutLoading(true);
    clearCheckoutError();

    try {
      // Call POST /api/billing/checkout/start
      const res = await fetch(`${API_BASE}/api/billing/checkout/start`, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({
          plan_id: selectedPlan.id,
          email: email,
          credits: selectedPlan.credits,
          amount_pence: Math.round(selectedPlan.price * 100)
        })
      });

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.error?.message || `Checkout failed (${res.status})`);
      }

      if (data.checkout_url) {
        // Redirect to Stripe checkout
        window.location.href = data.checkout_url;
      } else {
        throw new Error('No checkout URL returned');
      }

    } catch (err) {
      console.error('[Credits] Checkout failed:', err);
      showCheckoutError(err.message || 'Failed to start checkout. Please try again.');
      setCheckoutLoading(false);
    }
  }

  function setCheckoutLoading(loading) {
    if (!checkoutBtn) return;

    checkoutBtn.disabled = loading;

    const btnText = checkoutBtn.querySelector('.btn-text');
    const btnLoader = checkoutBtn.querySelector('.btn-loader');

    if (btnText) btnText.style.display = loading ? 'none' : '';
    if (btnLoader) btnLoader.style.display = loading ? 'inline-flex' : 'none';
  }

  // ─────────────────────────────────────────────────────────────
  // Event Listeners
  // ─────────────────────────────────────────────────────────────

  // Buy button opens modal
  buyCreditsBtn?.addEventListener('click', (e) => {
    e.preventDefault();
    openBuyCreditsModal();
  });

  // Credits pill click also opens modal
  creditsPill?.addEventListener('click', () => {
    openBuyCreditsModal();
  });

  // Close button
  buyCreditsClose?.addEventListener('click', (e) => {
    e.preventDefault();
    closeBuyCreditsModal();
  });

  // Backdrop click closes modal
  buyCreditsModal?.addEventListener('click', (e) => {
    if (e.target === buyCreditsModal) {
      closeBuyCreditsModal();
    }
  });

  // ESC closes modals
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      if (successModal?.classList.contains('open')) {
        closeSuccessModal();
      } else if (buyCreditsModal?.classList.contains('open')) {
        closeBuyCreditsModal();
      }
    }
  });

  // Plan card selection (in modal)
  planCards.forEach(card => {
    card.addEventListener('click', () => {
      const planId = card.dataset.plan;
      if (planId) selectPlan(planId);
    });

    // Keyboard support
    card.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        const planId = card.dataset.plan;
        if (planId) selectPlan(planId);
      }
    });
  });

  // Pricing page CTA buttons -> open modal with preselection
  pricingCtaButtons.forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      const planId = btn.dataset.plan;
      openBuyCreditsModal(planId);
    });
  });

  // Email input validation
  checkoutEmail?.addEventListener('input', () => {
    clearCheckoutError();
    validateCheckoutForm();
  });

  checkoutEmail?.addEventListener('blur', () => {
    const email = checkoutEmail.value.trim();
    if (email && !isValidEmail(email)) {
      showCheckoutError('Please enter a valid email address.');
    }
  });

  // Checkout button
  checkoutBtn?.addEventListener('click', (e) => {
    e.preventDefault();
    startCheckout();
  });

  // Enter key in email field triggers checkout
  checkoutEmail?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !checkoutBtn?.disabled) {
      e.preventDefault();
      startCheckout();
    }
  });

  // Success modal close button
  successCloseBtn?.addEventListener('click', (e) => {
    e.preventDefault();
    closeSuccessModal();
  });

  // Success modal backdrop click
  successModal?.addEventListener('click', (e) => {
    if (e.target === successModal) {
      closeSuccessModal();
    }
  });

  // ─────────────────────────────────────────────────────────────
  // Initialize
  // ─────────────────────────────────────────────────────────────

  // Fetch wallet on load
  fetchWallet();

  // Refresh wallet periodically (every 60 seconds)
  setInterval(fetchWallet, 60000);

  // Handle checkout return (check URL params)
  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.get('checkout') === 'success') {
    // Clean URL immediately
    window.history.replaceState({}, '', window.location.pathname);

    // Fetch wallet and show success modal with new balance
    (async function handleCheckoutSuccess() {
      const wallet = await fetchWallet();
      openSuccessModal(wallet ? wallet.available : 0);
    })();
  } else if (urlParams.get('checkout') === 'cancelled') {
    // Clean URL silently
    window.history.replaceState({}, '', window.location.pathname);
  }

  // Expose for external use
  window.TimrXCredits = {
    refresh: refreshCredits,
    fetchWallet: fetchWallet,
    openModal: openBuyCreditsModal,
    closeModal: closeBuyCreditsModal,
    selectPlan: selectPlan,
    getBalance: () => walletBalance,
    getReserved: () => walletReserved,
    getAvailable: () => walletAvailable,
    getIdentityId: () => identityId
  };

})();
