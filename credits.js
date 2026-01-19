/**
 * credits.js
 * Handles credits display, wallet fetching, and buy credits modal for hub.html
 */

(function() {
  'use strict';

  // API endpoint - always use the custom domain for proper cookie handling
  const API_BASE = window.TIMRX_3D_API_BASE || 'https://3d.timrx.live';

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
      // Call POST /api/billing/checkout (Mollie)
      // Redirect URL is configured server-side via PUBLIC_BASE_URL
      const res = await fetch(`${API_BASE}/api/billing/checkout`, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({
          plan_code: selectedPlan.id,  // plan_code matches DB: starter_80, creator_300, studio_600
          email: email
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

    // Poll for wallet update - webhook may be delayed
    // Try up to 6 times, every 2 seconds (12 seconds max)
    (async function handleCheckoutSuccess() {
      const initialBalance = walletAvailable;
      let attempts = 0;
      const maxAttempts = 6;
      const pollInterval = 2000; // 2 seconds

      console.log('[Credits] Checkout success - polling for balance update, initial:', initialBalance);

      async function pollBalance() {
        attempts++;
        const wallet = await fetchWallet();
        const newBalance = wallet ? wallet.available : 0;

        console.log(`[Credits] Poll ${attempts}/${maxAttempts}: balance=${newBalance} (was ${initialBalance})`);

        // If balance increased or we've reached max attempts, show the modal
        if (newBalance > initialBalance || attempts >= maxAttempts) {
          if (newBalance > initialBalance) {
            console.log('[Credits] Balance updated! Showing success modal');
          } else {
            console.log('[Credits] Max attempts reached, showing current balance');
          }
          openSuccessModal(newBalance);
          return;
        }

        // Otherwise, schedule next poll
        setTimeout(pollBalance, pollInterval);
      }

      // Start polling after a short delay (give webhook a head start)
      setTimeout(pollBalance, 1000);
    })();
  } else if (urlParams.get('checkout') === 'cancelled') {
    // Clean URL silently
    window.history.replaceState({}, '', window.location.pathname);
  }

  // ─────────────────────────────────────────────────────────────
  // EMAIL ATTACH / VERIFY / RESTORE
  // ─────────────────────────────────────────────────────────────

  // Secure credits section DOM elements
  const secureState1 = document.getElementById('secureState1');
  const secureState2 = document.getElementById('secureState2');
  const secureState3 = document.getElementById('secureState3');
  const restorePanel = document.getElementById('restorePanel');

  const secureEmailInput = document.getElementById('secureEmail');
  const sendCodeBtn = document.getElementById('sendCodeBtn');
  const secureError = document.getElementById('secureError');
  const secureMessage = document.getElementById('secureMessage');

  const sentToEmail = document.getElementById('sentToEmail');
  const verifyCodeInput = document.getElementById('verifyCode');
  const verifyCodeBtn = document.getElementById('verifyCodeBtn');
  const verifyError = document.getElementById('verifyError');
  const verifyMessage = document.getElementById('verifyMessage');
  const resendCodeBtn = document.getElementById('resendCodeBtn');
  const changeEmailBtn = document.getElementById('changeEmailBtn');

  const verifiedEmailEl = document.getElementById('verifiedEmail');
  const changeVerifiedEmailBtn = document.getElementById('changeVerifiedEmailBtn');
  const showRestoreBtn = document.getElementById('showRestoreBtn');

  // Toggle button and collapsible card
  const secureToggleBtn = document.getElementById('secureToggleBtn');
  const secureCreditsCard = document.getElementById('secureCreditsCard');

  /**
   * Toggle the secure credits card visibility
   */
  function toggleSecureCredits() {
    if (!secureToggleBtn || !secureCreditsCard) return;

    const isExpanded = secureCreditsCard.classList.contains('expanded');

    if (isExpanded) {
      // Collapse
      secureCreditsCard.classList.remove('expanded');
      secureCreditsCard.classList.add('collapsed');
      secureToggleBtn.classList.remove('expanded');
    } else {
      // Expand
      secureCreditsCard.classList.remove('collapsed');
      secureCreditsCard.classList.add('expanded');
      secureToggleBtn.classList.add('expanded');
    }
  }

  // Toggle button event listener
  secureToggleBtn?.addEventListener('click', toggleSecureCredits);

  // Email state
  let pendingEmail = '';
  let emailVerified = false;
  let resendCooldown = 0;
  let resendTimer = null;
  let isRestoreMode = false;

  /**
   * Show secure credits section state
   * @param {1|2|3} stateNum - Which state to show
   */
  function showSecureState(stateNum) {
    if (secureState1) secureState1.style.display = stateNum === 1 ? 'block' : 'none';
    if (secureState2) secureState2.style.display = stateNum === 2 ? 'block' : 'none';
    if (secureState3) secureState3.style.display = stateNum === 3 ? 'block' : 'none';

    // Show restore panel only in state 1 (anonymous)
    if (restorePanel) {
      restorePanel.style.display = stateNum === 1 ? 'block' : 'none';
    }

    // Clear error/message when switching states
    clearSecureMessages();
  }

  function clearSecureMessages() {
    if (secureError) secureError.textContent = '';
    if (secureMessage) secureMessage.textContent = '';
    if (verifyError) verifyError.textContent = '';
    if (verifyMessage) verifyMessage.textContent = '';
  }

  function setSecureError(msg) {
    if (secureError) secureError.textContent = msg;
    if (secureMessage) secureMessage.textContent = '';
  }

  function setSecureMessage(msg) {
    if (secureMessage) secureMessage.textContent = msg;
    if (secureError) secureError.textContent = '';
  }

  function setVerifyError(msg) {
    if (verifyError) verifyError.textContent = msg;
    if (verifyMessage) verifyMessage.textContent = '';
  }

  function setVerifyMessage(msg) {
    if (verifyMessage) verifyMessage.textContent = msg;
    if (verifyError) verifyError.textContent = '';
  }

  /**
   * Update secure credits UI based on current email state
   */
  function updateSecureCreditsUI() {
    if (!secureState1) return; // Not on hub.html

    if (emailVerified && userEmail) {
      // State 3: Verified
      if (verifiedEmailEl) verifiedEmailEl.textContent = userEmail;
      showSecureState(3);
    } else if (userEmail && !emailVerified) {
      // State 2: Email attached but unverified (code sent)
      pendingEmail = userEmail;
      if (sentToEmail) sentToEmail.textContent = userEmail;
      showSecureState(2);
    } else {
      // State 1: No email
      showSecureState(1);
    }
  }

  /**
   * Send verification code to email
   */
  async function sendCode() {
    const email = secureEmailInput?.value?.trim().toLowerCase();

    if (!email) {
      setSecureError('Please enter an email address');
      return;
    }

    if (!email.includes('@') || !email.includes('.')) {
      setSecureError('Please enter a valid email address');
      return;
    }

    sendCodeBtn?.classList.add('loading');
    clearSecureMessages();

    try {
      const endpoint = isRestoreMode
        ? `${API_BASE}/api/auth/restore/request`
        : `${API_BASE}/api/auth/email/attach`;

      const res = await fetch(endpoint, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({ email })
      });

      const data = await res.json();

      if (!res.ok) {
        if (data.error?.code === 'RATE_LIMITED') {
          setSecureError(data.error.message || 'Please wait before requesting another code');
        } else {
          setSecureError(data.error?.message || 'Failed to send code');
        }
        return;
      }

      // Success - move to state 2
      pendingEmail = email;
      if (sentToEmail) sentToEmail.textContent = email;
      showSecureState(2);
      setVerifyMessage('Code sent! Check your email.');

      // Start resend cooldown
      startResendCooldown();

      // Focus code input
      verifyCodeInput?.focus();

    } catch (err) {
      console.error('[Credits] sendCode error:', err);
      setSecureError('Failed to send code. Please try again.');
    } finally {
      sendCodeBtn?.classList.remove('loading');
    }
  }

  /**
   * Verify the entered code
   */
  async function verifyCode() {
    const code = verifyCodeInput?.value?.trim();

    if (!code) {
      setVerifyError('Please enter the code');
      return;
    }

    if (code.length !== 6 || !/^\d+$/.test(code)) {
      setVerifyError('Code must be 6 digits');
      return;
    }

    verifyCodeBtn?.classList.add('loading');
    clearSecureMessages();

    try {
      const endpoint = isRestoreMode
        ? `${API_BASE}/api/auth/restore/redeem`
        : `${API_BASE}/api/auth/email/verify`;

      const res = await fetch(endpoint, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({ email: pendingEmail, code })
      });

      const data = await res.json();

      if (!res.ok) {
        if (data.error?.code === 'INVALID_CODE') {
          setVerifyError('Invalid or expired code');
        } else if (data.error?.code === 'TOO_MANY_ATTEMPTS') {
          setVerifyError('Too many attempts. Please request a new code.');
        } else if (data.error?.code === 'CODE_EXPIRED') {
          setVerifyError('Code has expired. Please request a new one.');
        } else {
          setVerifyError(data.error?.message || 'Verification failed');
        }
        return;
      }

      // Success!
      console.log('[Credits] Email verified successfully');
      userEmail = pendingEmail;
      emailVerified = true;
      isRestoreMode = false;

      // Refresh wallet to get updated state (especially for restore)
      await fetchWallet();

      // Show verified state
      if (verifiedEmailEl) verifiedEmailEl.textContent = userEmail;
      showSecureState(3);

    } catch (err) {
      console.error('[Credits] verifyCode error:', err);
      setVerifyError('Verification failed. Please try again.');
    } finally {
      verifyCodeBtn?.classList.remove('loading');
    }
  }

  /**
   * Start resend cooldown timer (60 seconds)
   */
  function startResendCooldown() {
    resendCooldown = 60;
    updateResendButton();

    if (resendTimer) clearInterval(resendTimer);

    resendTimer = setInterval(() => {
      resendCooldown--;
      updateResendButton();

      if (resendCooldown <= 0) {
        clearInterval(resendTimer);
        resendTimer = null;
      }
    }, 1000);
  }

  function updateResendButton() {
    if (!resendCodeBtn) return;

    if (resendCooldown > 0) {
      resendCodeBtn.disabled = true;
      resendCodeBtn.textContent = `Resend (${resendCooldown}s)`;
    } else {
      resendCodeBtn.disabled = false;
      resendCodeBtn.textContent = 'Resend Code';
    }
  }

  /**
   * Resend verification code
   */
  async function resendCode() {
    if (resendCooldown > 0) return;

    resendCodeBtn?.classList.add('loading');
    clearSecureMessages();

    try {
      const endpoint = isRestoreMode
        ? `${API_BASE}/api/auth/restore/request`
        : `${API_BASE}/api/auth/email/attach`;

      const res = await fetch(endpoint, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({ email: pendingEmail })
      });

      const data = await res.json();

      if (!res.ok) {
        setVerifyError(data.error?.message || 'Failed to resend code');
        return;
      }

      setVerifyMessage('New code sent!');
      startResendCooldown();

      // Clear code input
      if (verifyCodeInput) verifyCodeInput.value = '';

    } catch (err) {
      console.error('[Credits] resendCode error:', err);
      setVerifyError('Failed to resend code. Please try again.');
    } finally {
      resendCodeBtn?.classList.remove('loading');
    }
  }

  /**
   * Go back to change email
   */
  function changeEmail() {
    isRestoreMode = false;
    showSecureState(1);
    if (secureEmailInput) {
      secureEmailInput.value = pendingEmail || '';
      secureEmailInput.focus();
    }
  }

  /**
   * Switch to restore mode for existing account
   */
  function showRestoreMode() {
    isRestoreMode = true;
    // Update UI to indicate restore mode
    if (secureState1) {
      const h3 = secureState1.querySelector('h3');
      const subtitle = secureState1.querySelector('.secure-subtitle');
      if (h3) h3.textContent = 'Restore Your Account';
      if (subtitle) subtitle.textContent = 'Enter the email linked to your existing credits.';
    }
    secureEmailInput?.focus();
  }

  /**
   * Reset to attach mode (from restore mode)
   */
  function resetToAttachMode() {
    isRestoreMode = false;
    if (secureState1) {
      const h3 = secureState1.querySelector('h3');
      const subtitle = secureState1.querySelector('.secure-subtitle');
      if (h3) h3.textContent = 'Secure Your Credits';
      if (subtitle) subtitle.textContent = 'Add an email to restore your credits on any device.';
    }
  }

  // Event listeners for secure credits section
  sendCodeBtn?.addEventListener('click', sendCode);
  verifyCodeBtn?.addEventListener('click', verifyCode);
  resendCodeBtn?.addEventListener('click', resendCode);
  changeEmailBtn?.addEventListener('click', changeEmail);
  changeVerifiedEmailBtn?.addEventListener('click', () => {
    emailVerified = false;
    userEmail = '';
    changeEmail();
  });
  showRestoreBtn?.addEventListener('click', showRestoreMode);

  // Enter key handlers
  secureEmailInput?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      sendCode();
    }
  });

  verifyCodeInput?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      verifyCode();
    }
  });

  // Auto-format code input (numbers only)
  verifyCodeInput?.addEventListener('input', () => {
    if (verifyCodeInput) {
      verifyCodeInput.value = verifyCodeInput.value.replace(/\D/g, '').slice(0, 6);
    }
  });

  // ─────────────────────────────────────────────────────────────
  // UPDATED INIT: Also update secure credits UI
  // ─────────────────────────────────────────────────────────────

  // Modify fetchWallet to also update email state
  const originalFetchWallet = fetchWallet;

  // Wrap fetchWallet to update secure credits UI
  async function fetchWalletAndUpdateUI() {
    const result = await originalFetchWallet();

    // Update email state from latest /api/me response
    // (userEmail is already set in originalFetchWallet)
    // We need to track email_verified separately
    try {
      const meRes = await fetch(`${API_BASE}/api/me`, {
        method: 'GET',
        credentials: 'include',
        headers: { 'Accept': 'application/json' }
      });
      if (meRes.ok) {
        const meData = await meRes.json();
        if (meData.ok) {
          userEmail = meData.email || '';
          emailVerified = meData.email_verified || false;
          updateSecureCreditsUI();
        }
      }
    } catch (err) {
      console.warn('[Credits] Failed to update email state:', err);
    }

    return result;
  }

  // Initial secure credits UI update
  // This happens after the first fetchWallet call
  setTimeout(async () => {
    try {
      const meRes = await fetch(`${API_BASE}/api/me`, {
        method: 'GET',
        credentials: 'include',
        headers: { 'Accept': 'application/json' }
      });
      if (meRes.ok) {
        const meData = await meRes.json();
        if (meData.ok) {
          userEmail = meData.email || '';
          emailVerified = meData.email_verified || false;
          updateSecureCreditsUI();
        }
      }
    } catch (err) {
      console.warn('[Credits] Failed to get email state:', err);
      updateSecureCreditsUI(); // Still update to show state 1
    }
  }, 500);

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
    getIdentityId: () => identityId,
    getEmail: () => userEmail,
    isEmailVerified: () => emailVerified
  };

})();
