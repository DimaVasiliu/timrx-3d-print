/**
 * main.js
 * The entry point. Imports all modules, runs initialization logic,
 * and sets up the primary event listeners.
 */

import { byId, safe, log, onThreeReady, normalizeEpochMs, BACKEND } from './config.js';
import * as State from './state.js';
import * as Viewer from './viewer.js';
import * as UI from './ui-utils.js';
import {
  renderHistory,
  shortTitle,
  closeActiveHistoryMenu,
  closeActiveHistorySubmenu,
  openHistoryMenu,
  openHistorySubmenu,
  updateActiveHistoryMenuPosition,
  getFilteredHistory,
  getActiveHistoryMenu,
  getActiveHistorySubmenu
} from './history.js';
import * as API from './api.js';
import * as Converter from './converter.js';
import * as Credits from './workspace-credits.js';

// ============================================================================
// MODULE STATE
// ============================================================================

// DOM references
let imageDrop, imageInput, imagePreview, imageModelName;
let uploadModal, openUpload, closeUpload, cancelUpload, continueUpload;
let modelDrop, modelInput, modelFileHint, historyUploadBtn;
let genHint;

// ============================================================================
// HISTORY FILTER SWITCHING
// ============================================================================

function switchHistoryFilter(filter = 'all') {
  // Only reset page if filter actually changed
  if (State.historyState.filter !== filter) {
    State.historyState.filter = filter;
    State.historyState.page = 1;
    renderHistory();
  }
}

// ============================================================================
// MODAL MANAGEMENT
// ============================================================================

function showModal(show) {
  const on = !!show;
  uploadModal?.classList.toggle('show', on);
  document.body.classList.toggle('modal-open', on);
  if (window.viewerControls) window.viewerControls.enabled = !on;
}

function showErrorToast(message) {
  if (!document.body) {
    alert(message);
    return;
  }
  const toast = document.createElement('div');
  toast.textContent = message;
  toast.setAttribute('role', 'status');
  toast.style.cssText = [
    'position:fixed',
    'right:24px',
    'bottom:24px',
    'z-index:9999',
    'background:#2b1414',
    'color:#fff',
    'padding:12px 16px',
    'border:1px solid rgba(255,255,255,0.1)',
    'border-radius:12px',
    'box-shadow:0 10px 24px rgba(0,0,0,0.35)',
    'font-size:14px',
    'max-width:320px',
    'opacity:0',
    'transition:opacity 180ms ease'
  ].join(';');
  document.body.appendChild(toast);
  requestAnimationFrame(() => { toast.style.opacity = '1'; });
  setTimeout(() => {
    toast.style.opacity = '0';
    setTimeout(() => toast.remove(), 220);
  }, 2600);
}

// ============================================================================
// FILE HANDLERS
// ============================================================================

function handleImageFile(file) {
  if (!file.type.startsWith('image/')) {
    alert('Please select an image (.png, .jpg, .webp)');
    return;
  }
  UI.state.imageFile = file;
  const url = URL.createObjectURL(file);
  if (imagePreview) {
    imagePreview.src = url;
    imagePreview.classList.remove('hidden');
  }
  UI.updateGenerateHint();
}

function handleModelFile(file) {
  const ok = /\.(glb|gltf)$/i.test(file.name);
  if (!ok) {
    alert('For instant preview, upload a .glb or .gltf file.');
    return;
  }
  UI.state.modelFile = file;
  if (modelFileHint) modelFileHint.textContent = `Selected: ${file.name}`;
}

// ============================================================================
// HISTORY MIGRATION
// ============================================================================

function migrateHistoryDates() {
  const arr = State.getHistory();
  let dirty = false;
  const fixed = arr.map(it => {
    const ms = normalizeEpochMs(it?.created_at);
    const y = new Date(ms).getFullYear();
    if (!it || (y < 2000 || y > 2099) || ms !== it.created_at) {
      dirty = true;
      return { ...it, created_at: ms };
    }
    return it;
  });
  if (dirty) State.saveHistory(fixed);
}

function migrateHistoryTitles() {
  try {
    const arr = State.getHistory();
    let dirty = false;
    const fixed = arr.map(it => {
      if (!it) return it;
      const title = shortTitle(it);
      if (it.title !== title) {
        dirty = true;
        return { ...it, title };
      }
      return it;
    });
    if (dirty) State.saveHistory(fixed);
  } catch { /* ignore */ }
}

// ============================================================================
// GENERATE BUTTON LISTENERS (Event Delegation)
// ============================================================================

function setupGenerateButtonListeners() {
  const leftStack = document.getElementById('leftStack');
  if (!leftStack) {
    log('leftStack not found for generate button listeners');
    return;
  }

  leftStack.addEventListener('click', (e) => {
    const btn = e.target.closest('button');
    if (!btn) return;

    const btnId = btn.id;
    log('Generate button clicked:', btnId);

    if (btnId === 'applyRemeshBtn') {
      API.startRemeshFromPanel();
      return;
    }
    if (btnId === 'generateTextureBtn') {
      API.startTextureFromPanel();
      return;
    }
    if (btnId === 'applyRigBtn') {
      API.startRigFromPanel();
      return;
    }
    if (!btnId || !btnId.includes('generate')) return;

    if (btnId === 'generateModelBtn') {
      API.onGenerateClick();
    } else if (btnId === 'generateImageBtn') {
      API.startImageGenerationByProvider();
    } else if (btnId === 'generateVideoBtn') {
      API.onGenerateClick();
    }
  });

  log('Generate button listeners set up via event delegation');
}

// ============================================================================
// VIEWER TOOLBAR
// ============================================================================

function initViewerToolbar() {
  const toolbar = document.getElementById('viewerToolbar');
  if (!toolbar) return;

  toolbar.addEventListener('click', (e) => {
    const btn = e.target.closest('.viewer-toolbar__btn');
    if (!btn) return;

    const action = btn.dataset.action;
    const activeItem = API.getActiveHistoryItem();

    if (action === 'download' && activeItem?.glb_url) {
      const a = document.createElement('a');
      a.href = activeItem.glb_url;
      a.download = 'model.glb';
      document.body.appendChild(a);
      a.click();
      a.remove();
    }

    if (action === 'texture' && activeItem) {
      API.startTextureFromHistory(activeItem);
    }

    if (action === 'remesh' && activeItem) {
      API.startRemeshFromHistory(activeItem);
    }

    if (action === 'rig' && activeItem) {
      API.startRigFromHistory(activeItem);
    }
  });
}

// ============================================================================
// MAIN UI INITIALIZATION
// ============================================================================

function initUi() {
  // Initialize tab references
  UI.initTabRefs();

  // DOM lookups
  imageDrop = byId('imageDrop');
  imageInput = byId('imageUpload');
  imagePreview = byId('imagePreview');
  imageModelName = byId('imageModelName');

  uploadModal = byId('uploadModal');
  openUpload = byId('openUpload');
  closeUpload = byId('closeUpload');
  cancelUpload = byId('cancelUpload');
  continueUpload = byId('continueUpload');
  modelDrop = byId('modelDrop');
  modelInput = byId('customModelUpload');
  modelFileHint = byId('modelFileHint');
  historyUploadBtn = byId('historyUploadBtn');
  genHint = byId('genHint');

  // Set initial tab
  UI.setActiveTab('text3d');

  // Image drop zone
  safe(imageDrop, () => {
    const hl = (on) => imageDrop.classList.toggle('dragover', !!on);
    ['dragenter', 'dragover'].forEach(evt =>
      imageDrop.addEventListener(evt, e => { e.preventDefault(); hl(true); })
    );
    ['dragleave', 'drop'].forEach(evt =>
      imageDrop.addEventListener(evt, e => { e.preventDefault(); hl(false); })
    );
    imageDrop.addEventListener('drop', (e) => {
      const f = e.dataTransfer.files?.[0];
      if (f) handleImageFile(f);
    });
    imageDrop.addEventListener('click', () => imageInput?.click());
    imageDrop.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') imageInput?.click();
    });
  });

  safe(imageInput, () => {
    imageInput.addEventListener('change', () => {
      const f = imageInput.files?.[0];
      if (f) handleImageFile(f);
    });
  });

  // Upload modal
  safe(openUpload, () => {
    openUpload.addEventListener('click', (e) => { e.preventDefault(); showModal(true); });
  });
  safe(historyUploadBtn, () => {
    historyUploadBtn.addEventListener('click', (e) => { e.preventDefault(); showModal(true); });
  });
  safe(closeUpload, () => closeUpload.addEventListener('click', () => showModal(false)));
  safe(cancelUpload, () => cancelUpload.addEventListener('click', () => showModal(false)));
  safe(uploadModal, () => {
    uploadModal.addEventListener('click', (e) => {
      if (e.target === uploadModal) showModal(false);
    });
  });

  const uploadDialog = uploadModal?.querySelector('.upload-modal-content');
  if (uploadDialog) {
    uploadDialog.addEventListener('click', (e) => e.stopPropagation());
  }

  // Model drop zone
  safe(modelDrop, () => {
    const hl = (on) => modelDrop.classList.toggle('dragover', !!on);
    ['dragenter', 'dragover'].forEach(evt =>
      modelDrop.addEventListener(evt, e => { e.preventDefault(); e.stopPropagation(); hl(true); })
    );
    ['dragleave', 'drop'].forEach(evt =>
      modelDrop.addEventListener(evt, e => { e.preventDefault(); e.stopPropagation(); hl(false); })
    );
    modelDrop.addEventListener('drop', (e) => {
      const f = e.dataTransfer?.files?.[0];
      if (f) handleModelFile(f);
    });
    modelDrop.addEventListener('click', () => modelInput?.click());
    modelDrop.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); modelInput?.click(); }
    });
  });

  safe(modelInput, () => {
    modelInput.addEventListener('change', () => {
      const f = modelInput.files?.[0];
      if (f) handleModelFile(f);
    });
  });

  safe(continueUpload, () => {
    continueUpload.addEventListener('click', (e) => {
      e.preventDefault();
      if (!UI.state.modelFile) {
        alert('Please choose a .glb or .gltf file.');
        return;
      }
      if (typeof loadLocalGLB === 'function') {
        loadLocalGLB(UI.state.modelFile);
      }
      showModal(false);
    });
  });

  // Enter key triggers Generate from prompt textareas
  const promptTextareas = ['modelPrompt', 'imagePrompt', 'texturePrompt', 'videoMotion'];
  promptTextareas.forEach(id => {
    const textarea = byId(id);
    if (textarea) {
      textarea.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey && !e.repeat) {
          e.preventDefault();
          const genBtn = document.querySelector('button[id*="generate"]:not([disabled])');
          if (genBtn) API.onGenerateClick();
        }
      });
    }
  });

  // Global Enter key handler for focused buttons
  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Enter' || e.repeat) return;
    const focused = document.activeElement;
    if (focused && focused.tagName === 'BUTTON' && !focused.disabled) {
      const inTextarea = document.activeElement?.tagName === 'TEXTAREA';
      if (!inTextarea) {
        e.preventDefault();
        focused.click();
        log('Enter key triggered button:', focused.id || focused.textContent?.trim().substring(0, 20));
      }
    }
  });

  // ESC to close modal
  window.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && uploadModal?.classList.contains('show')) showModal(false);
  });

  // Migrate history data
  migrateHistoryTitles();
  migrateHistoryDates();

  // Initialize custom selects
  UI.initNiceSelects();

  UI.updateGenerateHint();
}

// ============================================================================
// HISTORY GALLERY WIRING
// ============================================================================

function wireGallery() {
  const grid = document.getElementById('historyGrid');
  const q = document.getElementById('historySearch');
  const size = document.getElementById('historyPageSize');
  const prev = document.getElementById('historyPrev');
  const next = document.getElementById('historyNext');
  const first = document.getElementById('historyFirst');
  const last = document.getElementById('historyLast');

  // Search input
  if (q) {
    q.addEventListener('input', e => {
      State.historyState.query = e.target.value.trim().toLowerCase();
      State.historyState.page = 1;
      renderHistory();
    });
  }

  // Page size select
  if (size) {
    size.addEventListener('change', (e) => {
      const nextSize = Math.max(1, parseInt(e.target.value, 10) || 9);
      State.historyState.pageSize = nextSize;
      State.historyState.page = 1;
      renderHistory();
    });
  }

  // Filter buttons
  const filterBtns = document.querySelectorAll('.filter-btn');
  filterBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      closeActiveHistoryMenu();
      const filterType = btn.getAttribute('data-filter');
      if (filterType === 'all') {
        State.historyState.galleryExpanded = State.historyState.filter === 'all'
          ? !State.historyState.galleryExpanded
          : true;
        State.historyState.filter = 'all';
      } else {
        State.historyState.galleryExpanded = false;
        State.historyState.filter = filterType;
      }
      State.historyState.page = 1;
      renderHistory();
    });
  });

  // Sort toggle
  const sortToggle = document.getElementById('historySortToggle');
  if (sortToggle) {
    sortToggle.addEventListener('click', () => {
      State.historyState.sort = State.historyState.sort === 'desc' ? 'asc' : 'desc';
      renderHistory();
    });
  }

  // Collapse button
  const collapseBtn = document.getElementById('historyCollapseView');
  if (collapseBtn) {
    collapseBtn.addEventListener('click', () => {
      closeActiveHistoryMenu();
      State.historyState.galleryExpanded = false;
      State.historyState.page = 1;
      renderHistory();
    });
  }

  // Refresh/Restore from DB button
  const refreshBtn = document.getElementById('historyRefreshBtn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', async () => {
      if (refreshBtn.classList.contains('is-loading')) return;
      refreshBtn.classList.add('is-loading');
      try {
        await State.forceRestoreFromDB();
        State.historyState.page = 1;
        renderHistory();
        // Show success state briefly
        refreshBtn.classList.remove('is-loading');
        refreshBtn.classList.add('is-success');
        setTimeout(() => refreshBtn.classList.remove('is-success'), 1500);
      } catch (err) {
        console.error('Failed to restore history:', err);
        alert('Failed to restore history from database. Please try again.');
        refreshBtn.classList.remove('is-loading');
      }
    });
  }

  // Pagination
  if (first) first.addEventListener('click', () => {
    if (State.historyState.page > 1) {
      State.historyState.page = 1;
      renderHistory();
    }
  });
  if (prev) prev.addEventListener('click', () => {
    if (State.historyState.page > 1) {
      State.historyState.page--;
      renderHistory();
    }
  });
  if (next) next.addEventListener('click', () => {
    const filtered = getFilteredHistory();
    const total = Math.max(1, Math.ceil(filtered.length / State.historyState.pageSize));
    if (State.historyState.page < total) {
      State.historyState.page++;
      renderHistory();
    }
  });
  if (last) last.addEventListener('click', () => {
    const filtered = getFilteredHistory();
    const total = Math.max(1, Math.ceil(filtered.length / State.historyState.pageSize));
    if (State.historyState.page < total) {
      State.historyState.page = total;
      renderHistory();
    }
  });

  // Grid event delegation
  if (grid) {
    grid.addEventListener('click', async (e) => {
      // Toggle collection expansion
      const toggleBtn = e.target.closest('[data-action="toggle-collection"]');
      if (toggleBtn) {
        const collection = toggleBtn.closest('.history-collection');
        if (collection) {
          const isExpanded = collection.classList.toggle('is-expanded');
          toggleBtn.setAttribute('aria-expanded', isExpanded ? 'true' : 'false');
        }
        e.stopPropagation();
        return;
      }

      // Menu toggle
      const menuBtn = e.target.closest('[data-history-menu]');
      if (menuBtn) {
        const menu = menuBtn.nextElementSibling?.classList?.contains('card-menu')
          ? menuBtn.nextElementSibling
          : menuBtn.parentElement?.querySelector('.card-menu');
        if (!menu) return;
        const isOpen = menuBtn.getAttribute('aria-expanded') === 'true';
        if (isOpen) {
          closeActiveHistoryMenu();
        } else {
          openHistoryMenu(menuBtn, menu);
        }
        e.stopPropagation();
        return;
      }

      // Submenu toggle
      const submenuBtn = e.target.closest('[data-submenu-open]');
      if (submenuBtn) {
        const targetId = submenuBtn.getAttribute('data-submenu-open');
        if (!targetId) return;
        const panel = document.querySelector(`[data-submenu-panel="${targetId}"]`);
        if (!panel) return;
        const isOpen = submenuBtn.getAttribute('aria-expanded') === 'true';
        if (isOpen) {
          closeActiveHistorySubmenu();
        } else {
          openHistorySubmenu(submenuBtn, panel);
        }
        e.stopPropagation();
        return;
      }

      // Action buttons
      const btn = e.target.closest('[data-act]');
      if (!btn || btn.disabled) return;
      closeActiveHistoryMenu();

      const id = btn.getAttribute('data-id');
      const act = btn.getAttribute('data-act');
      const item = State.getHistory().find(x => x.id === id);
      if (!item) return;

      const glbUrl = item.glb_proxy || item.glb_url;

      // Handle actions
      if (act === 'open') {
        const wasGallery = !!State.historyState.galleryExpanded;
        if (wasGallery) {
          State.historyState.galleryExpanded = false;
          State.historyState.page = 1;
        }

        if (!glbUrl && (item.type === 'image' || item.image_url)) {
            State.setHistoryActiveModelId(id);
            renderHistory();
            const imgSrc = item.image_url || item.thumbnail_url || '';
          if (imgSrc) {
            const imageRailBtn = document.querySelector('[data-panel="image"]');
            if (imageRailBtn) imageRailBtn.click();
            Viewer.showImageInViewer(imgSrc);
          }
          return;
        }

        if (!glbUrl) return;

        const modelRailBtn = document.querySelector('[data-panel="model"]');
        if (modelRailBtn) modelRailBtn.click();

        const genHintEl = byId('genHint');
        if (genHintEl) genHintEl.textContent = 'Loading model...';
        State.setHistoryActiveModelId(id);
        renderHistory();

        if (wasGallery) {
          requestAnimationFrame(() => {
            window.scrollTo({ top: 0, behavior: 'smooth' });
          });
        }

        const primary = item.glb_proxy || item.glb_url;
        const fallback = (item.glb_url && item.glb_url !== primary) ? item.glb_url : null;
        await Viewer.loadModelWithFallback(primary, fallback);
        if (genHintEl) genHintEl.textContent = 'Loaded from history.';
        return;
      }

      if ((act === 'download' || act === 'print') && item.glb_url) {
        const a = document.createElement('a');
        a.href = item.glb_url;
        a.download = 'model.glb';
        document.body.appendChild(a);
        a.click();
        a.remove();
        return;
      }

      if (act === 'download-image') {
        const imageUrl = btn.getAttribute('data-image-url') || item.image_url || item.thumbnail_url;
        if (!imageUrl) {
          alert('No image available to download.');
          return;
        }
        const a = document.createElement('a');
        a.href = imageUrl;
        a.download = `${shortTitle(item)}.png`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        return;
      }

      if (act === 'copy-link') {
        const link = item.glb_proxy || item.glb_url || item.image_url;
        if (!link) {
          alert('No downloadable link available yet.');
          return;
        }
        if (navigator.clipboard?.writeText) {
          try {
            await navigator.clipboard.writeText(link);
            alert('Link copied to clipboard.');
          } catch {
            prompt('Copy link manually:', link);
          }
        } else {
          prompt('Copy link manually:', link);
        }
        return;
      }

      if (act === 'texture') {
        await API.startTextureFromHistory(item);
        return;
      }

      if (act === 'remesh') {
        await API.startRemeshFromHistory(item);
        return;
      }

      if (act === 'rig') {
        await API.startRigFromHistory(item);
        return;
      }

      if (act === 'refine') {
        API.onPostProcessFromHistory(item, 'refine');
        return;
      }

      if (act === 'image-to-3d') {
        await API.startImageTo3DFromHistory(item);
        return;
      }

      if (act === 'delete') {
        if (!confirm('Delete from database and S3 permanently?')) return;
        try {
          const resp = await fetch(`${BACKEND}/api/history/item/${encodeURIComponent(id)}`, {
            method: 'DELETE',
            credentials: 'include'
          });
          const data = await resp.json().catch(() => ({}));
          if (!resp.ok || data.ok === false) {
            throw new Error(data.error || `HTTP ${resp.status}`);
          }
          State.deleteHistoryItem(id, { skipRemote: true });
          renderHistory();
        } catch (err) {
          console.warn('[History] Delete failed:', err?.message || err);
          showErrorToast('Delete failed. Please try again.');
        }
        return;
      }
    });

    // Keyboard navigation in grid
    grid.addEventListener('keydown', (evt) => {
      const thumbBtn = evt.target.closest('.history-thumb__image, .history-thumb__preview');
      if (!thumbBtn) return;

      const moveFocus = (direction) => {
        const row = thumbBtn.closest('.history-collection');
        if (!row) return;
        const focusables = Array.from(row.querySelectorAll('.history-thumb__image, .history-thumb__preview'));
        if (!focusables.length) return;
        const currentIdx = focusables.indexOf(thumbBtn);
        if (currentIdx === -1) return;
        let nextIdx = currentIdx + direction;
        if (nextIdx < 0) nextIdx = focusables.length - 1;
        if (nextIdx >= focusables.length) nextIdx = 0;
        focusables[nextIdx]?.focus();
      };

      if (evt.key === 'ArrowRight') {
        evt.preventDefault();
        moveFocus(1);
        return;
      }
      if (evt.key === 'ArrowLeft') {
        evt.preventDefault();
        moveFocus(-1);
        return;
      }
      if (evt.key === 'Enter' || evt.key === ' ') {
        evt.preventDefault();
        thumbBtn.click();
      }
    });
  }

  // Close menus on outside click
  document.addEventListener('click', (evt) => {
    const { submenu: activeSubmenu } = getActiveHistorySubmenu();
    const { menu: activeMenu } = getActiveHistoryMenu();

    if (activeSubmenu) {
      const insideSubmenu = evt.target.closest('.card-submenu');
      const onSubToggle = evt.target.closest('[data-submenu-open]');
      if (!insideSubmenu && !onSubToggle) closeActiveHistorySubmenu();
    }
    if (!activeMenu) return;
    const insideMenu = evt.target.closest('.card-menu');
    const onToggle = evt.target.closest('[data-history-menu]');
    if (insideMenu || onToggle) return;
    closeActiveHistoryMenu();
  });

  // ESC to close menus/gallery
  document.addEventListener('keydown', (evt) => {
    if (evt.key === 'Escape') {
      const { submenu: activeSubmenu } = getActiveHistorySubmenu();
      const { menu: activeMenu } = getActiveHistoryMenu();

      if (activeSubmenu) {
        closeActiveHistorySubmenu();
        return;
      }
      if (activeMenu) {
        closeActiveHistoryMenu();
        return;
      }
      if (State.historyState.galleryExpanded) {
        State.historyState.galleryExpanded = false;
        State.historyState.page = 1;
        renderHistory();
      }
    }
  });

  // Initial render
  renderHistory();
}

// ============================================================================
// BOOTSTRAP
// ============================================================================

window.addEventListener('DOMContentLoaded', () => {
  log('Initializing TimrX 3D Print Hub...');

  // Initialize converter tool
  try {
    Converter.init();
  } catch (e) {
    console.error('Converter init failed:', e);
  }

  // Wait for Three.js to be ready
  onThreeReady(async () => {
    log('Three.js ready, initializing modules...');

    // Initialize viewer
    try {
      Viewer.initViewer();
    } catch (e) {
      console.error('Viewer init failed:', e);
    }

    // Initialize UI
    try {
      initUi();
    } catch (e) {
      console.error('UI init failed:', e);
    }

    // Wire up history gallery
    try {
      wireGallery();
    } catch (e) {
      console.error('Gallery wire failed:', e);
    }

    // Initialize credits system (fetch wallet + action costs)
    try {
      Credits.initCredits();
    } catch (e) {
      console.error('Credits init failed:', e);
    }

    // Load history from database and render
    try {
      await State.loadHistoryFromDB();
      renderHistory();
    } catch (e) {
      console.error('History load failed:', e);
      renderHistory(); // Still render with cache
    }

    // Sync history filter with rail buttons
    const imageRail = document.querySelector('[data-panel="image"]');
    const modelRail = document.querySelector('[data-panel="model"]');
    const videoRail = document.querySelector('[data-panel="video"]');
    if (imageRail) imageRail.addEventListener('click', () => switchHistoryFilter('image'));
    if (modelRail) modelRail.addEventListener('click', () => switchHistoryFilter('all'));
    if (videoRail) videoRail.addEventListener('click', () => switchHistoryFilter('video'));

    // Set up generate button listeners
    setupGenerateButtonListeners();

    // Initialize viewer toolbar
    initViewerToolbar();

    // Hide progress initially
    UI.showOutputEmpty();

    // Resume any pending jobs
    await API.resumePendingJobs({ skipEmptyUI: true });

    log('TimrX 3D Print Hub initialized successfully.');
  });
});

// ============================================================================
// EXPOSE GLOBALS (for backward compatibility)
// ============================================================================
window.renderHistory = renderHistory;
window.switchHistoryFilter = switchHistoryFilter;
window.showModal = showModal;
