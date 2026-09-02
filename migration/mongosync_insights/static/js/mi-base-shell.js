/**
 * Shared shell UI: settings, logout, credits overlays (base.html).
 */
(function (global) {
    'use strict';

    function openSettingsDialog() {
        var refresh = sessionStorage.getItem('mi_refresh_time') || '10';
        document.getElementById('settingsRefresh').value = refresh;
        if (typeof global.miOpenModal === 'function') {
            global.miOpenModal('settingsOverlay', document.getElementById('settingsBtn'));
        } else {
            document.getElementById('settingsOverlay').classList.add('active');
        }
    }

    function closeSettingsDialog() {
        if (typeof global.miCloseModal === 'function') {
            global.miCloseModal('settingsOverlay');
        } else {
            document.getElementById('settingsOverlay').classList.remove('active');
        }
    }

    function applySettings() {
        var refresh = document.getElementById('settingsRefresh').value;
        sessionStorage.setItem('mi_refresh_time', refresh);
        closeSettingsDialog();
        global.dispatchEvent(new CustomEvent('mi-refresh-changed', {
            detail: { refreshSec: parseInt(refresh, 10) },
        }));
    }

    function confirmLogout() {
        if (typeof global.miOpenModal === 'function') {
            global.miOpenModal('logoutOverlay', document.getElementById('logoutBtn'));
        } else {
            document.getElementById('logoutOverlay').classList.add('active');
        }
    }

    function closeLogoutDialog() {
        if (typeof global.miCloseModal === 'function') {
            global.miCloseModal('logoutOverlay');
        } else {
            document.getElementById('logoutOverlay').classList.remove('active');
        }
    }

    function doLogout() {
        fetch('/logout', { method: 'POST', credentials: 'same-origin' })
            .then(function () { global.location.href = '/'; })
            .catch(function () { global.location.href = '/'; });
    }

    function openCreditsOverlay() {
        if (typeof global.miOpenModal === 'function') {
            global.miOpenModal('creditsOverlay', document.getElementById('creditsInfoBtn'));
        } else {
            document.getElementById('creditsOverlay').classList.add('active');
        }
    }

    function closeCreditsOverlay() {
        if (typeof global.miCloseModal === 'function') {
            global.miCloseModal('creditsOverlay');
        } else {
            document.getElementById('creditsOverlay').classList.remove('active');
        }
    }

    function registerShellModals() {
        if (typeof global.miRegisterModal !== 'function') return;
        global.miRegisterModal('settingsOverlay', closeSettingsDialog, { labelledBy: 'settingsDialogTitle' });
        global.miRegisterModal('logoutOverlay', closeLogoutDialog, { labelledBy: 'logoutDialogTitle' });
        global.miRegisterModal('creditsOverlay', closeCreditsOverlay, { labelledBy: 'creditsDialogTitle' });
    }

    global.openSettingsDialog = openSettingsDialog;
    global.closeSettingsDialog = closeSettingsDialog;
    global.applySettings = applySettings;
    global.confirmLogout = confirmLogout;
    global.closeLogoutDialog = closeLogoutDialog;
    global.doLogout = doLogout;
    global.openCreditsOverlay = openCreditsOverlay;
    global.closeCreditsOverlay = closeCreditsOverlay;

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', registerShellModals);
    } else {
        registerShellModals();
    }
})(typeof window !== 'undefined' ? window : this);
