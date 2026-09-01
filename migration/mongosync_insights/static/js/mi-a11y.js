/**
 * Shared accessibility helpers: modal dialogs (focus trap, Escape, focus restore).
 */
(function (global) {
    'use strict';

    var registry = {};

    function getFocusable(container) {
        if (!container) return [];
        return Array.prototype.slice.call(container.querySelectorAll(
            'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
        )).filter(function (el) {
            return !el.hasAttribute('disabled') && el.getAttribute('aria-hidden') !== 'true';
        });
    }

    function findDialogEl(overlay) {
        var dialog = overlay.querySelector('[role="dialog"]');
        if (dialog) return dialog;
        return overlay.querySelector(
            '.settings-dialog, .confirm-dialog, .credits-panel, .upload-dialog, .dup-dialog'
        );
    }

    function trapFocus(event, dialog) {
        if (event.key !== 'Tab' || !dialog) return;
        var focusable = getFocusable(dialog);
        if (focusable.length === 0) return;
        var first = focusable[0];
        var last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
            event.preventDefault();
            last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault();
            first.focus();
        }
    }

    function registerModal(overlayId, closeFn, options) {
        var overlay = document.getElementById(overlayId);
        if (!overlay) return;

        options = options || {};
        var dialog = findDialogEl(overlay);
        if (dialog) {
            if (!dialog.getAttribute('role')) {
                dialog.setAttribute('role', 'dialog');
            }
            dialog.setAttribute('aria-modal', 'true');
            if (options.labelledBy && !dialog.getAttribute('aria-labelledby')) {
                dialog.setAttribute('aria-labelledby', options.labelledBy);
            }
        }

        overlay.setAttribute('aria-hidden', overlay.classList.contains('active') ? 'false' : 'true');

        registry[overlayId] = {
            closeFn: closeFn,
            overlay: overlay,
            dialog: dialog,
            previousFocus: null,
        };

        overlay.addEventListener('keydown', function (event) {
            if (event.key === 'Escape') {
                event.preventDefault();
                closeFn();
                return;
            }
            trapFocus(event, dialog || overlay);
        });
    }

    function openModal(overlayId, triggerEl) {
        var entry = registry[overlayId];
        if (!entry) return;
        entry.previousFocus = triggerEl || document.activeElement;
        entry.overlay.classList.add('active');
        entry.overlay.setAttribute('aria-hidden', 'false');
        var focusTarget = getFocusable(entry.dialog || entry.overlay)[0];
        if (focusTarget) {
            focusTarget.focus();
        }
    }

    function closeModal(overlayId) {
        var entry = registry[overlayId];
        if (!entry) return;
        entry.overlay.classList.remove('active');
        entry.overlay.setAttribute('aria-hidden', 'true');
        if (entry.previousFocus && typeof entry.previousFocus.focus === 'function') {
            try {
                entry.previousFocus.focus();
            } catch (e) {
                /* ignore stale focus targets */
            }
        }
        entry.previousFocus = null;
    }

    global.miRegisterModal = registerModal;
    global.miOpenModal = openModal;
    global.miCloseModal = closeModal;
})(typeof window !== 'undefined' ? window : this);
