/**
 * Combined live monitoring dashboard (progress-only sections).
 */
(function (global) {
    'use strict';

    function fetchJson(url) {
        return fetch(url, {
            method: 'POST',
            credentials: 'same-origin',
        }).then(function (response) {
            return response.json().then(function (payload) {
                return { response: response, payload: payload };
            });
        });
    }

    function startPolling(refreshFn, refreshMs) {
        var inflight = false;
        var intervalId = null;

        function refresh() {
            if (inflight) {
                return;
            }
            inflight = true;
            refreshFn()
                .catch(function (err) {
                    console.error(err);
                })
                .finally(function () {
                    inflight = false;
                });
        }

        refresh();
        intervalId = setInterval(refresh, refreshMs);

        function teardown() {
            if (intervalId) {
                clearInterval(intervalId);
                intervalId = null;
            }
            window.removeEventListener('pagehide', teardown);
        }

        window.addEventListener('pagehide', teardown);
        return { refresh: refresh, teardown: teardown };
    }

    function initMigrationSection(config) {
        var loading = document.getElementById(config.loadingId);
        var root = document.getElementById(config.rootId);
        if (!loading || !root) return;

        var renderOptions = { syncOnly: true };
        if (config.fullViewLink) {
            renderOptions.fullViewLink = config.fullViewLink;
        }

        startPolling(function () {
            return fetchJson(config.pollUrl).then(function (result) {
                if (!result.response.ok) {
                    var msg = (result.payload && result.payload.error) || 'Request failed';
                    throw new Error(msg);
                }
                var payload = result.payload;
                loading.style.display = 'none';
                root.style.display = 'block';
                if (typeof global.miRenderProgressMonitor === 'function') {
                    global.miRenderProgressMonitor(root, payload, renderOptions);
                }
            }).catch(function (err) {
                console.error('Error fetching migration dashboard data:', err);
                loading.style.display = 'none';
                root.style.display = 'block';
                if (typeof global.miRenderProgressMonitor === 'function') {
                    global.miRenderProgressMonitor(root, {
                        warnings: [],
                        display: null,
                        error: 'Error loading data: ' + err.message,
                    }, renderOptions);
                }
            });
        }, config.refreshMs);
    }

    function initVerifierSection(config) {
        var loading = document.getElementById(config.loadingId);
        var root = document.getElementById(config.rootId);
        if (!loading || !root) return;

        var slots = null;
        if (typeof global.miInitVerifierMonitorShell === 'function') {
            slots = global.miInitVerifierMonitorShell(root);
        }

        var toolbarOptions = config.fullViewLink ? { fullViewLink: config.fullViewLink } : null;

        startPolling(function () {
            return fetchJson(config.pollUrl).then(function (result) {
                if (!result.response.ok) {
                    var msg = (result.payload && result.payload.error) || 'Request failed';
                    throw new Error(msg);
                }
                var payload = result.payload;
                loading.style.display = 'none';
                root.style.display = 'block';

                if (!slots) return;

                var display = payload.display || {};
                var progress = display.verificationProgress || null;
                if (typeof global.miUpdateVerifierToolbar === 'function') {
                    global.miUpdateVerifierToolbar(
                        slots,
                        progress,
                        null,
                        null,
                        display.stateBadge,
                        payload.dataSources,
                        toolbarOptions
                    );
                }
                if (typeof global.miUpdateVerifierProgressSection === 'function') {
                    global.miUpdateVerifierProgressSection(slots, progress);
                }
                if (typeof global.miUpdateVerifierWarnings === 'function') {
                    global.miUpdateVerifierWarnings(slots, payload.warnings);
                }
            }).catch(function (err) {
                console.error('Error fetching verifier dashboard data:', err);
                loading.style.display = 'none';
                root.style.display = 'block';
            });
        }, config.refreshMs);
    }

    function miInitDashboard(config) {
        if (config.migration) {
            initMigrationSection(config.migration);
        }
        if (config.verifier) {
            initVerifierSection(config.verifier);
        }
    }

    global.miInitDashboard = miInitDashboard;
})(typeof window !== 'undefined' ? window : this);
