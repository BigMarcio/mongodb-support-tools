/**
 * Independent polling for Migration Verifier progress, summary, and metadata.
 */
(function (global) {
    'use strict';

    function warningsForSource(list) {
        return (list || []).filter(function (w) {
            return w;
        });
    }

    function rebuildWarnings(warningsBySource) {
        var merged = [];
        ['progress', 'summary', 'metadata'].forEach(function (source) {
            warningsForSource(warningsBySource[source]).forEach(function (w) {
                if (merged.indexOf(w) === -1) merged.push(w);
            });
        });
        return merged;
    }

    function miInitVerifierPolling(config) {
        var root = document.getElementById('verifier-monitor-root');
        var loading = document.getElementById('verifier-monitor-loading');
        if (!root || !config || !config.urls) return;

        var slots = global.miInitVerifierMonitorShell(root);
        var state = {
            verificationProgress: null,
            verificationSummary: null,
            metadataDisplay: null,
            stateBadge: null,
            warnings: [],
            warningsBySource: { progress: [], summary: [], metadata: [] },
            connectivity: null,
        };

        var inflight = { progress: false, summary: false, metadata: false };
        var enabled = { progress: true, summary: true, metadata: true };
        var intervalIds = { progress: null, summary: null, metadata: null };
        var firstResponse = false;

        var baseRefreshSec = config.baseRefreshSec || 10;
        var progressMs = config.progressRefreshMs || 10000;
        var summaryMs = config.summaryRefreshMs || 120000;
        var metadataMs = config.metadataRefreshMs || 60000;
        var progressRatio = (progressMs / 1000) / baseRefreshSec;
        var summaryRatio = (summaryMs / 1000) / baseRefreshSec;
        var metadataRatio = (metadataMs / 1000) / baseRefreshSec;

        function stopPoller(source) {
            if (intervalIds[source]) {
                clearInterval(intervalIds[source]);
                intervalIds[source] = null;
            }
        }

        function refreshAllSections() {
            global.miUpdateVerifierToolbar(
                slots,
                state.verificationProgress,
                state.verificationSummary,
                state.metadataDisplay,
                state.stateBadge
            );
            global.miUpdateVerifierProgressSection(slots, state.verificationProgress);
            global.miUpdateVerifierSummarySection(slots, state.verificationSummary);
            global.miUpdateVerifierMetadataSection(slots, state.metadataDisplay);
            global.miUpdateVerifierWarnings(slots, state.warnings);
            if (state.connectivity) {
                global.miUpdateVerifierConnectivity(slots, state.connectivity);
            }
        }

        function hideLoadingOnce() {
            if (firstResponse) return;
            firstResponse = true;
            if (loading) loading.style.display = 'none';
            root.style.display = 'block';
        }

        function applyConnectivity(connectivity) {
            if (!connectivity) return;
            state.connectivity = connectivity;
            global.miUpdateVerifierConnectivity(slots, state.connectivity);
        }

        function setSourceWarnings(source, warnings) {
            state.warningsBySource[source] = warningsForSource(warnings);
            state.warnings = rebuildWarnings(state.warningsBySource);
        }

        function applyStateBadge(source, badge) {
            if (!badge) return;
            if (source === 'progress') {
                state.stateBadge = badge;
            } else if (source === 'metadata' && state.stateBadge == null) {
                state.stateBadge = badge;
            }
        }

        function fetchSlice(url, source) {
            if (!enabled[source] || inflight[source]) return Promise.resolve();
            inflight[source] = true;
            return fetch(url, {
                method: 'POST',
                credentials: 'same-origin',
            })
                .then(function (response) {
                    return response.json().then(function (payload) {
                        return { response: response, payload: payload };
                    });
                })
                .then(function (result) {
                    if (result.response.status === 400) {
                        enabled[source] = false;
                        stopPoller(source);
                        setSourceWarnings(source, []);
                        global.miUpdateVerifierWarnings(slots, state.warnings);
                        return;
                    }
                    if (!result.response.ok && result.payload.error) {
                        setSourceWarnings(source, [result.payload.error]);
                        global.miUpdateVerifierWarnings(slots, state.warnings);
                        return;
                    }
                    var payload = result.payload;
                    hideLoadingOnce();
                    var sourceWarnings = warningsForSource(payload.warnings);
                    if (payload.error) sourceWarnings.push(payload.error);
                    setSourceWarnings(source, sourceWarnings);
                    applyConnectivity(payload.connectivity);

                    var display = payload.display || {};

                    if (source === 'progress') {
                        state.verificationProgress = display.verificationProgress || null;
                        applyStateBadge(source, display.stateBadge);
                        global.miUpdateVerifierProgressSection(slots, state.verificationProgress);
                    } else if (source === 'summary') {
                        state.verificationSummary = display.verificationSummary || null;
                        if (
                            state.verificationSummary &&
                            state.verificationSummary.estCheckSecsRemaining != null &&
                            state.verificationProgress
                        ) {
                            state.verificationProgress.estCheckSecsRemaining =
                                state.verificationSummary.estCheckSecsRemaining;
                        }
                        global.miUpdateVerifierSummarySection(slots, state.verificationSummary);
                    } else if (source === 'metadata') {
                        state.metadataDisplay = payload.display || null;
                        applyStateBadge(source, (state.metadataDisplay || {}).stateBadge);
                        global.miUpdateVerifierMetadataSection(slots, state.metadataDisplay);
                    }

                    global.miUpdateVerifierToolbar(
                        slots,
                        state.verificationProgress,
                        state.verificationSummary,
                        state.metadataDisplay,
                        state.stateBadge
                    );
                    global.miUpdateVerifierWarnings(slots, state.warnings);
                })
                .catch(function (err) {
                    console.error('Verifier ' + source + ' poll failed:', err);
                    setSourceWarnings(
                        source,
                        ['Error loading ' + source + ': ' + err.message]
                    );
                    hideLoadingOnce();
                    global.miUpdateVerifierWarnings(slots, state.warnings);
                })
                .finally(function () {
                    inflight[source] = false;
                });
        }

        function startPoller(source, url, intervalMs) {
            if (!url) {
                enabled[source] = false;
                stopPoller(source);
                return;
            }
            fetchSlice(url, source);
            stopPoller(source);
            intervalIds[source] = setInterval(function () {
                fetchSlice(url, source);
            }, intervalMs);
        }

        function restartTimers(newBaseSec) {
            progressMs = newBaseSec * progressRatio * 1000;
            summaryMs = newBaseSec * summaryRatio * 1000;
            metadataMs = newBaseSec * metadataRatio * 1000;
            if (enabled.progress) startPoller('progress', config.urls.progress, progressMs);
            if (enabled.summary) startPoller('summary', config.urls.summary, summaryMs);
            if (enabled.metadata) startPoller('metadata', config.urls.metadata, metadataMs);
            if (typeof config.onRefreshLabelUpdate === 'function') {
                config.onRefreshLabelUpdate(
                    progressMs / 1000, summaryMs / 1000, metadataMs / 1000,
                );
            }
        }

        function onRefreshChanged(e) {
            var newSec = e.detail && e.detail.refreshSec;
            if (newSec > 0) restartTimers(newSec);
        }

        function teardown() {
            ['progress', 'summary', 'metadata'].forEach(stopPoller);
            window.removeEventListener('mi-refresh-changed', onRefreshChanged);
            window.removeEventListener('pagehide', teardown);
        }

        startPoller('progress', config.urls.progress, progressMs);
        startPoller('summary', config.urls.summary, summaryMs);
        startPoller('metadata', config.urls.metadata, metadataMs);

        window.addEventListener('mi-refresh-changed', onRefreshChanged);
        window.addEventListener('pagehide', teardown);

        return { refreshAllSections: refreshAllSections, teardown: teardown };
    }

    global.miInitVerifierPolling = miInitVerifierPolling;
})(typeof window !== 'undefined' ? window : this);
