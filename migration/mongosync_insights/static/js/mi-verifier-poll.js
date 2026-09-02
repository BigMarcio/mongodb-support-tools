/**
 * Independent polling for Migration Verifier progress and metadata.
 * Mismatch summary (/summary) is manual-only via run_verifier_summary.
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
        ['progress', 'metadata', 'summaryRun'].forEach(function (source) {
            warningsForSource(warningsBySource[source]).forEach(function (w) {
                if (merged.indexOf(w) === -1) merged.push(w);
            });
        });
        return merged;
    }

    function mergeDataSourceBadges(dataSourcesById, payload) {
        if (!payload || !payload.dataSources || !payload.dataSources.badges) return;
        payload.dataSources.badges.forEach(function (b) {
            dataSourcesById[b.id] = b;
        });
    }

    function buildMergedDataSources(dataSourcesById) {
        var badges = Object.keys(dataSourcesById).map(function (id) {
            return dataSourcesById[id];
        });
        if (badges.length === 0) return null;
        var hasProgress = dataSourcesById.progress != null;
        var hasMetadata = dataSourcesById.metadata != null;
        var mode = 'none';
        if (hasProgress && hasMetadata) mode = 'both';
        else if (hasProgress) mode = 'endpoint';
        else if (hasMetadata) mode = 'metadata';
        return { mode: mode, badges: badges };
    }

    function applySummaryFromCache(state, summaryCache) {
        state.summaryCache = summaryCache || null;
        if (!summaryCache || !summaryCache.eligible) {
            state.verificationSummary = null;
            return;
        }
        state.verificationSummary = summaryCache.verificationSummary || null;
        if (
            state.verificationSummary &&
            state.verificationSummary.estCheckSecsRemaining != null &&
            state.verificationProgress
        ) {
            state.verificationProgress.estCheckSecsRemaining =
                state.verificationSummary.estCheckSecsRemaining;
        }
    }

    function miInitVerifierPolling(config) {
        var root = document.getElementById('verifier-monitor-root');
        var loading = document.getElementById('verifier-monitor-loading');
        if (!root || !config || !config.urls) return;

        if (typeof global.miConfigureVerifierDownloads === 'function') {
            global.miConfigureVerifierDownloads({
                docMismatches: config.urls.docMismatchesDownload,
                nsMismatches: config.urls.nsMismatchesDownload,
            });
        }

        var slots = global.miInitVerifierMonitorShell(root);
        var state = {
            verificationProgress: null,
            verificationSummary: null,
            summaryCache: null,
            summaryRunInFlight: false,
            metadataDisplay: null,
            stateBadge: null,
            warnings: [],
            warningsBySource: { progress: [], metadata: [], summaryRun: [] },
            dataSourcesById: {},
        };

        var inflight = { progress: false, metadata: false };
        var enabled = { progress: true, metadata: true };
        var intervalIds = { progress: null, metadata: null };
        var firstResponse = false;

        var baseRefreshSec = config.baseRefreshSec || 10;
        var progressMs = config.progressRefreshMs || 10000;
        var metadataMs = config.metadataRefreshMs || 60000;
        var progressRatio = (progressMs / 1000) / baseRefreshSec;
        var metadataRatio = (metadataMs / 1000) / baseRefreshSec;

        function stopPoller(source) {
            if (intervalIds[source]) {
                clearInterval(intervalIds[source]);
                intervalIds[source] = null;
            }
        }

        function mergedDataSources() {
            return buildMergedDataSources(state.dataSourcesById);
        }

        function updateSummarySection() {
            global.miUpdateVerifierSummarySection(
                slots,
                state.verificationSummary,
                state.summaryCache,
                state.verificationProgress,
                { runInFlight: state.summaryRunInFlight }
            );
        }

        function refreshAllSections() {
            global.miUpdateVerifierToolbar(
                slots,
                state.verificationProgress,
                state.verificationSummary,
                state.metadataDisplay,
                state.stateBadge,
                mergedDataSources()
            );
            global.miUpdateVerifierProgressSection(slots, state.verificationProgress);
            updateSummarySection();
            global.miUpdateVerifierMetadataSection(slots, state.metadataDisplay);
            global.miUpdateVerifierWarnings(slots, state.warnings);
        }

        function hideLoadingOnce() {
            if (firstResponse) return;
            firstResponse = true;
            if (loading) loading.style.display = 'none';
            root.style.display = 'block';
        }

        function applyDataSources(payload) {
            mergeDataSourceBadges(state.dataSourcesById, payload);
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
                    applyDataSources(payload);

                    var display = payload.display || {};

                    if (source === 'progress') {
                        state.verificationProgress = display.verificationProgress || null;
                        applyStateBadge(source, display.stateBadge);
                        applySummaryFromCache(state, display.summaryCache);
                        global.miUpdateVerifierProgressSection(slots, state.verificationProgress);
                        updateSummarySection();
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
                        state.stateBadge,
                        mergedDataSources()
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

        function runVerifierSummary() {
            var runUrl = config.urls.runSummary;
            if (!runUrl || state.summaryRunInFlight) return Promise.resolve();

            state.summaryRunInFlight = true;
            updateSummarySection();

            return fetch(runUrl, {
                method: 'POST',
                credentials: 'same-origin',
            })
                .then(function (response) {
                    return response.json().then(function (payload) {
                        return { response: response, payload: payload };
                    });
                })
                .then(function (result) {
                    var payload = result.payload;
                    var display = payload.display || {};

                    if (result.response.status === 429) {
                        setSourceWarnings('summaryRun', [payload.error || 'Summary cooldown active.']);
                        if (display.summaryCache) {
                            applySummaryFromCache(state, display.summaryCache);
                        }
                    } else if (result.response.status === 403) {
                        setSourceWarnings('summaryRun', [payload.error || 'Summary not available.']);
                    } else if (!result.response.ok) {
                        setSourceWarnings(
                            'summaryRun',
                            [payload.error || 'Failed to run mismatch summary.']
                        );
                        if (display.summaryCache) {
                            applySummaryFromCache(state, display.summaryCache);
                        }
                    } else {
                        setSourceWarnings('summaryRun', []);
                        applySummaryFromCache(state, display.summaryCache);
                    }

                    updateSummarySection();
                    global.miUpdateVerifierToolbar(
                        slots,
                        state.verificationProgress,
                        state.verificationSummary,
                        state.metadataDisplay,
                        state.stateBadge,
                        mergedDataSources()
                    );
                    global.miUpdateVerifierWarnings(slots, state.warnings);
                })
                .catch(function (err) {
                    console.error('Verifier summary run failed:', err);
                    setSourceWarnings(
                        'summaryRun',
                        ['Error running mismatch summary: ' + err.message]
                    );
                    global.miUpdateVerifierWarnings(slots, state.warnings);
                })
                .finally(function () {
                    state.summaryRunInFlight = false;
                    updateSummarySection();
                });
        }

        global.miRunVerifierSummary = runVerifierSummary;

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
            metadataMs = newBaseSec * metadataRatio * 1000;
            if (enabled.progress) startPoller('progress', config.urls.progress, progressMs);
            if (enabled.metadata) startPoller('metadata', config.urls.metadata, metadataMs);
            if (typeof config.onRefreshLabelUpdate === 'function') {
                config.onRefreshLabelUpdate(progressMs / 1000, metadataMs / 1000);
            }
        }

        function onRefreshChanged(e) {
            var newSec = e.detail && e.detail.refreshSec;
            if (newSec > 0) restartTimers(newSec);
        }

        function teardown() {
            ['progress', 'metadata'].forEach(stopPoller);
            global.miRunVerifierSummary = null;
            window.removeEventListener('mi-refresh-changed', onRefreshChanged);
            window.removeEventListener('pagehide', teardown);
        }

        startPoller('progress', config.urls.progress, progressMs);
        startPoller('metadata', config.urls.metadata, metadataMs);

        window.addEventListener('mi-refresh-changed', onRefreshChanged);
        window.addEventListener('pagehide', teardown);

        return { refreshAllSections: refreshAllSections, teardown: teardown, runVerifierSummary: runVerifierSummary };
    }

    global.miInitVerifierPolling = miInitVerifierPolling;
})(typeof window !== 'undefined' ? window : this);
