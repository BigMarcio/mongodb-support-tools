/**
 * Migration Verifier monitoring dashboard renderer.
 */
(function (global) {
    'use strict';

    var BANNER_ICONS = {
        info: 'i',
        warning: '!',
        danger: '✕',
        success: '✓',
    };

    function el(tag, className, text) {
        var node = document.createElement(tag);
        if (className) node.className = className;
        if (text !== undefined && text !== null) node.textContent = text;
        return node;
    }

    function badge(label, color, withDot) {
        var span = el('span', 'lm-badge ' + (color || 'gray'));
        if (withDot) span.appendChild(el('span', 'lm-dot'));
        span.appendChild(document.createTextNode(label || '—'));
        return span;
    }

    function banner(variant, message) {
        var div = el('div', 'lm-banner ' + variant);
        div.appendChild(el('span', 'lm-ico', BANNER_ICONS[variant] || 'i'));
        div.appendChild(el('div', 'lm-banner-body', message));
        return div;
    }

    function progressBar(percent, indeterminate) {
        var wrap = el('div', 'lm-progress' + (indeterminate ? ' indeterminate' : ''));
        var fill = el('div', 'lm-fill');
        if (!indeterminate && percent != null) {
            fill.style.width = Math.max(0, Math.min(100, percent)) + '%';
        }
        wrap.appendChild(fill);
        return wrap;
    }

    function metricTile(item) {
        var box = el('div', 'lm-metric');
        box.appendChild(el('div', 'lm-mlabel', item.label));
        var valueClass = 'lm-mvalue' + (item.small ? ' small' : '');
        if (item.badge) {
            var val = el('div', valueClass);
            val.appendChild(badge(item.value, item.badge, false));
            box.appendChild(val);
        } else {
            box.appendChild(el('div', valueClass, String(item.value)));
        }
        return box;
    }

    function card(title, desc, bodyChildren) {
        var section = el('section', 'lm-card');
        if (title) section.appendChild(el('h2', null, title));
        if (desc) section.appendChild(el('p', 'lm-card-desc', desc));
        var body = el('div', 'lm-card-body');
        (bodyChildren || []).forEach(function (c) {
            if (c) body.appendChild(c);
        });
        section.appendChild(body);
        return section;
    }

    function cardWithTitleElement(titleEl, desc, bodyChildren, bodyClassName) {
        var section = el('section', 'lm-card');
        if (titleEl) section.appendChild(titleEl);
        if (desc) section.appendChild(el('p', 'lm-card-desc', desc));
        var body = el('div', 'lm-card-body' + (bodyClassName ? ' ' + bodyClassName : ''));
        (bodyChildren || []).forEach(function (c) {
            if (c) body.appendChild(c);
        });
        section.appendChild(body);
        return section;
    }

    function kvRow(label, value) {
        var rowEl = el('div', 'lm-kv');
        rowEl.appendChild(el('span', 'lm-k', label));
        rowEl.appendChild(el('span', 'lm-v', value));
        return rowEl;
    }

    function formatCount(n) {
        if (n == null || n === '') return '—';
        try {
            return Number(n).toLocaleString('en-US');
        } catch (e) {
            return String(n);
        }
    }

    function appendLabeledProgress(parent, label, percent, countLabel) {
        var row = el('div', 'lm-phase-row');
        var text = el('span');
        text.appendChild(document.createTextNode(label + ': '));
        if (countLabel) {
            text.appendChild(el('b', null, countLabel));
            if (percent != null) {
                text.appendChild(el('span', 'lm-muted', ' (' + percent.toFixed(1) + '%)'));
            } else {
                text.appendChild(el('span', 'lm-muted', ' (—)'));
            }
        } else {
            var pctText = percent != null ? percent.toFixed(1) + '%' : '—';
            text.appendChild(el('b', null, pctText));
        }
        row.appendChild(text);
        parent.appendChild(row);
        parent.appendChild(progressBar(percent, percent == null));
    }

    function renderToolbar(display) {
        var toolbar = el('div', 'lm-toolbar');
        var textBlock = el('div', 'lm-toolbar-text');

        var title = el('h1', 'lm-page-title');
        title.appendChild(document.createTextNode('Migration Verifier Monitoring'));
        if (display && display.verificationProgress && display.verificationProgress.phaseBadge) {
            title.appendChild(
                badge(
                    display.verificationProgress.phaseBadge.label,
                    display.verificationProgress.phaseBadge.color,
                    true
                )
            );
        }
        if (display && display.stateBadge) {
            title.appendChild(
                badge(display.stateBadge.label, display.stateBadge.color, true)
            );
        }
        textBlock.appendChild(title);

        toolbar.appendChild(textBlock);
        return toolbar;
    }

    function formatComparedTotal(compared, total) {
        if (!total) return '—';
        return formatCount(compared) + ' / ' + formatCount(total);
    }

    function formatRate(value, suffix) {
        if (value == null || value === '') return '—';
        try {
            var n = Number(value);
            if (!isFinite(n)) return '—';
            return n.toLocaleString('en-US', { maximumFractionDigits: 1 }) + (suffix || '');
        } catch (e) {
            return String(value);
        }
    }

    function formatLagSecs(seconds) {
        if (seconds == null || seconds === '') return '—';
        var n = Number(seconds);
        if (!isFinite(n) || n < 0) return '—';
        if (n < 60) return n + 's';
        var m = Math.floor(n / 60);
        var s = n % 60;
        return m + 'm ' + s + 's';
    }

    var gen0DetailsExpanded = false;

    function renderChangeStatsSection(changeStats) {
        if (!changeStats) return null;
        var block = el('div', 'lm-verifier-change-stream');
        block.appendChild(el('div', 'lm-verifier-section-label', changeStats.label + ' change stream'));

        var metrics = el('div', 'lm-metrics lm-verifier-metrics');
        [
            { label: 'Lag', value: formatLagSecs(changeStats.lagSecs) },
            { label: 'Events/sec', value: formatRate(changeStats.eventsPerSecond) },
            {
                label: 'Buffer saturation',
                value: changeStats.bufferSaturation != null
                    ? (Number(changeStats.bufferSaturation) * 100).toFixed(1) + '%'
                    : '—',
            },
        ].forEach(function (m) {
            metrics.appendChild(metricTile(m));
        });
        block.appendChild(metrics);

        var counts = changeStats.eventCounts || {};
        var countsRow = el('div', 'lm-muted lm-verifier-events-line');
        countsRow.textContent =
            'Events: insert ' + formatCount(counts.insert) +
            ', update ' + formatCount(counts.update) +
            ', replace ' + formatCount(counts.replace) +
            ', delete ' + formatCount(counts.delete);
        block.appendChild(countsRow);
        return block;
    }

    function renderGen0StatsBlock(progress) {
        if (!progress.gen0Stats) return null;

        var gen0 = progress.gen0Stats;
        var hiddenByDefault = progress.generation != null && progress.generation !== 0;
        var isOpen = gen0DetailsExpanded || !hiddenByDefault;

        var block = el('div', 'lm-verifier-gen0-block');
        var toggle = el('button', 'lm-phase-times-toggle lm-muted');
        toggle.type = 'button';
        toggle.setAttribute('aria-expanded', isOpen ? 'true' : 'false');

        var chevron = el('span', 'lm-phase-times-chevron', isOpen ? '▾' : '▸');
        toggle.appendChild(chevron);
        toggle.appendChild(document.createTextNode('Initial check (generation 0)'));

        var details = el('div', 'lm-phase-times-details lm-verifier-gen0-details' + (isOpen ? ' is-open' : ''));
        appendLabeledProgress(
            details,
            'Documents',
            gen0.totalDocs > 0 ? gen0.docsPercent : null,
            formatComparedTotal(gen0.docsCompared, gen0.totalDocs)
        );
        if (gen0.totalSrcBytes > 0) {
            appendLabeledProgress(
                details,
                'Source bytes',
                gen0.bytesPercent,
                formatComparedTotal(gen0.srcBytesCompared, gen0.totalSrcBytes)
            );
        }

        toggle.addEventListener('click', function () {
            gen0DetailsExpanded = !gen0DetailsExpanded;
            toggle.setAttribute('aria-expanded', gen0DetailsExpanded ? 'true' : 'false');
            chevron.textContent = gen0DetailsExpanded ? '▾' : '▸';
            details.classList.toggle('is-open', gen0DetailsExpanded);
        });

        block.appendChild(toggle);
        block.appendChild(details);
        return block;
    }

    function renderVerificationProgressCard(progress) {
        if (!progress) return null;

        var body = el('div', 'lm-stack-tight lm-verifier-progress-body');

        if (progress.generation != null) {
            body.appendChild(
                el('div', 'lm-verifier-section-label', 'Generation ' + progress.generation)
            );
        }

        var docs = progress.documents || {};
        appendLabeledProgress(
            body,
            'Documents',
            docs.total > 0 ? docs.percent : null,
            formatComparedTotal(docs.compared, docs.total)
        );

        var bytes = progress.bytes || {};
        if (bytes.total > 0) {
            appendLabeledProgress(
                body,
                'Source bytes',
                bytes.percent,
                formatComparedTotal(bytes.compared, bytes.total)
            );
        }

        var metrics = el('div', 'lm-metrics lm-verifier-metrics');
        var tasks = progress.tasks || {};
        [
            { label: 'Namespaces', value: formatCount(progress.totalNamespaces) },
            { label: 'Tasks total', value: formatCount(tasks.total) },
            { label: 'Tasks pending', value: formatCount(tasks.added) },
            { label: 'Tasks processing', value: formatCount(tasks.processing) },
            { label: 'Tasks failed', value: formatCount(tasks.failed) },
            { label: 'Tasks completed', value: formatCount(tasks.completed) },
            { label: 'Metadata mismatches', value: formatCount(tasks.metadataMismatch) },
            { label: 'Docs/sec', value: formatRate(progress.docsComparedPerSecond) },
            { label: 'Bytes/sec', value: formatRate(progress.srcBytesComparedPerSecond) },
        ].forEach(function (m) {
            metrics.appendChild(metricTile(m));
        });
        body.appendChild(metrics);

        var srcChange = renderChangeStatsSection(progress.srcChangeStats);
        if (srcChange) body.appendChild(srcChange);
        var dstChange = renderChangeStatsSection(progress.dstChangeStats);
        if (dstChange) body.appendChild(dstChange);

        if (
            progress.srcLastRecheckedTS ||
            progress.dstLastRecheckedTS ||
            progress.totalRechecksDone > 0 ||
            (progress.recentRecheckSecs && progress.recentRecheckSecs.length > 0)
        ) {
            var recheckBlock = el('div', 'lm-verifier-recheck-block');
            recheckBlock.appendChild(el('div', 'lm-verifier-section-label', 'Recheck'));
            if (progress.srcLastRecheckedTS) {
                recheckBlock.appendChild(
                    kvRow('Source last rechecked', progress.srcLastRecheckedTS)
                );
            }
            if (progress.dstLastRecheckedTS) {
                recheckBlock.appendChild(
                    kvRow('Destination last rechecked', progress.dstLastRecheckedTS)
                );
            }
            if (progress.totalRechecksDone > 0) {
                recheckBlock.appendChild(
                    kvRow('Total rechecks done', formatCount(progress.totalRechecksDone))
                );
            }
            if (progress.recentRecheckSecs && progress.recentRecheckSecs.length > 0) {
                recheckBlock.appendChild(
                    kvRow(
                        'Recent recheck durations (s)',
                        progress.recentRecheckSecs.join(', ')
                    )
                );
            }
            body.appendChild(recheckBlock);
        }

        var gen0Block = renderGen0StatsBlock(progress);
        if (gen0Block) body.appendChild(gen0Block);

        if (progress.longestDocMismatch) {
            var mm = progress.longestDocMismatch;
            var detail = mm.namespace || '—';
            if (mm.type) detail += ' (' + mm.type + ')';
            if (mm.durationSecs != null) {
                detail += ' — ' + mm.durationSecs.toFixed(1) + 's';
            }
            body.appendChild(banner('warning', 'Longest-lived mismatch: ' + detail));
        }

        if (progress.error) {
            body.appendChild(banner('danger', 'Verifier error: ' + progress.error));
        }

        var title = el('h2', 'lm-card-title-row');
        title.appendChild(document.createTextNode('Verification Progress'));

        return cardWithTitleElement(
            title,
            progress.phaseDescription || null,
            [body],
            'lm-verifier-progress-card-body'
        );
    }

    function generationOverviewTitle(generationLimit) {
        var title = 'Generation Overview';
        if (generationLimit != null) {
            title += ' (Last ' + generationLimit + ')';
        }
        return title;
    }

    function renderGenerationsOverview(generations, generationLimit) {
        var title = generationOverviewTitle(generationLimit);

        if (!generations || generations.length === 0) {
            return card(title, null, [
                el('p', 'lm-muted', 'No verification generations found.'),
            ]);
        }

        var desc = 'Total, Completed, Failed, and Pending are verification task counts ' +
            '(excluding the coordinator primary task). Documents and Partitions show ' +
            'metadata progress (compared or finished vs total).';

        var table = el('table', 'lm-phase-times-table');
        var thead = el('thead');
        var headerRow = el('tr');
        [
            'Generation', 'Name', 'Total', 'Completed', 'Failed', 'Pending',
            'Documents', 'Partitions', 'Start Time',
        ].forEach(function (h) {
            headerRow.appendChild(el('th', null, h));
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = el('tbody');
        generations.forEach(function (g) {
            var tr = el('tr');
            tr.appendChild(el('td', null, String(g.num)));
            tr.appendChild(el('td', null, g.name || '—'));
            tr.appendChild(el('td', null, formatCount(g.total)));
            tr.appendChild(el('td', null, formatCount(g.completed)));
            tr.appendChild(el('td', null, formatCount(g.failed)));
            tr.appendChild(el('td', null, formatCount(g.pending)));
            tr.appendChild(el('td', null, formatComparedTotal(g.docsCompared, g.totalDocs)));
            tr.appendChild(el('td', null, formatComparedTotal(g.partitionsDone, g.partitionsTotal)));
            tr.appendChild(el('td', null, g.startTime || '—'));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        return card(title, desc, [table]);
    }

    function hasMetadata(display) {
        return display && display.metadataAvailable === true;
    }

    function hasPreviousGeneration(display) {
        return display && display.previousGeneration != null;
    }

    function previousGenerationUnavailableCard(title, desc) {
        return card(title, desc, [
            banner('info', 'No previous generation available (current is generation 0).'),
        ]);
    }

    function renderFailedTasks(display) {
        var failedTasks = display.failedTasks || [];
        var limit = display.failedTasksLimit;
        var desc = 'Document verification failures and operational task errors from the ' +
            'previous generation (collection metadata mismatches are listed separately).';
        if (limit) {
            desc += ' Showing up to ' + limit + ' most recent rows.';
        }

        if (!hasPreviousGeneration(display)) {
            return previousGenerationUnavailableCard(
                'Failed Tasks / Document Mismatches',
                desc
            );
        }

        if (failedTasks.length === 0) {
            return card(
                'Failed Tasks / Document Mismatches',
                desc,
                [banner('info', 'No failed document verification tasks in the previous generation.')]
            );
        }

        var table = el('table', 'lm-phase-times-table');
        var thead = el('thead');
        var headerRow = el('tr');
        ['Namespace', 'Type', 'Status', 'Details', 'Time'].forEach(function (h) {
            headerRow.appendChild(el('th', null, h));
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = el('tbody');
        failedTasks.forEach(function (row) {
            var tr = el('tr');
            tr.appendChild(el('td', null, row.namespace || '—'));
            tr.appendChild(el('td', null, row.type || '—'));
            tr.appendChild(el('td', null, row.status || '—'));
            tr.appendChild(el('td', null, row.details || '—'));
            tr.appendChild(el('td', null, row.beginTime || '—'));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        return card('Failed Tasks / Document Mismatches', desc, [table]);
    }

    function renderCompletenessCard(completeness, options) {
        if (!completeness) return null;

        var opts = options || {};
        var title = opts.title || 'Verification completeness';
        var desc = opts.desc;
        if (!desc) {
            desc = completeness.isRecheckGeneration
                ? 'Recheck progress (documents scheduled for recheck, not full cluster size).'
                : 'Initial check progress for the current generation.';
        }

        var body = el('div', 'lm-stack-tight');

        var docs = completeness.documents || {};
        appendLabeledProgress(
            body,
            'Documents',
            docs.total > 0 ? docs.percent : null,
            formatCount(docs.compared) + ' / ' + formatCount(docs.total)
        );

        var nss = completeness.namespaces || {};
        appendLabeledProgress(
            body,
            'Namespaces',
            nss.total > 0 ? nss.percent : null,
            formatCount(nss.complete) + ' / ' + formatCount(nss.total)
        );

        var parts = completeness.partitions || {};
        appendLabeledProgress(
            body,
            'Partitions',
            parts.total > 0 ? parts.percent : null,
            formatCount(parts.done) + ' / ' + formatCount(parts.total)
        );

        if (completeness.bytes && completeness.bytes.total > 0) {
            var bytes = completeness.bytes;
            appendLabeledProgress(
                body,
                'Bytes',
                bytes.percent,
                formatCount(bytes.compared) + ' / ' + formatCount(bytes.total)
            );
        }

        var tasks = completeness.tasks || {};
        var metrics = el('div', 'lm-metrics');
        [
            { label: 'Tasks pending', value: formatCount(tasks.pending) },
            { label: 'Tasks failed', value: formatCount(tasks.failed) },
            { label: 'Tasks completed', value: formatCount(tasks.completed) },
        ].forEach(function (m) {
            metrics.appendChild(metricTile(m));
        });
        body.appendChild(metrics);

        return card(title, desc, [body]);
    }

    function renderVerificationCompleteness(display) {
        if (!display || !display.verificationCompleteness) return [];

        var title = display.currentGeneration > 0
            ? 'Current generation completeness'
            : 'Verification completeness';
        var card = renderCompletenessCard(display.verificationCompleteness, { title: title });
        return card ? [card] : [];
    }

    function renderNamespaces(namespaces, display) {
        var desc = 'Document progress by namespace for the previous generation.';

        if (!hasPreviousGeneration(display)) {
            return previousGenerationUnavailableCard('Namespace Progress', desc);
        }

        if (!namespaces || namespaces.length === 0) {
            return card('Namespace Progress', desc, [
                el('p', 'lm-muted', 'No namespace data available for the previous generation.'),
            ]);
        }

        var table = el('table', 'lm-phase-times-table');
        var thead = el('thead');
        var headerRow = el('tr');
        ['Namespace', 'Docs Compared', 'Total Docs', 'Partitions Done', 'Partitions Pending'].forEach(function (h) {
            headerRow.appendChild(el('th', null, h));
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = el('tbody');
        namespaces.forEach(function (ns) {
            var tr = el('tr');
            tr.appendChild(el('td', null, ns.name || '—'));
            tr.appendChild(el('td', null, formatCount(ns.docsCompared)));
            tr.appendChild(el('td', null, formatCount(ns.totalDocs)));
            tr.appendChild(el('td', null, formatCount(ns.partitionsDone)));
            tr.appendChild(el('td', null, formatCount(ns.partitionsPending)));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        return card('Namespace Progress', desc, [table]);
    }

    function renderCollectionMismatches(display) {
        var mismatches = display.collectionMismatches || [];
        var desc = 'Collection/index metadata mismatches from the previous generation.';

        if (!hasPreviousGeneration(display)) {
            return previousGenerationUnavailableCard('Collection Metadata', desc);
        }

        if (mismatches.length === 0) {
            return card(
                'Collection Metadata',
                desc,
                [banner('info', 'No collection metadata mismatches in the previous generation.')]
            );
        }

        var table = el('table', 'lm-phase-times-table');
        var thead = el('thead');
        var headerRow = el('tr');
        ['Namespace', 'Index/Metadata Issues'].forEach(function (h) {
            headerRow.appendChild(el('th', null, h));
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = el('tbody');
        mismatches.forEach(function (row) {
            var tr = el('tr');
            tr.appendChild(el('td', null, row.namespace || '—'));
            tr.appendChild(el('td', null, row.details || '—'));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        return card('Collection Metadata', desc, [table]);
    }

    function renderConnectivity(conn) {
        if (!conn || !conn.rows || conn.rows.length === 0) return null;
        var body = el('div', 'lm-connectivity');
        conn.rows.forEach(function (r) {
            body.appendChild(kvRow(r.label, r.value));
        });
        return card(conn.title || 'Connectivity', null, [body]);
    }

    function renderWarningsCard(warnings) {
        if (!warnings || warnings.length === 0) return null;
        var tight = el('div', 'lm-stack-tight');
        warnings.forEach(function (w) {
            tight.appendChild(banner('warning', w));
        });
        return card('Warnings', null, [tight]);
    }

    function appendWarnings(parent, warnings) {
        var warnCard = renderWarningsCard(warnings);
        if (warnCard) parent.appendChild(warnCard);
    }

    function renderVerifierMonitor(root, payload) {
        if (!root) return;
        root.replaceChildren();

        if (payload.error && !payload.display) {
            var shell = el('div', 'lm-stack');
            shell.appendChild(banner('danger', payload.error));
            appendWarnings(shell, payload.warnings);
            var errConn = renderConnectivity(payload.connectivity);
            if (errConn) shell.appendChild(errConn);
            root.appendChild(shell);
            return;
        }

        if (!payload.display) {
            var infoShell = el('div', 'lm-stack');
            infoShell.appendChild(
                banner(
                    'info',
                    payload.error || 'No verifier data available.'
                )
            );
            appendWarnings(infoShell, payload.warnings);
            var infoConn = renderConnectivity(payload.connectivity);
            if (infoConn) infoShell.appendChild(infoConn);
            root.appendChild(infoShell);
            return;
        }

        var display = payload.display;

        root.appendChild(renderToolbar(display));

        var stack = el('div', 'lm-stack lm-stack-after-toolbar');

        var progressCard = renderVerificationProgressCard(display.verificationProgress);
        if (progressCard) {
            stack.appendChild(progressCard);
        }

        appendWarnings(stack, payload.warnings);

        if (hasMetadata(display)) {
            renderVerificationCompleteness(display).forEach(function (completenessCard) {
                stack.appendChild(completenessCard);
            });

            stack.appendChild(renderGenerationsOverview(
                display.generations,
                display.generationLimit
            ));

            stack.appendChild(renderNamespaces(display.namespaces, display));

            stack.appendChild(renderFailedTasks(display));

            stack.appendChild(renderCollectionMismatches(display));
        }

        var connCard = renderConnectivity(payload.connectivity);
        if (connCard) stack.appendChild(connCard);

        root.appendChild(stack);
    }

    global.miRenderVerifierMonitor = renderVerifierMonitor;
})(typeof window !== 'undefined' ? window : this);
