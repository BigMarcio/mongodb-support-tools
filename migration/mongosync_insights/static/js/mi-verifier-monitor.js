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
        var desc = 'Document verification failures and operational task errors from the ' +
            'previous generation (collection metadata mismatches are listed separately).';

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

        appendWarnings(stack, payload.warnings);

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

        var connCard = renderConnectivity(payload.connectivity);
        if (connCard) stack.appendChild(connCard);

        root.appendChild(stack);
    }

    global.miRenderVerifierMonitor = renderVerifierMonitor;
})(typeof window !== 'undefined' ? window : this);
