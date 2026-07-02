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

    var generationExpanded = {};

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

        var subtitle = el(
            'p',
            'lm-page-subtitle',
            'Progress of the standalone migration-verifier tool. The final generation determines pass/fail.'
        );
        textBlock.appendChild(subtitle);

        toolbar.appendChild(textBlock);
        return toolbar;
    }

    function renderGenerationsOverview(generations) {
        if (!generations || generations.length === 0) {
            return card('Generation Overview', null, [
                el('p', 'lm-muted', 'No verification generations found.'),
            ]);
        }

        var table = el('table', 'lm-phase-times-table');
        var thead = el('thead');
        var headerRow = el('tr');
        ['Generation', 'Name', 'Total', 'Completed', 'Failed', 'Start Time'].forEach(function (h) {
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
            tr.appendChild(el('td', null, g.startTime || '—'));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        return card('Generation Overview', null, [table]);
    }

    function renderFailedTasksTable(failedTasks) {
        if (!failedTasks || failedTasks.length === 0) {
            return banner('info', 'No failed or mismatched tasks.');
        }

        var table = el('table', 'lm-phase-times-table');
        var thead = el('thead');
        var headerRow = el('tr');
        ['Type', 'Source NS', 'Dest NS', 'Mismatch Details'].forEach(function (h) {
            headerRow.appendChild(el('th', null, h));
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = el('tbody');
        failedTasks.forEach(function (row) {
            var tr = el('tr');
            tr.appendChild(el('td', null, row.type || '—'));
            tr.appendChild(el('td', null, row.sourceNs || '—'));
            tr.appendChild(el('td', null, row.destNs || '—'));
            tr.appendChild(el('td', null, row.details || '—'));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        return table;
    }

    function renderGenerationDetail(genDetail) {
        var genKey = String(genDetail.num);
        var defaultExpanded = genDetail.isFinal || generationExpanded[genKey] === true;
        if (generationExpanded[genKey] === undefined) {
            generationExpanded[genKey] = defaultExpanded;
        }

        var block = el('div', 'lm-natural-order-block');
        var toggle = el('button', 'lm-natural-order-toggle lm-muted');
        toggle.type = 'button';
        toggle.setAttribute('aria-expanded', generationExpanded[genKey] ? 'true' : 'false');

        var chevron = el('span', 'lm-natural-order-chevron', generationExpanded[genKey] ? '▾' : '▸');
        toggle.appendChild(chevron);
        toggle.appendChild(document.createTextNode(genDetail.name || ('Generation ' + genDetail.num)));

        var details = el(
            'div',
            'lm-natural-order-details' + (generationExpanded[genKey] ? ' is-open' : '')
        );

        var phaseRow = el('div', 'lm-phase-row');
        var phaseText = el('span');
        phaseText.appendChild(document.createTextNode('Progress: '));
        phaseText.appendChild(el('b', null, (genDetail.percentComplete != null ? genDetail.percentComplete.toFixed(1) : '0') + '%'));
        phaseRow.appendChild(phaseText);
        details.appendChild(phaseRow);
        details.appendChild(progressBar(genDetail.percentComplete, false));

        var metrics = el('div', 'lm-metrics');
        [
            { label: 'Completed', value: formatCount(genDetail.completed) },
            { label: 'Failed', value: formatCount(genDetail.failed) },
            { label: 'Pending', value: formatCount(genDetail.pending) },
        ].forEach(function (m) {
            metrics.appendChild(metricTile(m));
        });
        details.appendChild(metrics);
        details.appendChild(renderFailedTasksTable(genDetail.failedTasks));

        toggle.addEventListener('click', function () {
            generationExpanded[genKey] = !generationExpanded[genKey];
            toggle.setAttribute('aria-expanded', generationExpanded[genKey] ? 'true' : 'false');
            chevron.textContent = generationExpanded[genKey] ? '▾' : '▸';
            details.classList.toggle('is-open', generationExpanded[genKey]);
        });

        block.appendChild(toggle);
        block.appendChild(details);
        return block;
    }

    function renderGenerationDetails(generationDetails) {
        if (!generationDetails || generationDetails.length === 0) {
            return null;
        }

        var body = el('div', 'lm-stack-tight');
        generationDetails.forEach(function (genDetail) {
            body.appendChild(renderGenerationDetail(genDetail));
        });

        return card(
            'Generation Details',
            'Expand a generation to view progress and failed tasks.',
            [body]
        );
    }

    function renderNamespaces(namespaces) {
        if (!namespaces || namespaces.length === 0) {
            return card('Namespace Progress', 'All-time task counts grouped by namespace.', [
                el('p', 'lm-muted', 'No namespace data available.'),
            ]);
        }

        var table = el('table', 'lm-phase-times-table');
        var thead = el('thead');
        var headerRow = el('tr');
        ['Namespace', 'Completed', 'Failed', 'Pending', 'Total'].forEach(function (h) {
            headerRow.appendChild(el('th', null, h));
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = el('tbody');
        namespaces.forEach(function (ns) {
            var tr = el('tr');
            tr.appendChild(el('td', null, ns.name || '—'));
            tr.appendChild(el('td', null, formatCount(ns.completed)));
            tr.appendChild(el('td', null, formatCount(ns.failed)));
            tr.appendChild(el('td', null, formatCount(ns.pending)));
            tr.appendChild(el('td', null, formatCount(ns.total)));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        return card(
            'Namespace Progress',
            'All-time task counts grouped by namespace (top 25 by failures).',
            [table]
        );
    }

    function renderCollectionMismatches(display) {
        var mismatches = display.collectionMismatches || [];
        var finalName = display.finalGenerationName || 'final generation';

        if (mismatches.length === 0) {
            return card(
                'Collection Metadata',
                'Collection/index metadata mismatches from the final generation only.',
                [banner('info', 'No collection metadata mismatches in ' + finalName + '.')]
            );
        }

        var table = el('table', 'lm-phase-times-table');
        var thead = el('thead');
        var headerRow = el('tr');
        ['Generation', 'Namespace', 'Index/Metadata Issues'].forEach(function (h) {
            headerRow.appendChild(el('th', null, h));
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = el('tbody');
        mismatches.forEach(function (row) {
            var tr = el('tr');
            tr.appendChild(el('td', null, row.generation || '—'));
            tr.appendChild(el('td', null, row.namespace || '—'));
            tr.appendChild(el('td', null, row.details || '—'));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        return card(
            'Collection Metadata',
            'Collection/index metadata mismatches from the final generation only.',
            [table]
        );
    }

    function renderConnectivity(conn) {
        if (!conn || !conn.rows || conn.rows.length === 0) return null;
        var body = el('div', 'lm-connectivity');
        conn.rows.forEach(function (r) {
            body.appendChild(kvRow(r.label, r.value));
        });
        return card(conn.title || 'Connectivity', null, [body]);
    }

    function renderVerifierMonitor(root, payload) {
        if (!root) return;
        root.replaceChildren();

        if (payload.error && !payload.display) {
            var shell = el('div', 'lm-stack');
            shell.appendChild(banner('danger', payload.error));
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
            var infoConn = renderConnectivity(payload.connectivity);
            if (infoConn) infoShell.appendChild(infoConn);
            root.appendChild(infoShell);
            return;
        }

        var display = payload.display;

        root.appendChild(renderToolbar(display));

        var stack = el('div', 'lm-stack lm-stack-after-toolbar');
        stack.appendChild(renderGenerationsOverview(display.generations));

        var genDetailsCard = renderGenerationDetails(display.generationDetails);
        if (genDetailsCard) stack.appendChild(genDetailsCard);

        stack.appendChild(renderNamespaces(display.namespaces));
        stack.appendChild(renderCollectionMismatches(display));

        var connCard = renderConnectivity(payload.connectivity);
        if (connCard) stack.appendChild(connCard);

        root.appendChild(stack);
    }

    global.miRenderVerifierMonitor = renderVerifierMonitor;
})(typeof window !== 'undefined' ? window : this);
