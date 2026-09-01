/**
 * Live Monitoring tab renderer.
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

    function applyHoverTitle(node, title) {
        if (!node || !title) return node;
        node.classList.add('lm-has-hover');
        if (title.indexOf('\n') !== -1) {
            node.classList.add('lm-has-hover-multiline');
        }
        node.setAttribute('data-hover-title', title);
        return node;
    }

    function metricTile(item) {
        var box = el('div', 'lm-metric');
        box.appendChild(el('div', 'lm-mlabel', item.label));
        var valueClass = 'lm-mvalue' + (item.small ? ' small' : '') + (item.highLag ? ' lm-lag-high' : '');
        if (item.badge) {
            var val = el('div', valueClass);
            val.appendChild(badge(item.value, item.badge, false));
            box.appendChild(val);
        } else {
            box.appendChild(el('div', valueClass, String(item.value)));
        }
        applyHoverTitle(box, item.title);
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

    function migrationProgressTitle(sync) {
        var title = el('h2', 'lm-card-title-row lm-migration-progress-title');
        title.appendChild(document.createTextNode(sync.cardTitle || 'Migration Progress'));
        if (sync.showMongosyncVersion) {
            var versionWrap = el('span', 'lm-mongosync-version');
            versionWrap.appendChild(document.createTextNode('Mongosync Version: '));
            var versionValue = sync.mongosyncVersion || '—';
            versionWrap.appendChild(el('span', 'lm-mongosync-version-value', versionValue));
            if (!sync.mongosyncVersion && sync.mongosyncVersionMissingTitle) {
                applyHoverTitle(versionWrap, sync.mongosyncVersionMissingTitle);
            }
            title.appendChild(versionWrap);
        }
        return title;
    }

    function kvRow(label, value, title) {
        var rowEl = el('div', 'lm-kv');
        rowEl.appendChild(el('span', 'lm-k', label));
        var valueEl = el('span', 'lm-v', value);
        applyHoverTitle(valueEl, title);
        rowEl.appendChild(valueEl);
        return rowEl;
    }

    function indexSummaryLine(idx) {
        var line = el('span');
        line.appendChild(el('b', null, formatCount(idx.built)));
        line.appendChild(document.createTextNode(' of '));
        line.appendChild(el('b', null, formatCount(idx.total)));
        line.appendChild(document.createTextNode(' indexes built'));
        return line;
    }

    function formatCount(n) {
        if (n == null || n === '') return '—';
        try {
            return Number(n).toLocaleString('en-US');
        } catch (e) {
            return String(n);
        }
    }

    var copyDetailsExpanded = false;
    var phaseStartTimesExpanded = false;
    var lagBreakdownExpanded = false;
    var naturalOrderExpanded = false;
    var inclusionFilterExpanded = false;
    var exclusionFilterExpanded = false;

    function buildFilterMigrationSection(section, getExpanded, setExpanded) {
        var block = el('div', 'lm-filter-migration-section');
        var toggle = el('button', 'lm-filter-migration-toggle lm-muted');
        toggle.type = 'button';
        toggle.setAttribute('aria-expanded', getExpanded() ? 'true' : 'false');

        var label = section.label || 'Filter';
        var chevron = el('span', 'lm-filter-migration-chevron', getExpanded() ? '▾' : '▸');
        toggle.appendChild(chevron);
        toggle.appendChild(document.createTextNode(label));

        var details = el(
            'div',
            'lm-filter-migration-details' + (getExpanded() ? ' is-open' : '')
        );

        var table = el('table', 'lm-filter-migration-table');
        var thead = el('thead');
        var headerRow = el('tr');
        headerRow.appendChild(el('th', null, 'Key'));
        headerRow.appendChild(el('th', null, 'Value'));
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = el('tbody');
        (section.rows || []).forEach(function (row) {
            var tr = el('tr');
            tr.appendChild(el('td', null, row.key || '—'));
            tr.appendChild(el('td', null, row.value || '—'));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        details.appendChild(table);

        toggle.addEventListener('click', function () {
            setExpanded(!getExpanded());
            toggle.setAttribute('aria-expanded', getExpanded() ? 'true' : 'false');
            chevron.textContent = getExpanded() ? '▾' : '▸';
            details.classList.toggle('is-open', getExpanded());
        });

        block.appendChild(toggle);
        block.appendChild(details);
        return block;
    }

    function renderFilteredMigration(data) {
        if (!data) {
            return null;
        }

        var body = el('div', 'lm-filter-migration-block');
        if (data.inclusion) {
            body.appendChild(
                buildFilterMigrationSection(
                    data.inclusion,
                    function () {
                        return inclusionFilterExpanded;
                    },
                    function (value) {
                        inclusionFilterExpanded = value;
                    }
                )
            );
        }
        if (data.exclusion) {
            body.appendChild(
                buildFilterMigrationSection(
                    data.exclusion,
                    function () {
                        return exclusionFilterExpanded;
                    },
                    function (value) {
                        exclusionFilterExpanded = value;
                    }
                )
            );
        }

        return card(data.title || 'Filtered migration', null, [body]);
    }

    function renderNaturalOrder(data) {
        if (!data || !data.rows || data.rows.length === 0) {
            return null;
        }

        var block = el('div', 'lm-natural-order-block');
        var toggle = el('button', 'lm-natural-order-toggle lm-muted');
        toggle.type = 'button';
        toggle.setAttribute('aria-expanded', naturalOrderExpanded ? 'true' : 'false');

        var label = data.label || 'Natural order collections';
        var chevron = el('span', 'lm-natural-order-chevron', naturalOrderExpanded ? '▾' : '▸');
        toggle.appendChild(chevron);
        toggle.appendChild(document.createTextNode(label));

        var details = el(
            'div',
            'lm-natural-order-details' + (naturalOrderExpanded ? ' is-open' : '')
        );

        var table = el('table', 'lm-natural-order-table');
        var thead = el('thead');
        var headerRow = el('tr');
        headerRow.appendChild(el('th', null, 'Database'));
        headerRow.appendChild(el('th', null, 'Collections'));
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = el('tbody');
        data.rows.forEach(function (row) {
            var tr = el('tr');
            tr.appendChild(el('td', null, row.database || '—'));
            tr.appendChild(el('td', null, row.collections || '—'));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        details.appendChild(table);

        toggle.addEventListener('click', function () {
            naturalOrderExpanded = !naturalOrderExpanded;
            toggle.setAttribute('aria-expanded', naturalOrderExpanded ? 'true' : 'false');
            chevron.textContent = naturalOrderExpanded ? '▾' : '▸';
            details.classList.toggle('is-open', naturalOrderExpanded);
        });

        block.appendChild(toggle);
        block.appendChild(details);
        return card(data.title || 'Copy in natural order', data.description, [block]);
    }

    function phaseStartTimesBlock(sync, options) {
        options = options || {};
        var data = sync.phaseStartTimes;
        if (!data || !data.rows || data.rows.length === 0) {
            return null;
        }

        var block = el('div', 'lm-phase-times-block');
        var label = data.label || 'Phase start times';

        var table = el('table', 'lm-phase-times-table');
        var thead = el('thead');
        var headerRow = el('tr');
        headerRow.appendChild(el('th', null, 'Phase'));
        headerRow.appendChild(el('th', null, 'Started'));
        thead.appendChild(headerRow);
        table.appendChild(thead);

        var tbody = el('tbody');
        data.rows.forEach(function (row) {
            var tr = el('tr');
            tr.appendChild(el('td', null, row.phase || '—'));
            tr.appendChild(el('td', null, row.startedAt || '—'));
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);

        var alwaysShow = !!options.isSummaryView;
        var details = el(
            'div',
            'lm-phase-times-details' + (alwaysShow || phaseStartTimesExpanded ? ' is-open' : '')
        );
        details.appendChild(table);
        if (data.timezoneNote && !data.timezoneNoteBelowTitle) {
            details.appendChild(
                el('div', 'lm-muted lm-phase-times-note', 'Times in ' + data.timezoneNote)
            );
        }

        if (alwaysShow) {
            block.appendChild(el('div', 'lm-phase-times-heading lm-muted', label));
            if (data.timezoneNote && data.timezoneNoteBelowTitle) {
                block.appendChild(
                    el(
                        'div',
                        'lm-muted lm-phase-times-timezone lm-phase-times-timezone-below',
                        'Times in ' + data.timezoneNote
                    )
                );
            }
            block.appendChild(details);
            return block;
        }

        var toggle = el('button', 'lm-phase-times-toggle lm-muted');
        toggle.type = 'button';
        toggle.setAttribute('aria-expanded', phaseStartTimesExpanded ? 'true' : 'false');

        var chevron = el('span', 'lm-phase-times-chevron', phaseStartTimesExpanded ? '▾' : '▸');
        toggle.appendChild(chevron);
        toggle.appendChild(document.createTextNode(label));

        toggle.addEventListener('click', function () {
            phaseStartTimesExpanded = !phaseStartTimesExpanded;
            toggle.setAttribute('aria-expanded', phaseStartTimesExpanded ? 'true' : 'false');
            chevron.textContent = phaseStartTimesExpanded ? '▾' : '▸';
            details.classList.toggle('is-open', phaseStartTimesExpanded);
        });

        block.appendChild(toggle);
        if (data.timezoneNote && data.timezoneNoteBelowTitle) {
            block.appendChild(
                el(
                    'div',
                    'lm-muted lm-phase-times-timezone lm-phase-times-timezone-below',
                    'Times in ' + data.timezoneNote
                )
            );
        }
        block.appendChild(details);
        return block;
    }

    function copyPercentLabel(sync) {
        if (!sync.showCopyProgress || sync.copyPercent == null) {
            return null;
        }
        return el('span', 'lm-muted', sync.copyPercent.toFixed(1) + '%');
    }

    function copiedProgressBlock(sync, options) {
        options = options || {};
        var alwaysShow = !!options.isSummaryView;
        var hasDetails = !!(
            sync.collectionsCopiedLabel ||
            sync.partitionsCopiedLabel
        );
        if (!sync.copiedLabel && !hasDetails) {
            return null;
        }

        function appendCopyDetailLines(parent, useDetailClass) {
            var lineClass = useDetailClass ? 'lm-muted lm-copied-detail-line' : 'lm-muted lm-copied-line';
            if (sync.collectionsCopiedLabel) {
                parent.appendChild(el('div', lineClass, sync.collectionsCopiedLabel));
            }
            if (sync.partitionsCopiedLabel) {
                parent.appendChild(el('div', lineClass, sync.partitionsCopiedLabel));
            }
        }

        if (!sync.copiedLabel) {
            var fallback = el('div', 'lm-copied-block');
            appendCopyDetailLines(fallback, false);
            return fallback;
        }

        if (!hasDetails) {
            var simpleBlock = el('div', 'lm-copied-block');
            var simpleRow = el('div', 'lm-phase-row');
            var line = el('div', 'lm-muted lm-copied-line');
            line.style.marginTop = '0';
            line.appendChild(document.createTextNode(sync.copiedLabel));
            applyHoverTitle(line, sync.copiedTitle);
            simpleRow.appendChild(line);
            var simplePercent = copyPercentLabel(sync);
            if (simplePercent) {
                simpleRow.appendChild(simplePercent);
            }
            simpleBlock.appendChild(simpleRow);
            return simpleBlock;
        }

        var block = el('div', 'lm-copied-block');
        var copiedRow = el('div', 'lm-phase-row');
        var toggle = null;
        var chevron = null;

        if (alwaysShow) {
            var heading = el('span', 'lm-copied-toggle lm-muted lm-copied-toggle-static');
            heading.appendChild(document.createTextNode(sync.copiedLabel));
            applyHoverTitle(heading, sync.copiedTitle);
            copiedRow.appendChild(heading);
        } else {
            toggle = el('button', 'lm-copied-toggle lm-muted');
            toggle.type = 'button';
            toggle.setAttribute('aria-expanded', copyDetailsExpanded ? 'true' : 'false');

            chevron = el('span', 'lm-copied-chevron', copyDetailsExpanded ? '▾' : '▸');
            toggle.appendChild(chevron);
            toggle.appendChild(document.createTextNode(sync.copiedLabel));
            applyHoverTitle(toggle, sync.copiedTitle);
            copiedRow.appendChild(toggle);
        }

        var copiedPercent = copyPercentLabel(sync);
        if (copiedPercent) {
            copiedRow.appendChild(copiedPercent);
        }
        block.appendChild(copiedRow);

        var details = el(
            'div',
            'lm-copied-details' + (alwaysShow || copyDetailsExpanded ? ' is-open' : '')
        );
        appendCopyDetailLines(details, true);

        if (toggle) {
            toggle.addEventListener('click', function () {
                copyDetailsExpanded = !copyDetailsExpanded;
                toggle.setAttribute('aria-expanded', copyDetailsExpanded ? 'true' : 'false');
                chevron.textContent = copyDetailsExpanded ? '▾' : '▸';
                details.classList.toggle('is-open', copyDetailsExpanded);
            });
        }

        block.appendChild(details);
        return block;
    }

    function lagBreakdownBlock(sync) {
        var breakdown = sync.lagBreakdown;
        if (!breakdown || !breakdown.show) {
            return null;
        }

        var block = el('div', 'lm-copied-block lm-lag-breakdown-block');
        var toggle = el('button', 'lm-copied-toggle lm-muted');
        toggle.type = 'button';
        toggle.setAttribute('aria-expanded', lagBreakdownExpanded ? 'true' : 'false');

        var label = 'Lag breakdown';
        var chevron = el('span', 'lm-copied-chevron', lagBreakdownExpanded ? '▾' : '▸');
        toggle.appendChild(chevron);
        toggle.appendChild(document.createTextNode(label));

        var details = el(
            'div',
            'lm-copied-details' + (lagBreakdownExpanded ? ' is-open' : '')
        );

        var crudValue = breakdown.crud != null ? breakdown.crud : '—';
        details.appendChild(kvRow('CRUD lag', crudValue));

        var ddlValue = breakdown.ddl != null ? breakdown.ddl : '—';
        details.appendChild(kvRow('DDL lag', ddlValue));

        if (breakdown.ddlUnavailable) {
            details.appendChild(
                el(
                    'div',
                    'lm-muted lm-copied-detail-line',
                    'DDL applier disabled or no DDL events applied yet'
                )
            );
        }

        toggle.addEventListener('click', function () {
            lagBreakdownExpanded = !lagBreakdownExpanded;
            toggle.setAttribute('aria-expanded', lagBreakdownExpanded ? 'true' : 'false');
            chevron.textContent = lagBreakdownExpanded ? '▾' : '▸';
            details.classList.toggle('is-open', lagBreakdownExpanded);
        });

        block.appendChild(toggle);
        block.appendChild(details);
        return block;
    }

    function renderSync(sync, options) {
        if (!sync) return null;
        options = options || {};
        var phaseRow = el('div', 'lm-phase-row');
        var phaseText = el('span');
        phaseText.appendChild(document.createTextNode('Current Phase: '));
        phaseText.appendChild(el('b', null, sync.phase));
        phaseRow.appendChild(phaseText);

        var children = [phaseRow];
        var copiedBlock = copiedProgressBlock(sync, options);
        if (copiedBlock) {
            var copiedSection = el('div', 'lm-sync-subsection');
            copiedSection.appendChild(copiedBlock);
            children.push(copiedSection);
        }
        if (sync.showCopyProgress) {
            children.push(progressBar(sync.copyPercent, sync.copyIndeterminate));
        }

        var metrics = el('div', 'lm-metrics');
        (sync.metrics || []).forEach(function (m) {
            metrics.appendChild(metricTile(m));
        });
        children.push(metrics);

        var lagBlock = lagBreakdownBlock(sync);
        if (lagBlock) {
            children.push(lagBlock);
        }

        if (sync.metadataMetrics && sync.metadataMetrics.length > 0) {
            var metaMetrics = el('div', 'lm-metrics lm-metrics-secondary');
            sync.metadataMetrics.forEach(function (m) {
                metaMetrics.appendChild(metricTile(m));
            });
            children.push(metaMetrics);
        }

        var phaseTimesBlock = phaseStartTimesBlock(sync, options);
        if (phaseTimesBlock) {
            children.push(phaseTimesBlock);
        }

        return cardWithTitleElement(migrationProgressTitle(sync), null, children);
    }

    function renderIndexBuilding(idx) {
        if (!idx) return null;
        if (idx.mode === 'info') {
            return card(idx.title, idx.description, []);
        }
        var headerRow = el('div', 'lm-phase-row');
        headerRow.appendChild(indexSummaryLine(idx));
        if (idx.percent != null) {
            headerRow.appendChild(el('span', 'lm-muted', Math.round(idx.percent) + '%'));
        }
        var children = [headerRow, progressBar(idx.percent, false)];
        var metrics = el('div', 'lm-metrics');
        (idx.metrics || []).forEach(function (m) {
            metrics.appendChild(metricTile(m));
        });
        children.push(metrics);
        return card(idx.title, idx.description, children);
    }

    function renderDirection(dir) {
        if (!dir) return null;
        var wrap = el('div', 'lm-direction');
        ['source', 'destination'].forEach(function (side, i) {
            var node = dir[side];
            if (!node) return;
            var box = el('div', 'lm-node');
            box.appendChild(el('div', 'lm-role', side === 'source' ? 'Source' : 'Destination'));
            box.appendChild(el('div', 'lm-addr', node.address));
            if (node.ping) {
                var pingLine = el('div', 'lm-muted lm-ping-line', 'ping ' + node.ping);
                box.appendChild(pingLine);
            }
            wrap.appendChild(box);
            if (i === 0) wrap.appendChild(el('div', 'lm-arrow', '→'));
        });
        return card('Direction Mapping', null, [wrap]);
    }

    function renderVerification(ver) {
        if (!ver) return null;
        if (ver.mode === 'info') {
            return card(ver.title, ver.description, []);
        }
        var row = el('div', 'lm-row');
        [
            { label: 'Source', rows: ver.source },
            { label: 'Destination', rows: ver.destination },
        ].forEach(function (col) {
            var colEl = el('div', 'lm-ver-col');
            colEl.appendChild(el('div', 'lm-section-label', col.label));
            (col.rows || []).forEach(function (r) {
                colEl.appendChild(kvRow(r.label, r.value, r.title));
            });
            row.appendChild(colEl);
        });
        return card(ver.title, ver.description, [row]);
    }

    function renderWarningsCard(warnings) {
        if (!warnings || warnings.length === 0) return null;
        var tight = el('div', 'lm-stack-tight');
        warnings.forEach(function (w) {
            tight.appendChild(banner('warning', w));
        });
        return card('Warnings', null, [tight]);
    }

    function appendDataSourceBadges(title, dataSources) {
        if (!dataSources || !dataSources.badges) return;
        dataSources.badges.forEach(function (b) {
            title.appendChild(badge(b.label, b.color, true));
        });
    }

    function appendFullViewLink(toolbar, fullViewLink) {
        if (!fullViewLink || !fullViewLink.href) return;
        var actions = el('div', 'lm-toolbar-actions');
        var link = el('a', 'lm-full-view-link');
        link.href = fullViewLink.href;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        if (fullViewLink.title) link.title = fullViewLink.title;
        link.appendChild(document.createTextNode(fullViewLink.label || 'Open full dashboard'));
        actions.appendChild(link);
        toolbar.appendChild(actions);
    }

    function renderToolbar(display, dataSources, options) {
        options = options || {};
        var toolbar = el('div', 'lm-toolbar');
        var textBlock = el('div', 'lm-toolbar-text');

        var title = el('h2', 'lm-page-title');
        title.appendChild(document.createTextNode(options.pageTitle || 'Migration Monitoring'));
        if (display && display.stateBadge) {
            title.appendChild(
                badge(display.stateBadge.label, display.stateBadge.color, true)
            );
        }
        if (display) {
            (display.toolbarBadges || []).forEach(function (b) {
                title.appendChild(badge(b.label, b.color, true));
            });
        }
        appendDataSourceBadges(title, dataSources);
        textBlock.appendChild(title);
        if (options.pageSubtitle) {
            textBlock.appendChild(el('p', 'lm-page-subtitle', options.pageSubtitle));
        }

        toolbar.appendChild(textBlock);
        appendFullViewLink(toolbar, options.fullViewLink);
        return toolbar;
    }

    function renderProgressMonitor(root, payload, options) {
        if (!root) return;
        options = options || {};
        var syncOnly = !!options.syncOnly;
        root.replaceChildren();
        payload = payload || {};
        var toolbarOpts = {
            pageTitle: payload.pageTitle,
            pageSubtitle: payload.pageSubtitle,
        };
        if (options.fullViewLink) {
            toolbarOpts.fullViewLink = options.fullViewLink;
        }

        if (payload.error && !payload.display) {
            var errShell = el('div', 'lm-stack');
            errShell.appendChild(renderToolbar(null, payload.dataSources, toolbarOpts));
            errShell.appendChild(banner('danger', payload.error));
            root.appendChild(errShell);
            return;
        }

        if (!payload.display) {
            var infoShell = el('div', 'lm-stack');
            infoShell.appendChild(renderToolbar(null, payload.dataSources, toolbarOpts));
            infoShell.appendChild(
                banner(
                    'info',
                    payload.error ||
                        'No progress data available. Configure a Mongosync Progress Endpoint URL from Migration monitoring home.'
                )
            );
            root.appendChild(infoShell);
            return;
        }

        var display = payload.display;

        root.appendChild(renderToolbar(display, payload.dataSources, toolbarOpts));

        var stack = el('div', 'lm-stack lm-stack-after-toolbar');
        if (payload.progressWarning) {
            stack.appendChild(banner('warning', payload.progressWarning));
        }
        if (payload.metadataWarning) {
            stack.appendChild(banner('warning', payload.metadataWarning));
        }
        var syncRenderOptions = {
            isSummaryView: !!options.isSummaryView || payload.pageTitle === 'Migration Summary',
        };
        var syncCard = renderSync(display.sync, syncRenderOptions);
        if (syncCard) stack.appendChild(syncCard);

        if (syncOnly) {
            var warnCardOnly = renderWarningsCard(payload.warnings);
            if (warnCardOnly) stack.appendChild(warnCardOnly);
            root.appendChild(stack);
            return;
        }

        var idxCard = renderIndexBuilding(display.indexBuilding);
        if (idxCard) stack.appendChild(idxCard);

        var dirCard = renderDirection(display.direction);
        if (dirCard) stack.appendChild(dirCard);

        var verCard = renderVerification(display.verification);
        if (verCard) stack.appendChild(verCard);

        var naturalOrderCard = renderNaturalOrder(display.naturalOrder);
        if (naturalOrderCard) stack.appendChild(naturalOrderCard);

        var filteredMigrationCard = renderFilteredMigration(display.filteredMigration);
        if (filteredMigrationCard) stack.appendChild(filteredMigrationCard);

        var warnCard = renderWarningsCard(payload.warnings);
        if (warnCard) stack.appendChild(warnCard);

        root.appendChild(stack);
    }

    global.miRenderProgressMonitor = renderProgressMonitor;
})(typeof window !== 'undefined' ? window : this);
