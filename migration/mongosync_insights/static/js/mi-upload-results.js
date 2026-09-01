(function (global) {
'use strict';

var cfg = global.__MI_UPLOAD_PAGE__ || {};

var optionsData = cfg.optionsData || [];
var hiddenOptionsData = cfg.hiddenOptionsData || [];
var startOptionsData = cfg.startOptionsData || [];
var naturalOrderData = cfg.naturalOrderData || [];
var errorsData = cfg.errorsData || [];
var partitionInitData = cfg.partitionInitData || [];
var progressData = cfg.progressData || [];
var busiestCollectionsData = cfg.busiestCollectionsData || [];
var busiestCollectionsEventTypes = cfg.busiestCollectionsEventTypes || [];
var summaryPayload = cfg.summaryPayload || {};
var LV_STORE_ID = cfg.logStoreId || '';
var LV_INITIAL_LINES = cfg.logViewerLines || [];
var LV_SERVER_MAX_LINES = cfg.logViewerMaxLines || 2000;
var LV_MAX_LINES = LV_SERVER_MAX_LINES;
var LOGS_SEARCH_URL = cfg.searchLogsUrl || '';
var busiestCollectionsPlot = cfg.busiestCollectionsPlot || null;
var busiestCollectionsPlotRendered = false;

// ===== Chart Info Badge Overlay =====

function getAxisTraceColor(trace) {
    if (trace && trace.line && trace.line.color) return trace.line.color;
    if (trace && trace.marker && trace.marker.color && typeof trace.marker.color === 'string') return trace.marker.color;
    if (trace && trace.fillcolor) return trace.fillcolor;
    return '#6b7280';
}

function closeInfoTooltips(plotEl) {
    if (!plotEl) return;
    var overlay = plotEl.querySelector('.chart-info-overlay');
    if (!overlay) return;
    overlay.querySelectorAll('.chart-info-tooltip').forEach(function(t) { t.remove(); });
}

function buildInfoOverlay(plotElId) {
    var plotEl = document.getElementById(plotElId);
    if (!plotEl || !plotEl._fullLayout || !plotEl._fullData) return;
    var fullLayout = plotEl._fullLayout;
    var fullData = plotEl._fullData;
    var size = fullLayout._size;
    if (!size) return;

    var oldOverlay = plotEl.querySelector('.chart-info-overlay');
    if (oldOverlay) oldOverlay.remove();

    var overlay = document.createElement('div');
    overlay.className = 'chart-info-overlay';

    var pairs = [];
    Object.keys(fullLayout).forEach(function(key) {
        if (!/^xaxis\d*$/.test(key)) return;
        var suffix = key.replace('xaxis', '');
        var xaxis = fullLayout[key];
        var yaxis = fullLayout['yaxis' + suffix];
        if (!xaxis || !yaxis || !Array.isArray(xaxis.domain) || !Array.isArray(yaxis.domain)) return;
        pairs.push({ suffix: suffix, xaxis: xaxis, yaxis: yaxis });
    });

    pairs.sort(function(a, b) {
        var yA = a.yaxis.domain[1];
        var yB = b.yaxis.domain[1];
        if (Math.abs(yB - yA) > 1e-6) return yB - yA;
        return a.xaxis.domain[0] - b.xaxis.domain[0];
    });

    pairs.forEach(function(pair) {
        var suffix = pair.suffix;
        var xRef = 'x' + (suffix || '');
        var yRef = 'y' + (suffix || '');
        var traces = fullData.filter(function(trace) {
            var tx = trace.xaxis || 'x';
            var ty = trace.yaxis || 'y';
            return tx === xRef && ty === yRef;
        });
        if (traces.length === 0) return;

        // Skip subplots with only "NO DATA" placeholders
        var hasRealData = traces.some(function(t) {
            if (t.type === 'table') return false;
            if (t.mode === 'text' && t.text && (t.text === 'NO DATA' || (Array.isArray(t.text) && t.text[0] === 'NO DATA'))) return false;
            return true;
        });
        if (!hasRealData) return;

        var leftPx = size.l + pair.xaxis.domain[1] * size.w - 20;
        var topPx = size.t + (1 - pair.yaxis.domain[1]) * size.h + 4;

        var badge = document.createElement('button');
        badge.type = 'button';
        badge.className = 'chart-info-badge';
        badge.textContent = 'i';
        badge.style.left = Math.max(0, leftPx) + 'px';
        badge.style.top = Math.max(0, topPx) + 'px';

        badge.addEventListener('click', function(e) {
            e.stopPropagation();
            var existing = overlay.querySelector('.chart-info-tooltip[data-suffix="' + (suffix || 'base') + '"]');
            closeInfoTooltips(plotEl);
            if (existing) return;

            var tooltip = document.createElement('div');
            tooltip.className = 'chart-info-tooltip';
            tooltip.dataset.suffix = suffix || 'base';
            var tooltipLeft = Math.max(8, Math.min(leftPx - 170, size.l + size.w - 210));
            var tooltipTop = Math.max(8, topPx + 22);
            tooltip.style.left = tooltipLeft + 'px';
            tooltip.style.top = tooltipTop + 'px';

            var title = document.createElement('div');
            title.className = 'chart-info-tooltip-title';
            title.textContent = 'Series labels';
            tooltip.appendChild(title);

            traces.forEach(function(trace) {
                if (trace.type === 'table') return;
                if (trace.mode === 'text' && trace.text && (trace.text === 'NO DATA' || (Array.isArray(trace.text) && trace.text[0] === 'NO DATA'))) return;
                var item = document.createElement('div');
                item.className = 'chart-info-item';
                var swatch = document.createElement('span');
                swatch.className = 'chart-info-swatch';
                swatch.style.background = getAxisTraceColor(trace);
                var label = document.createElement('span');
                label.textContent = trace.name || 'Series';
                item.appendChild(swatch);
                item.appendChild(label);
                tooltip.appendChild(item);
            });

            overlay.appendChild(tooltip);
        });

        overlay.appendChild(badge);
    });

    plotEl.appendChild(overlay);
    return overlay;
}

function chartCopyIconHtml() {
    return '<svg class="chart-copy-icon" xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>';
}

function chartCopiedIconHtml() {
    return '<svg class="chart-copy-icon" xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polyline points="20 6 9 17 4 12"></polyline></svg>';
}

function showChartCopyFeedback(buttonElement) {
    var originalHtml = buttonElement.innerHTML;
    var originalTitle = buttonElement.title;
    buttonElement.innerHTML = chartCopiedIconHtml();
    buttonElement.classList.add('copied');
    buttonElement.title = 'Copied!';

    setTimeout(function() {
        buttonElement.innerHTML = originalHtml;
        buttonElement.classList.remove('copied');
        buttonElement.title = originalTitle;
    }, 2000);
}

function buildTableCopyOverlay(plotElId) {
    var plotEl = document.getElementById(plotElId);
    if (!plotEl || plotElId !== 'plot' || !plotEl._fullLayout || !plotEl._fullData) return;
    var fullData = plotEl._fullData;
    var size = plotEl._fullLayout._size;
    if (!size) return;

    var overlay = plotEl.querySelector('.chart-info-overlay');
    if (!overlay) {
        overlay = document.createElement('div');
        overlay.className = 'chart-info-overlay';
        plotEl.appendChild(overlay);
    }

    overlay.querySelectorAll('.chart-copy-badge').forEach(function(btn) { btn.remove(); });

    var tableTrace = fullData.find(function(trace) {
        return trace.type === 'table'
            && trace.meta
            && trace.meta.markdownKey === 'progress';
    });
    if (!tableTrace) return;

    var cellValues = tableTrace.cells && tableTrace.cells.values;
    if (!cellValues || !cellValues[0] || cellValues[0].length === 0) return;

    var domain = tableTrace.domain;
    if (!domain || !Array.isArray(domain.x) || !Array.isArray(domain.y)) return;

    var leftPx = size.l + domain.x[1] * size.w - 22;
    var topPx = size.t + (1 - domain.y[1]) * size.h + 4;

    var badge = document.createElement('button');
    badge.type = 'button';
    badge.className = 'chart-copy-badge';
    badge.innerHTML = chartCopyIconHtml();
    badge.title = 'Copy as Markdown';
    badge.setAttribute('aria-label', 'Copy Mongosync Progress as Markdown');
    badge.style.left = Math.max(0, leftPx) + 'px';
    badge.style.top = Math.max(0, topPx) + 'px';

    badge.addEventListener('click', function(e) {
        e.stopPropagation();
        copyProgressAsMarkdown(badge);
    });

    overlay.appendChild(badge);
}

function bindInfoOverlayEvents(plotElId) {
    var plotEl = document.getElementById(plotElId);
    if (!plotEl || plotEl.dataset.infoOverlayBound === 'true') return;
    plotEl.dataset.infoOverlayBound = 'true';
    plotEl.on('plotly_relayout', function() {
        buildInfoOverlay(plotElId);
        buildTableCopyOverlay(plotElId);
    });
    document.addEventListener('click', function(event) {
        var overlay = plotEl.querySelector('.chart-info-overlay');
        if (!overlay) return;
        if (!overlay.contains(event.target)) {
            closeInfoTooltips(plotEl);
        }
    });
}

function refreshPlotAfterTabShow(elId) {
    if (!window._miPlotRegistry || !window._miPlotRegistry[elId] || typeof window.miRelayoutPlot !== 'function') {
        return;
    }
    requestAnimationFrame(function() {
        window.miRelayoutPlot(elId, function() {
            buildInfoOverlay(elId);
            if (elId === 'plot') {
                buildTableCopyOverlay(elId);
            }
        });
    });
}

// Render mongosync logs plot if data exists

// Render Prometheus metrics plot if data exists


// Store table data for Markdown conversion

function renderBusiestCollectionsPlot() {
    if (busiestCollectionsPlotRendered) {
        return;
    }
    if (!busiestCollectionsPlot || !busiestCollectionsPlot.data) {
        return;
    }
    busiestCollectionsPlotRendered = true;
    miRenderPlot('busiest-collections-plot', busiestCollectionsPlot, {
        onRendered: function(elId) {
            requestAnimationFrame(function() {
                if (typeof Plotly !== 'undefined') {
                    Plotly.Plots.resize(elId);
                }
                buildInfoOverlay(elId);
                bindInfoOverlayEvents(elId);
            });
        }
    });
}

function switchTab(tabName) {
    document.querySelectorAll('.tab-btn').forEach(function(btn) {
        var isActive = btn.id === 'tab-' + tabName;
        btn.classList.toggle('active', isActive);
        btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
        btn.setAttribute('tabindex', isActive ? '0' : '-1');
    });

    document.querySelectorAll('.tab-content').forEach(function(content) {
        content.classList.remove('active');
        content.setAttribute('aria-hidden', 'true');
    });
    var activePanel = document.getElementById(tabName + '-tab');
    if (activePanel) {
        activePanel.classList.add('active');
        activePanel.setAttribute('aria-hidden', 'false');
    }

    if (tabName === 'busiest') {
        renderBusiestCollectionsPlot();
    } else if (tabName === 'charts') {
        refreshPlotAfterTabShow('plot');
    } else if (tabName === 'metrics') {
        refreshPlotAfterTabShow('metrics-plot');
    }

    if (tabName === 'logviewer' && !lvInitialized && LV_INITIAL_LINES && LV_INITIAL_LINES.length > 0) {
        lvInitialized = true;
        lvDisplayStaticLogs(LV_INITIAL_LINES);
    }
}

function miInitUploadResultsTabs() {
    var tablist = document.querySelector('.tab-buttons[role="tablist"]');
    if (!tablist) return;

    document.querySelectorAll('.tab-btn').forEach(function(btn) {
        var isActive = btn.classList.contains('active');
        btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
        btn.setAttribute('tabindex', isActive ? '0' : '-1');
    });

    document.querySelectorAll('.tab-content').forEach(function(panel) {
        panel.setAttribute('aria-hidden', panel.classList.contains('active') ? 'false' : 'true');
    });

    tablist.addEventListener('keydown', function(event) {
        var tabs = Array.prototype.slice.call(tablist.querySelectorAll('[role="tab"]'));
        if (tabs.length === 0) return;
        var currentIndex = tabs.indexOf(document.activeElement);
        if (currentIndex === -1) return;

        var nextIndex = currentIndex;
        if (event.key === 'ArrowRight') {
            nextIndex = (currentIndex + 1) % tabs.length;
        } else if (event.key === 'ArrowLeft') {
            nextIndex = (currentIndex - 1 + tabs.length) % tabs.length;
        } else if (event.key === 'Home') {
            nextIndex = 0;
        } else if (event.key === 'End') {
            nextIndex = tabs.length - 1;
        } else {
            return;
        }

        event.preventDefault();
        tabs[nextIndex].focus();
        var tabName = tabs[nextIndex].id.replace(/^tab-/, '');
        switchTab(tabName);
    });
}

function copyTableAsMarkdown(tableType, buttonElement) {
    var data, title;
    
    if (tableType === 'options') {
        data = optionsData;
        title = 'Mongosync Options';
    } else if (tableType === 'start') {
        data = startOptionsData;
        title = 'Mongosync Start Options';
    } else {
        data = hiddenOptionsData;
        title = 'Mongosync Hidden Options';
    }
    
    if (!data || data.length === 0) {
        return;
    }
    
    // Build Markdown table
    var markdown = '## ' + title + '\n\n';
    markdown += '| Key | Value |\n';
    markdown += '|-----|-------|\n';
    
    data.forEach(function(item) {
        // Escape pipe characters and replace newlines with spaces for Markdown compatibility
        var value = String(item.value)
            .replace(/\|/g, '\\|')
            .replace(/\n/g, ' ');
        
        markdown += '| ' + item.key + ' | ' + value + ' |\n';
    });
    
    // Copy to clipboard
    navigator.clipboard.writeText(markdown).then(function() {
        // Visual feedback
        var originalText = buttonElement.textContent;
        buttonElement.textContent = 'Copied!';
        buttonElement.classList.add('copied');
        
        setTimeout(function() {
            buttonElement.textContent = originalText;
            buttonElement.classList.remove('copied');
        }, 2000);
    }).catch(function(err) {
        console.error('Failed to copy: ', err);
        // Fallback for older browsers
        fallbackCopyToClipboard(markdown, buttonElement);
    });
}

function fallbackCopyToClipboard(text, buttonElement) {
    var textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-999999px';
    textArea.style.top = '-999999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    
    try {
        document.execCommand('copy');
        if (buttonElement.classList.contains('chart-copy-badge')) {
            showChartCopyFeedback(buttonElement);
            return;
        }

        var originalText = buttonElement.textContent;
        buttonElement.textContent = 'Copied!';
        buttonElement.classList.add('copied');
        
        setTimeout(function() {
            buttonElement.textContent = originalText;
            buttonElement.classList.remove('copied');
        }, 2000);
    } catch (err) {
        console.error('Fallback copy failed: ', err);
        if (buttonElement.classList.contains('chart-copy-badge')) {
            buttonElement.title = 'Copy failed';
            setTimeout(function() {
                buttonElement.title = 'Copy as Markdown';
            }, 2000);
        } else {
            buttonElement.textContent = 'Copy failed';
            setTimeout(function() {
                buttonElement.textContent = 'Copy as Markdown';
            }, 2000);
        }
    }
    
    document.body.removeChild(textArea);
}

function filterTables(searchText) {
    var filter = searchText.toLowerCase().trim();
    
    // Get all table bodies
    var tables = document.querySelectorAll('.options-table tbody');
    
    tables.forEach(function(tbody) {
        var rows = tbody.querySelectorAll('tr');
        var visibleCount = 0;
        
        rows.forEach(function(row) {
            var keyCell = row.querySelector('.key-cell');
            var valueCell = row.querySelector('.value-cell');
            
            if (keyCell && valueCell) {
                var keyText = keyCell.textContent.toLowerCase();
                var valueText = valueCell.textContent.toLowerCase();
                
                // Show row if filter is empty OR key/value contains the search text
                if (filter === '' || keyText.includes(filter) || valueText.includes(filter)) {
                    row.style.display = '';
                    visibleCount++;
                } else {
                    row.style.display = 'none';
                }
            }
        });
        
        // Show/hide "no results" message for each table
        var tableWrapper = tbody.closest('.options-table-wrapper');
        var noResultsMsg = tableWrapper.querySelector('.no-results');
        
        if (noResultsMsg) {
            if (visibleCount === 0 && filter !== '') {
                noResultsMsg.style.display = 'block';
            } else {
                noResultsMsg.style.display = 'none';
            }
        }
    });
}

function toggleErrorDetail(id, toggleBtn) {
    var detailRow = document.getElementById(id);
    if (!detailRow) return;

    var isVisible = detailRow.style.display !== 'none' && detailRow.style.display !== '';
    detailRow.style.display = isVisible ? 'none' : 'table-row';

    if (toggleBtn) {
        toggleBtn.setAttribute('aria-expanded', isVisible ? 'false' : 'true');
        var icon = toggleBtn.querySelector('.error-expand-icon');
        if (icon) {
            icon.textContent = isVisible ? '▸' : '▾';
        }
    }
}

function filterErrorsTable(searchText) {
    var filter = searchText.toLowerCase().trim();
    var table = document.getElementById('errors-table');
    if (!table) return;
    
    var rows = table.querySelectorAll('tbody tr.error-row');
    var visibleCount = 0;
    
    rows.forEach(function(row) {
        var keyCell = row.querySelector('.key-cell');
        var valueCell = row.querySelector('.value-cell');
        var recCell = row.querySelector('.recommendation-cell');
        
        var friendlyName = keyCell ? keyCell.textContent.toLowerCase() : '';
        var message = valueCell ? valueCell.textContent.toLowerCase() : '';
        var recommendation = recCell ? recCell.textContent.toLowerCase() : '';
        
        var detailRow = row.nextElementSibling;
        
        if (filter === '' || friendlyName.includes(filter) || message.includes(filter) || recommendation.includes(filter)) {
            row.style.display = '';
            visibleCount++;
        } else {
            row.style.display = 'none';
            // Also hide the detail row when filtering out
            if (detailRow && detailRow.classList.contains('error-detail-row')) {
                detailRow.style.display = 'none';
            }
        }
    });
    
    var noResultsMsg = document.getElementById('errors-no-results');
    if (noResultsMsg) {
        noResultsMsg.style.display = (visibleCount === 0 && filter !== '') ? 'block' : 'none';
    }
}

function copyProgressAsMarkdown(buttonElement) {
    if (!progressData || progressData.length === 0) {
        return;
    }

    var markdown = '## Mongosync Progress\n\n';
    markdown += '| Date Time | Event |\n';
    markdown += '|-----------|-------|\n';

    progressData.forEach(function(item) {
        var time = String(item.time || '')
            .replace(/\|/g, '\\|')
            .replace(/\n/g, ' ');
        var event = String(item.event || '')
            .replace(/\|/g, '\\|')
            .replace(/\n/g, ' ');

        markdown += '| ' + time + ' | ' + event + ' |\n';
    });

    navigator.clipboard.writeText(markdown).then(function() {
        if (buttonElement.classList.contains('chart-copy-badge')) {
            showChartCopyFeedback(buttonElement);
            return;
        }

        var originalText = buttonElement.textContent;
        buttonElement.textContent = 'Copied!';
        buttonElement.classList.add('copied');

        setTimeout(function() {
            buttonElement.textContent = originalText;
            buttonElement.classList.remove('copied');
        }, 2000);
    }).catch(function(err) {
        console.error('Failed to copy: ', err);
        fallbackCopyToClipboard(markdown, buttonElement);
    });
}

function copyErrorsAsMarkdown(buttonElement) {
    if (!errorsData || errorsData.length === 0) {
        return;
    }
    
    // Build Markdown table
    var markdown = '## Common Errors Found in Log\n\n';
    markdown += '| Friendly Name | Time | Level | Recommendation | Message |\n';
    markdown += '|---------------|------|-------|----------------|----------|\n';
    
    errorsData.forEach(function(item) {
        var message = String(item.message)
            .replace(/\|/g, '\\|')
            .replace(/\n/g, ' ');
        var recommendation = String(item.recommendation || '')
            .replace(/\|/g, '\\|')
            .replace(/\n/g, ' ');
        
        markdown += '| ' + item.friendly_name + ' | ' + item.time + ' | ' + item.level + ' | ' + recommendation + ' | ' + message + ' |\n';
    });
    
    // Copy to clipboard
    navigator.clipboard.writeText(markdown).then(function() {
        var originalText = buttonElement.textContent;
        buttonElement.textContent = 'Copied!';
        buttonElement.classList.add('copied');
        
        setTimeout(function() {
            buttonElement.textContent = originalText;
            buttonElement.classList.remove('copied');
        }, 2000);
    }).catch(function(err) {
        console.error('Failed to copy: ', err);
        fallbackCopyToClipboard(markdown, buttonElement);
    });
}

function copyErrorLog(index, format, buttonElement) {
    var logElement = document.getElementById('error-log-' + index);
    if (!logElement) return;

    var logContent = logElement.textContent;
    var textToCopy = (format === 'markdown') ? '```json\n' + logContent + '\n```' : logContent;

    navigator.clipboard.writeText(textToCopy).then(function() {
        var originalText = buttonElement.textContent;
        buttonElement.textContent = 'Copied!';
        buttonElement.classList.add('copied');

        setTimeout(function() {
            buttonElement.textContent = originalText;
            buttonElement.classList.remove('copied');
        }, 2000);
    }).catch(function(err) {
        console.error('Failed to copy: ', err);
        fallbackCopyToClipboard(textToCopy, buttonElement);
    });
}

var _sortState = {};

function _parseSortableHeader(th) {
    var tableId = th.getAttribute('data-table');
    var colIndex = th.getAttribute('data-col');
    var dataType = th.getAttribute('data-type');
    if (tableId && colIndex && dataType) {
        return {
            tableId: tableId,
            colIndex: parseInt(colIndex, 10),
            dataType: dataType,
        };
    }

    var onclick = th.getAttribute('onclick') || '';
    var match = onclick.match(/sortTableByColumn\('([^']+)',\s*(\d+),\s*'([^']+)'\)/);
    if (!match) return null;
    return {
        tableId: match[1],
        colIndex: parseInt(match[2], 10),
        dataType: match[3],
    };
}

function _getSortButton(th) {
    return th.querySelector('.sortable-th-btn');
}

function _setSortButtonLabel(th, labelText) {
    var btn = _getSortButton(th);
    if (btn) {
        btn.textContent = labelText;
    } else {
        th.textContent = labelText;
    }
}

function miInitSortableTableHeaders() {
    document.querySelectorAll('th.sortable-th').forEach(function(th) {
        if (th.querySelector('.sortable-th-btn')) return;

        var config = _parseSortableHeader(th);
        if (!config) return;

        var label = th.getAttribute('data-label') || th.textContent.trim();
        th.removeAttribute('onclick');
        th.removeAttribute('title');

        var btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'sortable-th-btn';
        btn.textContent = label;
        btn.setAttribute('aria-label', 'Sort by ' + label);

        btn.addEventListener('click', function() {
            sortTableByColumn(config.tableId, config.colIndex, config.dataType);
        });
        btn.addEventListener('keydown', function(event) {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                sortTableByColumn(config.tableId, config.colIndex, config.dataType);
            }
        });

        th.textContent = '';
        th.appendChild(btn);
        th.setAttribute('data-table', config.tableId);
        th.setAttribute('data-col', String(config.colIndex));
        th.setAttribute('data-type', config.dataType);
        th.setAttribute('data-label', label);
    });
}

function sortTableByColumn(tableId, colIndex, dataType) {
    var table = document.getElementById(tableId);
    if (!table) return;

    var tbody = table.querySelector('tbody');
    var isErrorsTable = (tableId === 'errors-table');
    var rows = Array.from(tbody.querySelectorAll(isErrorsTable ? 'tr.error-row' : 'tr'));

    var key = tableId + '-' + colIndex;
    _sortState[key] = (_sortState[key] === 'asc') ? 'desc' : 'asc';
    var dir = _sortState[key];
    var mult = (dir === 'asc') ? 1 : -1;

    var headers = table.querySelectorAll('th.sortable-th');
    headers.forEach(function(th) {
        var label = th.getAttribute('data-label');
        if (label) _setSortButtonLabel(th, label);
    });
    var activeHeader = headers[colIndex];
    if (activeHeader) {
        var indicator = (dir === 'asc') ? ' \u25B2' : ' \u25BC';
        _setSortButtonLabel(activeHeader, activeHeader.getAttribute('data-label') + indicator);
    }

    rows.sort(function(a, b) {
        var aVal = a.cells[colIndex].textContent.trim();
        var bVal = b.cells[colIndex].textContent.trim();
        var result = 0;

        if (dataType === 'numeric') {
            var aNum = parseFloat(aVal.replace(/,/g, ''));
            var bNum = parseFloat(bVal.replace(/,/g, ''));
            if (isNaN(aNum)) aNum = -Infinity;
            if (isNaN(bNum)) bNum = -Infinity;
            result = aNum - bNum;
        } else if (dataType === 'date') {
            var aDate = new Date(aVal);
            var bDate = new Date(bVal);
            var aTime = isNaN(aDate.getTime()) ? 0 : aDate.getTime();
            var bTime = isNaN(bDate.getTime()) ? 0 : bDate.getTime();
            result = aTime - bTime;
        } else {
            result = aVal.localeCompare(bVal);
        }

        return result * mult;
    });

    rows.forEach(function(row) {
        tbody.appendChild(row);
        if (isErrorsTable) {
            var detailRow = row.nextElementSibling;
            if (detailRow && detailRow.classList.contains('error-detail-row')) {
                tbody.appendChild(detailRow);
            }
        }
    });
}

function filterPartitionInitTable(searchText) {
    var filter = searchText.toLowerCase().trim();
    var table = document.getElementById('partitioninit-table');
    if (!table) return;
    
    var rows = table.querySelectorAll('tbody tr');
    var visibleCount = 0;
    
    rows.forEach(function(row) {
        var cells = row.querySelectorAll('td');
        var match = false;
        
        cells.forEach(function(cell) {
            if (cell.textContent.toLowerCase().includes(filter)) {
                match = true;
            }
        });
        
        if (filter === '' || match) {
            row.style.display = '';
            visibleCount++;
        } else {
            row.style.display = 'none';
        }
    });
    
    var noResultsMsg = document.getElementById('partitioninit-no-results');
    if (noResultsMsg) {
        noResultsMsg.style.display = (visibleCount === 0 && filter !== '') ? 'block' : 'none';
    }
}

function copyPartitionInitAsMarkdown(buttonElement) {
    if (!partitionInitData || partitionInitData.length === 0) {
        return;
    }
    
    var markdown = '## Partition Initialization Details\n\n';
    markdown += '| Collection | Type | Reason | Partitions | Doc Count | Expected Size | Sampler | IDs Sampled | Init Started | Duration (s) |\n';
    markdown += '|------------|------|--------|------------|-----------|---------------|---------|-------------|--------------|-------------|\n';
    
    partitionInitData.forEach(function(item) {
        var reason = String(item.reason || '').replace(/\|/g, '\\|').replace(/\n/g, ' ');
        var docCount = item.doc_count ? String(item.doc_count) : 'N/A';
        var idsSampled = item.ids_sampled ? String(item.ids_sampled) : 'N/A';
        var duration = item.duration_sec !== null ? String(item.duration_sec) : 'N/A';
        
        markdown += '| ' + item.collection + ' | ' + item.type + ' | ' + reason + ' | ' + item.partition_count + ' | ' + docCount + ' | ' + item.expected_partition_size + ' | ' + item.sampler + ' | ' + idsSampled + ' | ' + item.init_started + ' | ' + duration + ' |\n';
    });
    
    navigator.clipboard.writeText(markdown).then(function() {
        var originalText = buttonElement.textContent;
        buttonElement.textContent = 'Copied!';
        buttonElement.classList.add('copied');
        
        setTimeout(function() {
            buttonElement.textContent = originalText;
            buttonElement.classList.remove('copied');
        }, 2000);
    }).catch(function(err) {
        console.error('Failed to copy: ', err);
        fallbackCopyToClipboard(markdown, buttonElement);
    });
}

function filterBusiestCollectionsTable(searchText) {
    var filter = searchText.toLowerCase().trim();
    var table = document.getElementById('busiest-collections-table');
    if (!table) return;

    var rows = table.querySelectorAll('tbody tr');
    var visibleCount = 0;

    rows.forEach(function(row) {
        var match = false;
        row.querySelectorAll('td').forEach(function(cell) {
            if (cell.textContent.toLowerCase().includes(filter)) {
                match = true;
            }
        });
        if (filter === '' || match) {
            row.style.display = '';
            visibleCount++;
        } else {
            row.style.display = 'none';
        }
    });

    var noResultsMsg = document.getElementById('busiest-no-results');
    if (noResultsMsg) {
        noResultsMsg.style.display = (visibleCount === 0 && filter !== '') ? 'block' : 'none';
    }
}

function copyBusiestCollectionsAsMarkdown(buttonElement) {
    if (!busiestCollectionsData || busiestCollectionsData.length === 0) {
        return;
    }

    var headers = ['Namespace', 'Total Write Ops'].concat(busiestCollectionsEventTypes || [])
        .concat(['Warnings', 'Max Spread Disparity']);
    var markdown = '## CEA Busiest Collections\n\n';
    markdown += '| ' + headers.join(' | ') + ' |\n';
    markdown += '|' + headers.map(function() { return '---'; }).join('|') + '|\n';

    busiestCollectionsData.forEach(function(row) {
        var cells = [
            row.namespace,
            String(row.totalEvents),
        ];
        (busiestCollectionsEventTypes || []).forEach(function(eventType) {
            var counts = row.totalEventsPerType || {};
            cells.push(String(counts[eventType] || 0));
        });
        cells.push(String(row.warningCount || 0));
        cells.push(
            row.maxSpreadDisparity !== null && row.maxSpreadDisparity !== undefined
                ? String(row.maxSpreadDisparity)
                : '—'
        );
        markdown += '| ' + cells.map(function(cell) {
            return String(cell).replace(/\|/g, '\\|');
        }).join(' | ') + ' |\n';
    });

    navigator.clipboard.writeText(markdown).then(function() {
        var originalText = buttonElement.textContent;
        buttonElement.textContent = 'Copied!';
        buttonElement.classList.add('copied');
        setTimeout(function() {
            buttonElement.textContent = originalText;
            buttonElement.classList.remove('copied');
        }, 2000);
    }).catch(function(err) {
        console.error('Failed to copy: ', err);
        fallbackCopyToClipboard(markdown, buttonElement);
    });
}

function lvFocusNamespace(namespace) {
    if (!namespace) return;
    switchTab('logviewer');
    var searchInput = document.getElementById('lvSearchInput');
    if (searchInput) {
        searchInput.value = namespace;
        lvOnSearchInput(namespace);
    }
}

function copyNaturalOrderAsMarkdown(buttonElement) {
    if (!naturalOrderData || naturalOrderData.length === 0) {
        return;
    }
    
    var markdown = '## Collections Copied in Natural Order\n\n';
    markdown += '| Database | Collection |\n';
    markdown += '|----------|------------|\n';
    
    naturalOrderData.forEach(function(item) {
        var collection = String(item.collection)
            .replace(/\|/g, '\\|')
            .replace(/\n/g, ' ');
        
        markdown += '| ' + item.database + ' | ' + collection + ' |\n';
    });
    
    navigator.clipboard.writeText(markdown).then(function() {
        var originalText = buttonElement.textContent;
        buttonElement.textContent = 'Copied!';
        buttonElement.classList.add('copied');
        
        setTimeout(function() {
            buttonElement.textContent = originalText;
            buttonElement.classList.remove('copied');
        }, 2000);
    }).catch(function(err) {
        console.error('Failed to copy: ', err);
        fallbackCopyToClipboard(markdown, buttonElement);
    });
}

// ===== Log Viewer =====


var lvLogLines = [];
var lvInitialized = false;
var lvViewerMode = sessionStorage.getItem('mi_lv_view_mode') || 'highlighted';
var lvSearchText = '';
var lvStartTime = '';
var lvEndTime = '';
var lvSearchDebounceTimer = null;
var lvIsServerSearch = false;
var lvCurrentPage = 1;
var lvPerPage = 50;
var lvTotalResults = 0;

var lvSeverityFilters = {
    info: true, debug: true, trace: true, warn: true,
    error: true, fatal: true, panic: true
};

var lvSemanticFilters = {
    errors: false, warnings: false, phases: false, opStats: false
};

// --- Severity & classification ---

function lvGetSeverity(line) {
    if (/"level"\s*:\s*"panic"/i.test(line) || /\bPANIC\b/.test(line)) return 'panic';
    if (/"level"\s*:\s*"fatal"/i.test(line) || /\bFATAL\b/.test(line)) return 'fatal';
    if (/"level"\s*:\s*"error"/i.test(line) || /\bERROR\b/.test(line)) return 'error';
    if (/"level"\s*:\s*"warn"/i.test(line) || /\bWARN(ING)?\b/.test(line)) return 'warn';
    if (/"level"\s*:\s*"debug"/i.test(line) || /\bDEBUG\b/.test(line)) return 'debug';
    if (/"level"\s*:\s*"trace"/i.test(line) || /\bTRACE\b/.test(line)) return 'trace';
    return 'info';
}

function lvGetLineClass(line) {
    if (/"level"\s*:\s*"error"/i.test(line) || /\bERROR\b/.test(line)) return 'error-line';
    if (/"level"\s*:\s*"warn"/i.test(line) || /\bWARN(ING)?\b/.test(line)) return 'warn-line';
    if (/phase.*transition|transitioning.*phase|entered.*phase/i.test(line)) return 'phase-line';
    return '';
}

function lvParseJson(line) {
    try { return JSON.parse(line); } catch(e) { return null; }
}

function lvEscapeHtml(text) {
    var div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// --- Semantic filters ---

function lvHasActiveSemanticFilters() {
    return Object.values(lvSemanticFilters).some(Boolean);
}

function lvMatchesSemanticFilters(line, parsed) {
    if (!lvHasActiveSemanticFilters()) return true;
    var msg = parsed ? (parsed.message || '') : line;
    var lvl = parsed ? (parsed.level || '') : '';
    if (lvSemanticFilters.errors && /error|fatal|panic/i.test(lvl)) return true;
    if (lvSemanticFilters.warnings && /warn/i.test(lvl)) return true;
    if (lvSemanticFilters.phases && /phase.*transition|Starting.*phase|Commit handler|Updating the in-memory phase/i.test(msg)) return true;
    if (lvSemanticFilters.opStats && /Operation duration stats/i.test(msg)) return true;
    return false;
}

// --- Formatting ---

function lvHighlightLine(line) {
    var html = lvEscapeHtml(line);
    var lineClass = lvGetLineClass(line);

    html = html.replace(/\{/g, '<span class="json-bracket">{</span>');
    html = html.replace(/\}/g, '<span class="json-bracket">}</span>');
    html = html.replace(/,/g, '<span class="json-comma">,</span>');
    html = html.replace(/(["\w])\s*:\s*(?!\/\/)/g, '$1<span class="json-colon">:</span>');

    html = html.replace(/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)/g,
        '<span class="timestamp">$1</span>');
    html = html.replace(/\b(INITIAL_SYNC|COLLECTION_COPY|CHANGE_EVENT_APPLICATION|COMMITTED|CEA|IDLE)\b/g,
        '<span class="phase">$1</span>');
    html = html.replace(/"([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)"/g,
        '"<span class="db-name">$1</span>"');

    html = html.replace(/"level"\s*:\s*"(error)"/gi, '"level":"<span class="level-error">$1</span>"');
    html = html.replace(/"level"\s*:\s*"(warn)"/gi, '"level":"<span class="level-warn">$1</span>"');
    html = html.replace(/"level"\s*:\s*"(info)"/gi, '"level":"<span class="level-info">$1</span>"');
    html = html.replace(/"level"\s*:\s*"(debug)"/gi, '"level":"<span class="level-debug">$1</span>"');
    html = html.replace(/"level"\s*:\s*"(trace)"/gi, '"level":"<span class="level-trace">$1</span>"');

    html = html.replace(/"(averageDurationMs)"\s*:\s*(\d+(?:\.\d+)?)/g,
        '"<span class="json-key">$1</span>":<span class="duration-avg">$2</span>');
    html = html.replace(/"(maximumDurationMs)"\s*:\s*(\d+(?:\.\d+)?)/g,
        '"<span class="json-key">$1</span>":<span class="duration-max">$2</span>');
    html = html.replace(/"(numOperations)"\s*:\s*(\d+)/g,
        '"<span class="json-key">$1</span>":<span class="operation-count">$2</span>');

    html = html.replace(/"([Ss]tate)"\s*:\s*"([^"]+)"/g,
        '"<span class="field-state">$1</span>":"<span class="field-state">$2</span>"');
    html = html.replace(/"([Cc]an[Cc]ommit)"\s*:\s*(true|false)/gi,
        '"<span class="field-cancommit">$1</span>":<span class="field-cancommit">$2</span>');
    html = html.replace(/"([Ll]ag[Tt]ime[Ss]econds?)"\s*:\s*(\d+(?:\.\d+)?)/g,
        '"<span class="field-lagtime">$1</span>":<span class="field-lagtime">$2</span>');
    html = html.replace(/"([Oo]verall[Ll]ag[Ss]econds?)"\s*:\s*(\d+(?:\.\d+)?)/g,
        '"<span class="field-lagtime">$1</span>":<span class="field-lagtime">$2</span>');
    html = html.replace(/"([Cc]rud[Ll]ag[Ss]econds?)"\s*:\s*(\d+(?:\.\d+)?)/g,
        '"<span class="field-lagtime">$1</span>":<span class="field-lagtime">$2</span>');
    html = html.replace(/"([Dd]dl[Ll]ag[Ss]econds?)"\s*:\s*(\d+(?:\.\d+)?)/g,
        '"<span class="field-lagtime">$1</span>":<span class="field-lagtime">$2</span>');
    html = html.replace(/"([Ee]stimated[Tt]otal[Bb]ytes)"\s*:\s*(\d+)/g,
        '"<span class="field-estimated-total">$1</span>":<span class="field-estimated-total">$2</span>');
    html = html.replace(/"([Ee]stimated[Cc]opied[Bb]ytes)"\s*:\s*(\d+)/g,
        '"<span class="field-estimated-copied">$1</span>":<span class="field-estimated-copied">$2</span>');

    html = html.replace(/"(totalEventsApplied)"\s*:\s*(\d+(?:\.\d+)?)/g,
        '"<span class="json-key">$1</span>":<span class="metric-value">$2</span>');
    html = html.replace(/:\s*(true)(?=\s*[,}])/g, ':<span class="boolean-true">$1</span>');
    html = html.replace(/:\s*(false)(?=\s*[,}])/g, ':<span class="boolean-false">$1</span>');
    html = html.replace(/:\s*(\d+(?:\.\d+)?)(?=\s*[,}])/g, ':<span class="json-number">$1</span>');

    return { html: html, lineClass: lineClass };
}

function lvFormatPrettyJson(line) {
    var parsed = lvParseJson(line);
    if (!parsed) {
        return { html: '<span class="invalid-json-badge">Invalid JSON</span> ' + lvEscapeHtml(line), lineClass: lvGetLineClass(line), formatClass: 'pretty' };
    }
    var pretty = JSON.stringify(parsed, null, 2);
    var html = lvEscapeHtml(pretty);
    html = html.replace(/"([^"]+)"(?=\s*:)/g, '"<span class="json-key">$1</span>"');
    html = html.replace(/:\s*"([^"]+)"/g, ': "<span class="json-string">$1</span>"');
    html = html.replace(/:\s*(\d+(?:\.\d+)?)(?=[,\n\r\s\}])/g, ': <span class="json-number">$1</span>');
    html = html.replace(/:\s*(true)(?=[,\n\r\s\}])/g, ': <span class="boolean-true">$1</span>');
    html = html.replace(/:\s*(false)(?=[,\n\r\s\}])/g, ': <span class="boolean-false">$1</span>');
    return { html: html, lineClass: lvGetLineClass(line), formatClass: 'pretty' };
}

function lvBuildSummary(parsed) {
    if (!parsed) return null;
    var parts = [];
    if (parsed.time) {
        var t = parsed.time;
        if (t.length > 19) t = t.substring(11, 23);
        parts.push(t);
    }
    var lvl = (parsed.level || 'info').toUpperCase();
    parts.push(lvl);
    if (parsed.message) parts.push(parsed.message);
    var progress = parsed.progress;
    if (progress) {
        if (progress.state) parts.push('state=' + progress.state);
        if (progress.canCommit !== undefined) parts.push('canCommit=' + progress.canCommit);
        var lagObj = progress.lag;
        if (lagObj && lagObj.overallLagSeconds !== undefined && lagObj.overallLagSeconds !== null) {
            var lagParts = ['lag=' + lagObj.overallLagSeconds + 's'];
            if (lagObj.crudLagSeconds != null) lagParts.push('CRUD ' + lagObj.crudLagSeconds + 's');
            if (lagObj.ddlLagSeconds != null) lagParts.push('DDL ' + lagObj.ddlLagSeconds + 's');
            parts.push(lagParts.join(', '));
        } else if (progress.lagTimeSeconds !== undefined) {
            parts.push('lag=' + progress.lagTimeSeconds + 's');
        }
        if (progress.collectionCopy) {
            var cc = progress.collectionCopy;
            if (cc.estimatedTotalBytes !== undefined && cc.estimatedCopiedBytes !== undefined) {
                var pct = cc.estimatedTotalBytes > 0 ? ((cc.estimatedCopiedBytes / cc.estimatedTotalBytes) * 100).toFixed(1) : '0';
                parts.push('copy=' + pct + '%');
            }
        }
    }
    return parts.join(' | ');
}

function lvFormatSummary(line) {
    var parsed = lvParseJson(line);
    var summary = lvBuildSummary(parsed);
    if (summary) {
        return { html: lvEscapeHtml(summary), lineClass: lvGetLineClass(line), formatClass: 'summary' };
    }
    return { html: lvEscapeHtml(line), lineClass: lvGetLineClass(line), formatClass: 'summary' };
}

function lvFormatLine(line) {
    if (lvViewerMode === 'raw') return { html: lvEscapeHtml(line), lineClass: lvGetLineClass(line), formatClass: 'raw' };
    if (lvViewerMode === 'pretty') return lvFormatPrettyJson(line);
    if (lvViewerMode === 'summary') return lvFormatSummary(line);
    var h = lvHighlightLine(line);
    return { html: h.html, lineClass: h.lineClass, formatClass: 'highlighted' };
}

// --- Rendering ---

function lvFilterLines() {
    return lvLogLines.filter(function(line) {
        var sev = lvGetSeverity(line);
        if (!lvSeverityFilters[sev]) return false;
        if (lvSearchText && !lvIsServerSearch && !line.toLowerCase().includes(lvSearchText.toLowerCase())) return false;
        var parsed = lvParseJson(line);
        return lvMatchesSemanticFilters(line, parsed);
    });
}

function lvRenderSlice(lines) {
    var viewer = document.getElementById('lvViewer');
    var html = lines.map(function(line) {
        var f = lvFormatLine(line);
        var cls = ['lv-log-line', f.lineClass, f.formatClass].filter(Boolean).join(' ');
        return '<div class="' + cls + '">' + f.html + '</div>';
    }).join('');

    var container = viewer.querySelector('.lv-lines-container');
    if (!container) {
        container = document.createElement('div');
        container.className = 'lv-lines-container';
        viewer.appendChild(container);
    }
    container.innerHTML = html;
}

function lvRenderTailPage() {
    var viewer = document.getElementById('lvViewer');
    var placeholder = document.getElementById('lvPlaceholder');
    if (placeholder) placeholder.style.display = 'none';

    var filtered = lvFilterLines();
    var totalFiltered = filtered.length;
    var totalAll = lvLogLines.length;
    var totalPages = Math.max(1, Math.ceil(totalFiltered / lvPerPage));

    if (lvCurrentPage > totalPages) lvCurrentPage = totalPages;
    if (lvCurrentPage < 1) lvCurrentPage = 1;

    var start = (lvCurrentPage - 1) * lvPerPage;
    var pageSlice = filtered.slice(start, start + lvPerPage);

    lvRenderSlice(pageSlice);

    var countEl = document.getElementById('lvLineCount');
    if (totalFiltered === totalAll) {
        countEl.textContent = totalAll + ' lines';
    } else {
        countEl.textContent = totalFiltered + ' / ' + totalAll + ' lines';
    }

    var paginationEl = document.getElementById('lvPagination');
    if (totalPages > 1) {
        paginationEl.style.display = 'flex';
        document.getElementById('lvPageInfo').textContent = 'Page ' + lvCurrentPage + ' of ' + totalPages;
        document.getElementById('lvPageTotal').textContent = totalFiltered + ' lines';
        document.getElementById('lvPrevBtn').disabled = (lvCurrentPage <= 1);
        document.getElementById('lvNextBtn').disabled = (lvCurrentPage >= totalPages);
    } else {
        paginationEl.style.display = 'none';
    }

    document.getElementById('lvBackToTail').style.display = lvIsServerSearch ? 'inline-block' : 'none';

    viewer.scrollTop = 0;
}

function lvRenderLines() {
    if (lvIsServerSearch) {
        var viewer = document.getElementById('lvViewer');
        var placeholder = document.getElementById('lvPlaceholder');
        if (placeholder) placeholder.style.display = 'none';
        var filtered = lvFilterLines();
        lvRenderSlice(filtered);
        var countEl = document.getElementById('lvLineCount');
        countEl.textContent = filtered.length + ' lines';
    } else {
        lvRenderTailPage();
    }
}

function lvDisplayStaticLogs(lines) {
    var placeholder = document.getElementById('lvPlaceholder');
    if (placeholder) placeholder.style.display = 'none';
    lvLogLines = lines.slice(-LV_MAX_LINES);
    lvIsServerSearch = false;

    var filtered = lvFilterLines();
    var totalPages = Math.max(1, Math.ceil(filtered.length / lvPerPage));
    lvCurrentPage = totalPages;

    document.getElementById('lvTitle').textContent = 'Log Viewer (Tail - last ' + lvLogLines.length + ' lines)';
    lvRenderTailPage();
}

// --- Controls ---

function lvResetToLastPage() {
    if (!lvIsServerSearch) {
        var filtered = lvFilterLines();
        lvCurrentPage = Math.max(1, Math.ceil(filtered.length / lvPerPage));
    }
}

function lvToggleSeverity(severity) {
    lvSeverityFilters[severity] = !lvSeverityFilters[severity];
    var btn = document.querySelector('.lv-sev-btn[data-severity="' + severity + '"]');
    if (btn) btn.classList.toggle('active', lvSeverityFilters[severity]);
    lvResetToLastPage();
    lvRenderLines();
}

function lvSetViewMode(mode) {
    var allowed = ['highlighted', 'raw', 'pretty', 'summary'];
    if (allowed.indexOf(mode) === -1) return;
    lvViewerMode = mode;
    sessionStorage.setItem('mi_lv_view_mode', mode);
    lvUpdateControlsState();
    lvRenderLines();
}

function lvToggleFocus(filterName) {
    if (!(filterName in lvSemanticFilters)) return;
    lvSemanticFilters[filterName] = !lvSemanticFilters[filterName];
    lvUpdateControlsState();
    lvResetToLastPage();
    lvRenderLines();
}

function lvUpdateControlsState() {
    document.querySelectorAll('.lv-mode-btn').forEach(function(btn) {
        btn.classList.toggle('active', btn.getAttribute('data-log-mode') === lvViewerMode);
    });
    document.querySelectorAll('.lv-focus-btn').forEach(function(btn) {
        var name = btn.getAttribute('data-log-filter');
        btn.classList.toggle('active', !!lvSemanticFilters[name]);
    });
}

function lvClear() {
    lvLogLines = [];
    lvSearchText = '';
    lvStartTime = '';
    lvEndTime = '';
    lvIsServerSearch = false;
    var searchInput = document.getElementById('lvSearchInput');
    if (searchInput) searchInput.value = '';
    var startInput = document.getElementById('lvStartInput');
    if (startInput) startInput.value = '';
    var endInput = document.getElementById('lvEndInput');
    if (endInput) endInput.value = '';
    var viewer = document.getElementById('lvViewer');
    var container = viewer.querySelector('.lv-lines-container');
    if (container) container.innerHTML = '';
    var placeholder = document.getElementById('lvPlaceholder');
    if (placeholder) { placeholder.style.display = 'flex'; placeholder.querySelector('p').textContent = 'Log viewer cleared.'; }
    document.getElementById('lvLineCount').textContent = '0 lines';
    document.getElementById('lvTitle').textContent = 'Log Viewer';
    document.getElementById('lvPagination').style.display = 'none';
}

function lvDownloadLogs() {
    if (lvLogLines.length === 0) return;
    var text = lvLogLines.join('\n');
    var blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
    var now = new Date();
    var ts = now.toISOString().replace(/[:.]/g, '-').slice(0, -5);
    var fname = 'mongosync_logs_' + ts + '.log';
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = fname;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(a.href);
}

// --- Server search ---

function lvNormalizeDateTimeInput(value) {
    if (!value) return '';
    var normalized = value.trim();
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(normalized)) {
        normalized += ':00';
    }
    return normalized;
}

function lvReadDateTimeFilters() {
    var startInput = document.getElementById('lvStartInput');
    var endInput = document.getElementById('lvEndInput');
    lvStartTime = lvNormalizeDateTimeInput(startInput ? startInput.value : '');
    lvEndTime = lvNormalizeDateTimeInput(endInput ? endInput.value : '');
}

function lvHasServerSearchFilters() {
    return lvSearchText.length >= 3 || !!lvStartTime || !!lvEndTime;
}

function lvFormatDateTimeRangeLabel() {
    var parts = [];
    if (lvStartTime) parts.push(lvStartTime.slice(0, 19).replace('T', ' '));
    if (lvEndTime) parts.push(lvEndTime.slice(0, 19).replace('T', ' '));
    if (parts.length === 2) return parts[0] + ' – ' + parts[1];
    if (parts.length === 1) return (lvStartTime ? 'from ' : 'until ') + parts[0];
    return '';
}

function lvBuildSearchTitle(total) {
    if (lvSearchText) {
        return 'Log Viewer (Search: ' + total + ' results)';
    }
    var rangeLabel = lvFormatDateTimeRangeLabel();
    if (rangeLabel) {
        return 'Log Viewer (' + total + ' results, ' + rangeLabel + ')';
    }
    return 'Log Viewer (Search: ' + total + ' results)';
}

function lvTriggerSearch() {
    lvReadDateTimeFilters();
    if (LV_STORE_ID && lvHasServerSearchFilters()) {
        lvCurrentPage = 1;
        lvSearchServer(lvSearchText, '', lvCurrentPage);
        return;
    }

    lvIsServerSearch = false;
    if (!lvSearchText && !lvStartTime && !lvEndTime) {
        lvDisplayStaticLogs(LV_INITIAL_LINES);
    } else {
        lvResetToLastPage();
        lvRenderTailPage();
    }
}

function lvOnSearchInput(value) {
    lvSearchText = (value || '').trim();
    if (lvSearchDebounceTimer) clearTimeout(lvSearchDebounceTimer);
    lvSearchDebounceTimer = setTimeout(lvTriggerSearch, 300);
}

function lvOnDateTimeInput() {
    if (lvSearchDebounceTimer) clearTimeout(lvSearchDebounceTimer);
    lvSearchDebounceTimer = setTimeout(lvTriggerSearch, 300);
}


function lvSearchServer(query, level, page) {
    var url = LOGS_SEARCH_URL + '?store_id=' + encodeURIComponent(LV_STORE_ID)
        + '&q=' + encodeURIComponent(query)
        + '&page=' + page
        + '&per_page=' + lvPerPage;
    if (level) url += '&level=' + encodeURIComponent(level);
    if (lvStartTime) url += '&start=' + encodeURIComponent(lvStartTime);
    if (lvEndTime) url += '&end=' + encodeURIComponent(lvEndTime);

    document.getElementById('lvTitle').textContent = 'Log Viewer (Searching...)';

    fetch(url).then(function(resp) {
        if (!resp.ok) throw new Error('Search failed: ' + resp.status);
        return resp.json();
    }).then(function(data) {
        lvDisplaySearchResults(data);
    }).catch(function(err) {
        console.error('Log search error:', err);
        document.getElementById('lvTitle').textContent = 'Log Viewer (Search failed)';
    });
}

function lvDisplaySearchResults(data) {
    lvIsServerSearch = true;
    lvTotalResults = data.total || 0;
    lvCurrentPage = data.page || 1;

    lvLogLines = (data.results || []).map(function(r) { return r.raw; });

    var totalPages = Math.ceil(lvTotalResults / lvPerPage) || 1;
    document.getElementById('lvTitle').textContent = lvBuildSearchTitle(lvTotalResults);

    lvRenderLines();

    var paginationEl = document.getElementById('lvPagination');
    paginationEl.style.display = 'flex';
    document.getElementById('lvBackToTail').style.display = 'inline-block';

    document.getElementById('lvPageInfo').textContent = 'Page ' + lvCurrentPage + ' of ' + totalPages;
    document.getElementById('lvPageTotal').textContent = lvTotalResults + ' total results';
    document.getElementById('lvPrevBtn').disabled = (lvCurrentPage <= 1);
    document.getElementById('lvNextBtn').disabled = (lvCurrentPage >= totalPages);

    var viewer = document.getElementById('lvViewer');
    viewer.scrollTop = 0;
}

function lvPrevPage() {
    if (lvCurrentPage <= 1) return;
    lvCurrentPage--;
    if (lvIsServerSearch) {
        lvSearchServer(lvSearchText, '', lvCurrentPage);
    } else {
        lvRenderTailPage();
    }
}

function lvNextPage() {
    if (lvIsServerSearch) {
        var totalPages = Math.ceil(lvTotalResults / lvPerPage) || 1;
        if (lvCurrentPage < totalPages) {
            lvCurrentPage++;
            lvSearchServer(lvSearchText, '', lvCurrentPage);
        }
    } else {
        var filtered = lvFilterLines();
        var totalPages = Math.max(1, Math.ceil(filtered.length / lvPerPage));
        if (lvCurrentPage < totalPages) {
            lvCurrentPage++;
            lvRenderTailPage();
        }
    }
}

function lvBackToTail() {
    lvIsServerSearch = false;
    lvSearchText = '';
    lvStartTime = '';
    lvEndTime = '';
    var searchInput = document.getElementById('lvSearchInput');
    if (searchInput) searchInput.value = '';
    var startInput = document.getElementById('lvStartInput');
    if (startInput) startInput.value = '';
    var endInput = document.getElementById('lvEndInput');
    if (endInput) endInput.value = '';
    lvDisplayStaticLogs(LV_INITIAL_LINES);
}


function miInitUploadResultsCharts() {
    if (cfg.plot && cfg.plot.data && typeof global.miRenderPlot === 'function') {
        global.miRenderPlot('plot', cfg.plot, {
            onRendered: function(elId) {
                buildInfoOverlay(elId);
                buildTableCopyOverlay(elId);
                bindInfoOverlayEvents(elId);
            }
        });
    }
    if (cfg.metricsPlot && cfg.metricsPlot.data && typeof global.miRenderPlot === 'function') {
        global.miRenderPlot('metrics-plot', cfg.metricsPlot, {
            onRendered: function(elId) {
                buildInfoOverlay(elId);
                bindInfoOverlayEvents(elId);
            }
        });
    }
}

function miInitUploadResultsPage() {
    miInitUploadResultsTabs();
    miInitSortableTableHeaders();
    miInitUploadResultsCharts();
    lvUpdateControlsState();
    var summaryRoot = document.getElementById('log-summary-root');
    if (summaryRoot && typeof global.miRenderProgressMonitor === 'function') {
        var payload = summaryPayload;
        if (!payload || (!payload.display && !payload.error)) {
            payload = {
                error: 'No summary is available for this saved analysis. Re-upload the log to generate the Summary tab.',
                pageTitle: 'Migration Summary',
                pageSubtitle: 'Snapshot from this log (not live)'
            };
        }
        global.miRenderProgressMonitor(summaryRoot, payload);
    }
}

// Expose handlers used from inline HTML attributes.
var exports = {
    switchTab: switchTab,
    copyTableAsMarkdown: copyTableAsMarkdown,
    toggleErrorDetail: toggleErrorDetail,
    filterTables: filterTables,
    filterErrorsTable: filterErrorsTable,
    copyProgressAsMarkdown: copyProgressAsMarkdown,
    copyErrorsAsMarkdown: copyErrorsAsMarkdown,
    copyErrorLog: copyErrorLog,
    sortTableByColumn: sortTableByColumn,
    filterPartitionInitTable: filterPartitionInitTable,
    copyPartitionInitAsMarkdown: copyPartitionInitAsMarkdown,
    filterBusiestCollectionsTable: filterBusiestCollectionsTable,
    copyBusiestCollectionsAsMarkdown: copyBusiestCollectionsAsMarkdown,
    lvFocusNamespace: lvFocusNamespace,
    copyNaturalOrderAsMarkdown: copyNaturalOrderAsMarkdown,
    lvToggleSeverity: lvToggleSeverity,
    lvSetViewMode: lvSetViewMode,
    lvToggleFocus: lvToggleFocus,
    lvClear: lvClear,
    lvDownloadLogs: lvDownloadLogs,
    lvOnSearchInput: lvOnSearchInput,
    lvOnDateTimeInput: lvOnDateTimeInput,
    lvPrevPage: lvPrevPage,
    lvNextPage: lvNextPage,
    lvBackToTail: lvBackToTail,
};
Object.keys(exports).forEach(function(key) { global[key] = exports[key]; });

global.miInitUploadResultsPage = miInitUploadResultsPage;

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', miInitUploadResultsPage);
} else {
    miInitUploadResultsPage();
}

})(typeof window !== 'undefined' ? window : this);
