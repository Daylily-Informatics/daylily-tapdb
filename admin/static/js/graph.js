/**
 * TAPDB Admin - Cytoscape Graph Visualization
 */

let cy = null;
let keyboardHandlersInstalled = false;
let pendingLineageChildId = null;
const tapTracker = new Map();
const keyState = {
    d: false,
    l: false,
};

const TAP_SEQUENCE_MS = 700;
const WAVE_STEP_MS = 260;
const WAVE_GLOW_MS = 520;

const cytoscapeStyle = [
    {
        selector: 'node',
        style: {
            'background-color': 'data(color)',
            'label': 'data(id)',  // Show EUID as label
            'color': '#fff',
            'text-valign': 'bottom',
            'text-halign': 'center',
            'font-size': '11px',
            'font-weight': 'bold',
            'text-margin-y': '5px',
            'width': '40px',
            'height': '40px',
            'border-width': '2px',
            'border-color': '#333',
            'text-outline-color': '#000',
            'text-outline-width': '2px',
            'shadow-color': '#000',
            'shadow-opacity': 0.25,
            'shadow-blur': 4,
            'transition-property': 'background-color, border-color, border-width, shadow-color, shadow-opacity, shadow-blur',
            'transition-duration': '420ms',
        }
    },
    {
        selector: 'node:selected',
        style: {
            'border-width': '4px',
            'border-color': '#fff',
            'background-color': '#e74c3c',
        }
    },
    {
        selector: 'node.link-anchor',
        style: {
            'border-color': '#ffe47a',
            'border-width': '5px',
            'shadow-color': '#ffe47a',
            'shadow-opacity': 0.95,
            'shadow-blur': 22,
        }
    },
    {
        selector: 'node.wave-child',
        style: {
            'background-color': '#ff4fa3',
            'border-color': '#ffd7ea',
            'border-width': '5px',
            'shadow-color': '#ff4fa3',
            'shadow-opacity': 0.9,
            'shadow-blur': 26,
        }
    },
    {
        selector: 'node.wave-parent',
        style: {
            'background-color': '#26d9ff',
            'border-color': '#cbf7ff',
            'border-width': '5px',
            'shadow-color': '#26d9ff',
            'shadow-opacity': 0.9,
            'shadow-blur': 26,
        }
    },
    {
        selector: 'edge',
        style: {
            // Directed edges: always render arrowhead on the *target* end.
            // Extra endpoint/distance settings keep arrowheads visible outside node borders.
            'width': 2,
            'line-color': '#666',
            'curve-style': 'bezier',
            'source-arrow-shape': 'none',
            'target-arrow-shape': 'triangle',
            'target-arrow-fill': 'filled',
            'target-arrow-color': '#666',
            'source-endpoint': 'outside-to-node',
            'target-endpoint': 'outside-to-node',
            'target-distance-from-node': 6,
            'arrow-scale': 1.6,
        }
    },
    {
        selector: 'edge:selected',
        style: {
            'line-color': '#e74c3c',
            'target-arrow-color': '#e74c3c',
            'width': 3,
        }
    }
];

function setStatus(message, level = '') {
    const el = document.getElementById('graph-mode-status');
    if (!el) {
        return;
    }
    el.textContent = message;
    el.className = '';
    if (level) {
        el.classList.add(level);
    }
}

function installKeyboardHandlers() {
    if (keyboardHandlersInstalled) {
        return;
    }
    keyboardHandlersInstalled = true;

    document.addEventListener('keydown', (evt) => {
        const key = (evt.key || '').toLowerCase();
        if (key === 'd') {
            keyState.d = true;
        }
        if (key === 'l') {
            keyState.l = true;
            if (!pendingLineageChildId) {
                setStatus('Link mode: hold L and click a child node.', 'warn');
            }
        }
        if (key === 'escape') {
            clearPendingLineageSelection();
            setStatus('Cleared selection.', 'warn');
        }
    });

    document.addEventListener('keyup', (evt) => {
        const key = (evt.key || '').toLowerCase();
        if (key === 'd') {
            keyState.d = false;
        }
        if (key === 'l') {
            keyState.l = false;
        }
    });
}

function registerTapSequence(nodeId, button) {
    const key = `${nodeId}|${button}`;
    const now = Date.now();
    const prev = tapTracker.get(key);

    let count = 1;
    if (prev && now - prev.lastTs <= TAP_SEQUENCE_MS) {
        count = prev.count + 1;
    }

    tapTracker.set(key, { count, lastTs: now });

    if (count >= 3) {
        tapTracker.set(key, { count: 0, lastTs: now });
        return true;
    }
    return false;
}

function clearPendingLineageSelection() {
    if (!pendingLineageChildId || !cy) {
        pendingLineageChildId = null;
        return;
    }
    const node = cy.getElementById(pendingLineageChildId);
    if (node && node.length > 0) {
        node.removeClass('link-anchor');
    }
    pendingLineageChildId = null;
}

function setPendingLineageChild(node) {
    clearPendingLineageSelection();
    pendingLineageChildId = node.id();
    node.addClass('link-anchor');
    setStatus(`Link mode: child ${pendingLineageChildId} selected. Click parent node.`, 'warn');
}

function collectWaveLevels(startNode, direction) {
    const levels = [];
    const visited = new Set([startNode.id()]);
    let frontier = cy.collection(startNode);

    while (frontier.length > 0) {
        let next = cy.collection();

        frontier.forEach((node) => {
            const neighbors = direction === 'children'
                ? node.incomers('edge').sources()
                : node.outgoers('edge').targets();

            neighbors.forEach((neighbor) => {
                const nid = neighbor.id();
                if (!visited.has(nid)) {
                    visited.add(nid);
                    next = next.add(neighbor);
                }
            });
        });

        if (next.length === 0) {
            break;
        }

        levels.push(next);
        frontier = next;
    }

    return levels;
}

function runWaveFromNode(startNode, direction) {
    if (!cy || !startNode) {
        return;
    }

    const levels = collectWaveLevels(startNode, direction);
    if (levels.length === 0) {
        setStatus(`No ${direction} found for ${startNode.id()}.`, 'warn');
        return;
    }

    const className = direction === 'children' ? 'wave-child' : 'wave-parent';
    const colorName = direction === 'children' ? 'pink' : 'aqua';
    setStatus(`Running ${direction} wave (${colorName}) from ${startNode.id()}...`, 'ok');

    levels.forEach((nodes, index) => {
        window.setTimeout(() => {
            nodes.addClass(className);
            window.setTimeout(() => {
                nodes.removeClass(className);
            }, WAVE_GLOW_MS);
        }, index * WAVE_STEP_MS);
    });
}

async function deleteGraphObject(ele) {
    const objectId = ele.data('id');
    const typeLabel = ele.isNode() ? 'node' : 'edge';

    try {
        const response = await fetch(`/api/object/${encodeURIComponent(objectId)}`, {
            method: 'DELETE',
            headers: {
                'Accept': 'application/json',
            },
        });

        let payload = {};
        try {
            payload = await response.json();
        } catch (_err) {
            // Non-JSON responses still handled by HTTP status.
        }

        if (!response.ok) {
            throw new Error(payload.detail || payload.message || `Failed to delete ${typeLabel}`);
        }

        // Remove clicked element locally for immediate feedback.
        if (ele && ele.length > 0) {
            ele.remove();
        }

        refreshLegendFromCurrentGraph();
        setStatus(`Deleted ${typeLabel} ${objectId}.`, 'ok');
    } catch (error) {
        console.error('Delete failed:', error);
        setStatus(`Delete failed: ${error.message}`, 'error');
    }
}

function pickRelationshipType(childId, parentId) {
    const dialog = document.getElementById('relationship-dialog');
    const selectEl = document.getElementById('relationship-type-select');
    const contextEl = document.getElementById('relationship-dialog-context');
    const cancelBtn = document.getElementById('relationship-dialog-cancel');
    const createBtn = document.getElementById('relationship-dialog-create');

    if (!dialog || !selectEl || !cancelBtn || !createBtn || typeof dialog.showModal !== 'function') {
        const entered = window.prompt(
            `Relationship type for child ${childId} -> parent ${parentId}:`,
            'generic'
        );
        const trimmed = (entered || '').trim();
        return Promise.resolve(trimmed || null);
    }

    contextEl.textContent = `Child: ${childId} -> Parent: ${parentId}`;
    selectEl.value = 'generic';

    return new Promise((resolve) => {
        const cleanup = () => {
            cancelBtn.removeEventListener('click', onCancel);
            createBtn.removeEventListener('click', onCreate);
            dialog.removeEventListener('cancel', onCancel);
        };

        const onCancel = () => {
            cleanup();
            dialog.close();
            resolve(null);
        };

        const onCreate = () => {
            const value = (selectEl.value || '').trim();
            cleanup();
            dialog.close();
            resolve(value || 'generic');
        };

        cancelBtn.addEventListener('click', onCancel);
        createBtn.addEventListener('click', onCreate);
        dialog.addEventListener('cancel', onCancel);
        dialog.showModal();
    });
}

async function createLineageEdge(childId, parentId, relationshipType) {
    const response = await fetch('/api/lineage', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        },
        body: JSON.stringify({
            child_euid: childId,
            parent_euid: parentId,
            relationship_type: relationshipType || 'generic',
        }),
    });

    let payload = {};
    try {
        payload = await response.json();
    } catch (_err) {
        // Non-JSON responses still handled by HTTP status.
    }

    if (!response.ok) {
        throw new Error(payload.detail || payload.message || `Failed to create edge (${response.status})`);
    }

    const edgeId = payload.euid || `edge-${Date.now()}`;
    cy.add({
        group: 'edges',
        data: {
            id: edgeId,
            source: childId,
            target: parentId,
            relationship_type: relationshipType || 'generic',
        },
    });

    refreshLegendFromCurrentGraph();
    setStatus(`Created edge ${childId} -> ${parentId} (${relationshipType || 'generic'}).`, 'ok');
}

function refreshLegendFromCurrentGraph() {
    if (!cy) {
        return;
    }
    const typesInGraph = {};
    cy.nodes().forEach((node) => {
        const data = node.data();
        const category = data.category;
        const color = data.color;
        if (category && color && !typesInGraph[category]) {
            typesInGraph[category] = color;
        }
    });
    updateLegend(typesInGraph);
}

function initCytoscape(container, elements) {
    if (cy) {
        cy.destroy();
    }

    installKeyboardHandlers();
    clearPendingLineageSelection();

    container.addEventListener('contextmenu', (evt) => {
        evt.preventDefault();
    });

    cy = cytoscape({
        container: container,
        elements: elements,
        style: cytoscapeStyle,
        // With child->parent directionality, use bottom-to-top so parents tend to render above children.
        layout: { name: 'dagre', rankDir: 'BT', nodeSep: 50, rankSep: 80 },
        minZoom: 0.1,
        maxZoom: 3,
        wheelSensitivity: 0.3,
    });

    // Left click on node.
    cy.on('tap', 'node', async function(evt) {
        const node = evt.target;
        showNodeInfo(node.data());

        if (pendingLineageChildId) {
            const childId = pendingLineageChildId;
            const parentId = node.id();

            if (childId === parentId) {
                setStatus('Child and parent cannot be the same node.', 'warn');
                clearPendingLineageSelection();
                return;
            }

            const relationshipType = await pickRelationshipType(childId, parentId);
            if (!relationshipType) {
                clearPendingLineageSelection();
                setStatus('Edge creation cancelled.', 'warn');
                return;
            }

            try {
                await createLineageEdge(childId, parentId, relationshipType);
            } catch (error) {
                console.error('Edge creation failed:', error);
                setStatus(`Edge creation failed: ${error.message}`, 'error');
            } finally {
                clearPendingLineageSelection();
            }
            return;
        }

        if (keyState.l) {
            setPendingLineageChild(node);
            return;
        }

        if (registerTapSequence(node.id(), 'left')) {
            runWaveFromNode(node, 'children');
        }
    });

    // Right click on node.
    cy.on('cxttap', 'node', async function(evt) {
        const node = evt.target;

        if (keyState.d) {
            await deleteGraphObject(node);
            return;
        }

        showNodeInfo(node.data());

        if (registerTapSequence(node.id(), 'right')) {
            runWaveFromNode(node, 'parents');
        }
    });

    // Left click on edge.
    cy.on('tap', 'edge', function(evt) {
        const edge = evt.target;
        showEdgeInfo(edge.data());
    });

    // Right click on edge.
    cy.on('cxttap', 'edge', async function(evt) {
        const edge = evt.target;

        if (keyState.d) {
            await deleteGraphObject(edge);
            return;
        }

        showEdgeInfo(edge.data());
    });

    // Double-click to navigate.
    cy.on('dbltap', 'node', function(evt) {
        const euid = evt.target.data('id');
        window.location.href = '/object/' + euid;
    });

    setStatus('Ready.', '');
    return cy;
}

function escapeHtml(value) {
    return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function prettyJson(value) {
    if (value === undefined) {
        return '{}';
    }
    return JSON.stringify(value === null ? {} : value, null, 2);
}

function topLevelRowValue(key, value) {
    if (value === null || value === undefined || value === '') {
        return '<span style="color: var(--text-muted);">-</span>';
    }
    if (key === 'json_addl') {
        return '<span style="color: var(--text-muted);">See JSON section below</span>';
    }
    if (typeof value === 'object') {
        return `<code>${escapeHtml(JSON.stringify(value))}</code>`;
    }
    return escapeHtml(String(value));
}

function renderDetailsPanel({ euid, objectData, graphData, isNode }) {
    const content = document.getElementById('node-info-content');
    if (!content) {
        return;
    }

    const merged = { ...(objectData || {}) };
    if (!Object.prototype.hasOwnProperty.call(merged, 'euid')) {
        merged.euid = euid;
    }

    const preferredKeys = [
        'uuid',
        'euid',
        'name',
        'type',
        'obj_type',
        'category',
        'subtype',
        'version',
        'bstatus',
        'source',
        'target',
        'relationship_type',
        'created_dt',
        'json_addl',
    ];
    const remainingKeys = Object.keys(merged)
        .filter((k) => !preferredKeys.includes(k))
        .sort();
    const keys = preferredKeys.filter((k) => Object.prototype.hasOwnProperty.call(merged, k)).concat(remainingKeys);

    const topLevelRows = keys.map((key) => `
        <div class="detail-key">${escapeHtml(key)}</div>
        <div class="detail-value">${topLevelRowValue(key, merged[key])}</div>
    `).join('');

    const rawObjectPayload = objectData || {};
    const graphPayload = graphData || {};
    const jsonPayload = Object.prototype.hasOwnProperty.call(merged, 'json_addl')
        ? merged.json_addl
        : {};

    const actions = `
        <div style="display: flex; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 0.8rem;">
            <a href="/object/${encodeURIComponent(euid)}" class="btn">View Details</a>
            ${
                isNode
                    ? `<button onclick="centerOnNode('${escapeHtml(euid)}')" class="btn">Center on Node</button>`
                    : ''
            }
        </div>
    `;

    content.innerHTML = `
        ${actions}
        <div class="details-section-title">Top-Level Properties</div>
        <div class="details-grid">${topLevelRows || '<span style="color: var(--text-muted);">No properties.</span>'}</div>
        <div class="details-section-title">Raw Object JSON</div>
        <pre class="json-block">${escapeHtml(prettyJson(rawObjectPayload))}</pre>
        <div class="details-section-title">JSON (json_addl)</div>
        <pre class="json-block">${escapeHtml(prettyJson(jsonPayload))}</pre>
        <div class="details-section-title">Graph Payload</div>
        <pre class="json-block">${escapeHtml(prettyJson(graphPayload))}</pre>
    `;
}

async function fetchObjectData(euid) {
    const response = await fetch(`/api/object/${encodeURIComponent(euid)}`, {
        headers: {
            Accept: 'application/json',
        },
    });
    if (!response.ok) {
        throw new Error(`Failed to load object details (${response.status})`);
    }
    return response.json();
}

async function showNodeInfo(data) {
    const content = document.getElementById('node-info-content');
    if (content) {
        content.innerHTML = '<p style="color: var(--text-muted);">Loading node details...</p>';
    }
    try {
        const objectData = await fetchObjectData(data.id);
        renderDetailsPanel({
            euid: data.id,
            objectData,
            graphData: data,
            isNode: true,
        });
    } catch (error) {
        console.error('Failed to load node details:', error);
        renderDetailsPanel({
            euid: data.id,
            objectData: {
                euid: data.id,
                name: data.name,
                category: data.category,
                type: data.type,
                subtype: data.subtype,
            },
            graphData: data,
            isNode: true,
        });
        setStatus(`Could not load full node details: ${error.message}`, 'warn');
    }
}

async function showEdgeInfo(data) {
    const content = document.getElementById('node-info-content');
    if (content) {
        content.innerHTML = '<p style="color: var(--text-muted);">Loading edge details...</p>';
    }
    try {
        const objectData = await fetchObjectData(data.id);
        renderDetailsPanel({
            euid: data.id,
            objectData,
            graphData: data,
            isNode: false,
        });
    } catch (error) {
        console.error('Failed to load edge details:', error);
        renderDetailsPanel({
            euid: data.id,
            objectData: {
                euid: data.id,
                source: data.source,
                target: data.target,
                relationship_type: data.relationship_type || 'related',
            },
            graphData: data,
            isNode: false,
        });
        setStatus(`Could not load full edge details: ${error.message}`, 'warn');
    }
}

function centerOnNode(nodeId) {
    const node = cy.getElementById(nodeId);
    if (node.length > 0) {
        cy.animate({
            center: { eles: node },
            zoom: 1.5
        }, { duration: 300 });
        node.select();
    }
}

function applyLayout() {
    if (!cy) {
        return;
    }

    const layoutName = document.getElementById('layout-select').value;
    const layoutOptions = {
        dagre: { name: 'dagre', rankDir: 'BT', nodeSep: 50, rankSep: 80 },
        cose: { name: 'cose', animate: true, animationDuration: 500 },
        breadthfirst: { name: 'breadthfirst', directed: true, spacingFactor: 1.5 },
        circle: { name: 'circle' },
        grid: { name: 'grid' },
    };

    cy.layout(layoutOptions[layoutName] || { name: layoutName }).run();
}

async function loadGraph() {
    const startEuid = document.getElementById('start-euid').value;
    const depth = document.getElementById('depth').value;

    const container = document.getElementById('cy');
    container.innerHTML = '<div class="loading">Loading graph data...</div>';

    try {
        let url = '/api/graph/data?depth=' + depth;
        if (startEuid) {
            url += '&start_euid=' + encodeURIComponent(startEuid);
        }

        const response = await fetch(url);
        const data = await response.json();

        if (data.elements.nodes.length === 0) {
            container.innerHTML = '<div class="loading">No data found. Try a different EUID or leave empty for all.</div>';
            updateLegend({});
            setStatus('No graph data found for this query.', 'warn');
            return;
        }

        container.innerHTML = '';
        initCytoscape(container, data.elements);

        // Build dynamic legend from node categories in the graph.
        const typesInGraph = {};
        data.elements.nodes.forEach((node) => {
            const category = node.data.category;
            const color = node.data.color;
            if (category && !typesInGraph[category]) {
                typesInGraph[category] = color;
            }
        });
        updateLegend(typesInGraph);

        // Update URL without reload.
        const newUrl = '/graph?start_euid=' + encodeURIComponent(startEuid) + '&depth=' + depth;
        window.history.replaceState({}, '', newUrl);

    } catch (error) {
        console.error('Error loading graph:', error);
        container.innerHTML = '<div class="loading">Error loading graph: ' + error.message + '</div>';
        updateLegend({});
        setStatus(`Load failed: ${error.message}`, 'error');
    }
}

function updateLegend(typesInGraph) {
    const legendContainer = document.getElementById('legend-items');
    if (!legendContainer) {
        return;
    }

    if (Object.keys(typesInGraph).length === 0) {
        legendContainer.innerHTML = '<span style="color: var(--text-muted); font-size: 0.85rem;">No nodes in graph</span>';
        return;
    }

    // Sort types alphabetically.
    const sortedTypes = Object.keys(typesInGraph).sort();

    legendContainer.innerHTML = sortedTypes.map((type) => `
        <div class="legend-item">
            <div class="legend-color" style="background:${typesInGraph[type]}"></div>
            ${type}
        </div>
    `).join('');
}
