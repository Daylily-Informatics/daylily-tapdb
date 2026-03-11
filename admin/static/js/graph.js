/**
 * TAPDB Admin - Cytoscape Graph Visualization
 */

const TAPDB_BASE_PATH =
    typeof window !== 'undefined' && window.TAPDB_BASE_PATH
        ? String(window.TAPDB_BASE_PATH).replace(/\/+$/, '')
        : '';
const graphBootstrap =
    typeof window !== 'undefined' && window.TAPDB_GRAPH_BOOTSTRAP
        ? window.TAPDB_GRAPH_BOOTSTRAP
        : {};
let pendingAutoMergeRef =
    Number.isInteger(graphBootstrap.defaultMergeRef) ? graphBootstrap.defaultMergeRef : null;

function tapdbUrl(path) {
    const normalized = (path || '').startsWith('/') ? path : `/${path}`;
    return `${TAPDB_BASE_PATH}${normalized}`;
}

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
        selector: 'node[is_external]',
        style: {
            'border-style': 'dashed',
            'border-color': '#f0d79d',
            'background-opacity': 0.78,
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
        selector: 'edge[is_external]',
        style: {
            'line-style': 'dashed',
            'line-color': '#c1a967',
            'target-arrow-color': '#c1a967',
        }
    },
    {
        selector: 'edge[is_external_bridge]',
        style: {
            'line-style': 'dotted',
            'line-color': '#f7c948',
            'target-arrow-color': '#f7c948',
            'width': 3,
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
    if (ele.data('is_external') || ele.data('is_external_bridge')) {
        setStatus(`External ${typeLabel}s are read-only.`, 'warn');
        return;
    }

    try {
        const response = await fetch(tapdbUrl(`/api/object/${encodeURIComponent(objectId)}`), {
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
    const response = await fetch(tapdbUrl('/api/lineage'), {
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

        if (node.data('is_external')) {
            return;
        }

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
        if (evt.target.data('is_external')) {
            return;
        }
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

function currentDepth() {
    const raw = document.getElementById('depth')?.value || '4';
    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) ? parsed : 4;
}

function buildExternalGraphUrl(sourceEuid, refIndex) {
    const params = new URLSearchParams({
        source_euid: sourceEuid,
        ref_index: String(refIndex),
        depth: String(currentDepth()),
    });
    return `${tapdbUrl('/api/graph/external')}?${params.toString()}`;
}

function buildExternalObjectUrl(sourceEuid, refIndex, euid) {
    const params = new URLSearchParams({
        source_euid: sourceEuid,
        ref_index: String(refIndex),
        euid,
    });
    return `${tapdbUrl('/api/graph/external/object')}?${params.toString()}`;
}

function renderExternalRefs(refs, sourceEuid, allowMerge) {
    if (!Array.isArray(refs) || refs.length === 0) {
        return '';
    }
    const rows = refs.map((ref) => {
        const openRemote = ref.href
            ? `<a href="${escapeHtml(ref.href)}" class="btn" target="_blank" rel="noreferrer">Open Remote</a>`
            : '';
        const mergeButton = allowMerge
            ? `<button class="btn" onclick="mergeExternalRef('${escapeHtml(sourceEuid)}', ${Number(ref.ref_index || 0)})"` +
                `${ref.graph_expandable ? '' : ' disabled'}` +
                `>Merge External Graph</button>`
            : '';
        const disabledReason = !ref.graph_expandable && ref.reason
            ? `<div style="color: var(--text-muted); font-size: 0.8rem;">${escapeHtml(ref.reason)}</div>`
            : '';
        return `
            <div style="border:1px solid var(--border-color); border-radius:6px; padding:0.65rem; margin-bottom:0.6rem;">
                <div style="font-weight:600;">${escapeHtml(ref.label || ref.root_euid || 'External reference')}</div>
                <div style="font-size:0.82rem; color:var(--text-muted); margin:0.25rem 0 0.5rem 0;">
                    ${escapeHtml(ref.system || 'external')} :: ${escapeHtml(ref.root_euid || '-')}
                    ${ref.tenant_id ? ` :: ${escapeHtml(ref.tenant_id)}` : ''}
                </div>
                <div style="display:flex; gap:0.5rem; flex-wrap:wrap;">${openRemote}${mergeButton}</div>
                ${disabledReason}
            </div>
        `;
    }).join('');
    return `<div class="details-section-title">External References</div>${rows}`;
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
    const externalRefs = Array.isArray(merged.external_refs) ? merged.external_refs : [];
    const canRenderExternalRefs = isNode && !(graphData && graphData.is_external);
    const sourceEuid = graphData && graphData.external_source_euid ? graphData.external_source_euid : euid;
    const externalRefSection = renderExternalRefs(externalRefs, sourceEuid, canRenderExternalRefs);
    const canViewLocalDetail = !(graphData && graphData.is_external);

    const actions = `
        <div style="display: flex; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 0.8rem;">
            ${
                canViewLocalDetail
                    ? `<a href="${tapdbUrl(`/object/${encodeURIComponent(euid)}`)}" class="btn">View Details</a>`
                    : ''
            }
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
        ${externalRefSection}
        <div class="details-section-title">JSON (json_addl)</div>
        <pre class="json-block">${escapeHtml(prettyJson(jsonPayload))}</pre>
        <div class="details-section-title">Graph Payload</div>
        <pre class="json-block">${escapeHtml(prettyJson(graphPayload))}</pre>
    `;
}

async function fetchObjectData(euid) {
    const response = await fetch(tapdbUrl(`/api/object/${encodeURIComponent(euid)}`), {
        headers: {
            Accept: 'application/json',
        },
    });
    if (!response.ok) {
        throw new Error(`Failed to load object details (${response.status})`);
    }
    return response.json();
}

async function fetchExternalObjectData(sourceEuid, refIndex, euid) {
    const response = await fetch(buildExternalObjectUrl(sourceEuid, refIndex, euid), {
        headers: {
            Accept: 'application/json',
        },
    });
    if (!response.ok) {
        let payload = {};
        try {
            payload = await response.json();
        } catch (_err) {
            payload = {};
        }
        throw new Error(payload.detail || `Failed to load external object details (${response.status})`);
    }
    return response.json();
}

async function mergeExternalRef(sourceEuid, refIndex) {
    if (!cy) {
        setStatus('Load a graph before merging an external reference.', 'warn');
        return;
    }

    try {
        const response = await fetch(buildExternalGraphUrl(sourceEuid, refIndex), {
            headers: { Accept: 'application/json' },
        });
        const payload = await response.json();
        if (!response.ok) {
            throw new Error(payload.detail || `Failed to merge external graph (${response.status})`);
        }

        const elements = payload.elements || {};
        const nodes = Array.isArray(elements.nodes) ? elements.nodes : [];
        const edges = Array.isArray(elements.edges) ? elements.edges : [];
        const existingIds = new Set(cy.elements().map((ele) => ele.id()));
        const additions = [];
        [...nodes, ...edges].forEach((element) => {
            const id = element && element.data ? element.data.id : null;
            if (!id || existingIds.has(id)) {
                return;
            }
            additions.push(element);
            existingIds.add(id);
        });

        if (additions.length === 0) {
            setStatus('External graph already merged.', 'warn');
            return;
        }

        cy.add(additions);
        applyLayout();
        refreshLegendFromCurrentGraph();
        setStatus(`Merged external graph for ${sourceEuid}.`, 'ok');
    } catch (error) {
        console.error('Failed to merge external graph:', error);
        setStatus(`External merge failed: ${error.message}`, 'error');
    }
}

async function showNodeInfo(data) {
    const content = document.getElementById('node-info-content');
    if (content) {
        content.innerHTML = '<p style="color: var(--text-muted);">Loading node details...</p>';
    }
    try {
        const objectData = data.is_external
            ? await fetchExternalObjectData(data.external_source_euid, data.source_ref_index, data.remote_euid || data.id)
            : await fetchObjectData(data.id);
        renderDetailsPanel({
            euid: data.is_external ? (data.remote_euid || data.id) : data.id,
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
        const objectData = (data.is_external || data.is_external_bridge) && data.remote_euid
            ? await fetchExternalObjectData(data.external_source_euid, data.source_ref_index, data.remote_euid)
            : await fetchObjectData(data.id);
        renderDetailsPanel({
            euid: (data.is_external || data.is_external_bridge) ? (data.remote_euid || data.id) : data.id,
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
        let url = tapdbUrl('/api/graph/data') + '?depth=' + depth;
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
        const newUrl = tapdbUrl('/graph') + '?start_euid=' + encodeURIComponent(startEuid) + '&depth=' + depth;
        window.history.replaceState({}, '', newUrl);
        if (pendingAutoMergeRef !== null && startEuid) {
            const mergeRef = pendingAutoMergeRef;
            pendingAutoMergeRef = null;
            await mergeExternalRef(startEuid, mergeRef);
        }

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

    const staticItems = [
        '<div class="legend-item"><div class="legend-color" style="background:#666"></div>Local</div>',
        '<div class="legend-item"><div class="legend-color" style="background:#c1a967"></div>External</div>',
        '<div class="legend-item"><div class="legend-color" style="background:#f7c948"></div>Bridge</div>',
    ];
    if (Object.keys(typesInGraph).length === 0) {
        legendContainer.innerHTML = staticItems.join('');
        return;
    }

    const sortedTypes = Object.keys(typesInGraph).sort();

    legendContainer.innerHTML = staticItems.join('') + sortedTypes.map((type) => `
        <div class="legend-item">
            <div class="legend-color" style="background:${typesInGraph[type]}"></div>
            ${type}
        </div>
    `).join('');
}

window.mergeExternalRef = mergeExternalRef;
