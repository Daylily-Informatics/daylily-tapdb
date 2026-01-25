/**
 * TAPDB Admin - Cytoscape Graph Visualization
 */

let cy = null;

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

function initCytoscape(container, elements) {
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

    // Node click handler
    cy.on('tap', 'node', function(evt) {
        const node = evt.target;
        showNodeInfo(node.data());
    });

    // Edge click handler
    cy.on('tap', 'edge', function(evt) {
        const edge = evt.target;
        showEdgeInfo(edge.data());
    });

    // Double-click to navigate
    cy.on('dbltap', 'node', function(evt) {
        const euid = evt.target.data('id');
        window.location.href = '/object/' + euid;
    });

    return cy;
}

function showNodeInfo(data) {
    const content = document.getElementById('node-info-content');
    content.innerHTML = `
        <p><strong>EUID:</strong> <a href="/object/${data.id}">${data.id}</a></p>
        <p><strong>Name:</strong> ${data.name || '-'}</p>
        <p><strong>Category:</strong> ${data.category || '-'}</p>
        <p><strong>Type:</strong> ${data.type || '-'}</p>
        <p><strong>Subtype:</strong> ${data.subtype || '-'}</p>
        <hr style="border-color: var(--border-color); margin: 0.75rem 0;">
        <a href="/object/${data.id}" class="btn" style="width: 100%; text-align: center;">View Details</a>
        <button onclick="centerOnNode('${data.id}')" class="btn" style="width: 100%; margin-top: 0.5rem;">Center on Node</button>
    `;
}

function showEdgeInfo(data) {
    const content = document.getElementById('node-info-content');
    content.innerHTML = `
        <p><strong>Lineage EUID:</strong> <a href="/object/${data.id}">${data.id}</a></p>
		<p><strong>Child (source):</strong> <a href="/object/${data.source}">${data.source}</a></p>
		<p><strong>Parent (target):</strong> <a href="/object/${data.target}">${data.target}</a></p>
        <p><strong>Relationship:</strong> ${data.relationship_type || 'related'}</p>
        <hr style="border-color: var(--border-color); margin: 0.75rem 0;">
        <a href="/object/${data.id}" class="btn" style="width: 100%; text-align: center;">View Lineage</a>
    `;
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
            return;
        }

        container.innerHTML = '';
        initCytoscape(container, data.elements);

        // Build dynamic legend from node categories in the graph
        const typesInGraph = {};
        data.elements.nodes.forEach(node => {
            const category = node.data.category;
            const color = node.data.color;
            if (category && !typesInGraph[category]) {
                typesInGraph[category] = color;
            }
        });
        updateLegend(typesInGraph);

        // Update URL without reload
        const newUrl = '/graph?start_euid=' + encodeURIComponent(startEuid) + '&depth=' + depth;
        window.history.replaceState({}, '', newUrl);

    } catch (error) {
        console.error('Error loading graph:', error);
        container.innerHTML = '<div class="loading">Error loading graph: ' + error.message + '</div>';
        updateLegend({});
    }
}

function updateLegend(typesInGraph) {
    const legendContainer = document.getElementById('legend-items');
    if (!legendContainer) return;

    if (Object.keys(typesInGraph).length === 0) {
        legendContainer.innerHTML = '<span style="color: var(--text-muted); font-size: 0.85rem;">No nodes in graph</span>';
        return;
    }

    // Sort types alphabetically
    const sortedTypes = Object.keys(typesInGraph).sort();

    legendContainer.innerHTML = sortedTypes.map(type => `
        <div class="legend-item">
            <div class="legend-color" style="background:${typesInGraph[type]}"></div>
            ${type}
        </div>
    `).join('');
}

