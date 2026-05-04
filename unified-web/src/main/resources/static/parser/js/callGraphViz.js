window.PA = window.PA || {};

PA.callGraphViz = {
    _canvas: null,
    _ctx: null,
    _allNodes: [],
    _allEdges: [],
    _nodes: [],
    _edges: [],
    _transform: { x: 0, y: 0, scale: 1 },
    _drag: null,
    _hoveredNode: null,
    _selectedNode: null,
    _expandedNode: null,
    _visibleIds: null,
    _animFrame: null,

    load: async function() {
        var container = document.getElementById('graphContainer');
        if (!container) return;

        container.innerHTML = '<canvas id="graphCanvas"></canvas><div class="graph-controls" id="graphControls"></div><div class="graph-tooltip" id="graphTooltip" style="display:none"></div>';

        try {
            var data = await PA.api.getCallGraph();
            if (!data || !data.edges || data.edges.length === 0) {
                container.innerHTML = '<div class="empty-msg">No call graph data available</div>';
                return;
            }
            PA.callGraphViz._buildGraph(data);
            PA.callGraphViz._initCanvas();
            PA.callGraphViz._renderControls();
            PA.callGraphViz._simulate();
        } catch (e) {
            console.error('[PA] callGraphViz.load failed', e);
            container.innerHTML = '<div class="empty-msg">Failed to load call graph</div>';
        }
    },

    _buildGraph: function(data) {
        var nodeMap = {};
        var edges = data.edges || [];
        var indexNodes = PA.analysisData ? PA.analysisData.nodes : [];

        for (var i = 0; i < indexNodes.length; i++) {
            var n = indexNodes[i];
            var nid = n.nodeId || n.name || '';
            nodeMap[nid] = {
                id: nid, label: n.name || nid,
                schema: n.schema || '', objectType: n.objectType || '',
                depth: n.depth != null ? n.depth : 0,
                loc: n.linesOfCode || 0,
                x: Math.random() * 600 - 300, y: Math.random() * 400 - 200,
                vx: 0, vy: 0, radius: 0, inEdges: 0, outEdges: 0
            };
        }

        var graphEdges = [];
        for (var i = 0; i < edges.length; i++) {
            var e = edges[i];
            var fromId = e.fromNodeId || e.from || '';
            var toId = e.toNodeId || e.to || '';
            if (!nodeMap[fromId] && e.from) {
                nodeMap[fromId] = {
                    id: fromId, label: e.from, schema: e.fromSchema || '',
                    objectType: '', depth: 0, loc: 0,
                    x: Math.random() * 600 - 300, y: Math.random() * 400 - 200,
                    vx: 0, vy: 0, radius: 0, inEdges: 0, outEdges: 0
                };
            }
            if (!nodeMap[toId] && e.to) {
                nodeMap[toId] = {
                    id: toId, label: e.to, schema: e.toSchema || '',
                    objectType: '', depth: 0, loc: 0,
                    x: Math.random() * 600 - 300, y: Math.random() * 400 - 200,
                    vx: 0, vy: 0, radius: 0, inEdges: 0, outEdges: 0
                };
            }
            if (nodeMap[fromId]) nodeMap[fromId].outEdges++;
            if (nodeMap[toId]) nodeMap[toId].inEdges++;
            graphEdges.push({
                from: fromId, to: toId,
                count: e.count || 1, backEdge: e.backEdge || false,
                callType: e.callType || ''
            });
        }

        var nodes = Object.values(nodeMap);
        var maxEdges = 1;
        for (var i = 0; i < nodes.length; i++) {
            var total = nodes[i].inEdges + nodes[i].outEdges;
            if (total > maxEdges) maxEdges = total;
        }
        for (var i = 0; i < nodes.length; i++) {
            var total = nodes[i].inEdges + nodes[i].outEdges;
            var baseR = Math.max(8, Math.min(24, 8 + (total / maxEdges) * 16));
            if (nodes[i].depth === 0 && nodes[i].inEdges === 0) baseR = Math.max(baseR, 18);
            nodes[i].radius = baseR;
        }

        PA.callGraphViz._allNodes = nodes;
        PA.callGraphViz._allEdges = graphEdges;
        PA.callGraphViz._buildChildMap();
        var rootIds = PA.callGraphViz._getRootIds();
        PA.callGraphViz._visibleIds = new Set(rootIds);
        PA.callGraphViz._expandedNode = null;
        PA.callGraphViz._applyVisibility();
    },

    applyScope: function() {
        PA.callGraphViz._applyVisibility();
        if (PA.callGraphViz._canvas) {
            PA.callGraphViz._draw();
            PA.callGraphViz._updateControlsCount();
        }
    },

    _buildChildMap: function() {
        var edges = PA.callGraphViz._allEdges;
        var childMap = {};
        var parentMap = {};
        for (var i = 0; i < edges.length; i++) {
            var from = edges[i].from;
            var to = edges[i].to;
            if (!childMap[from]) childMap[from] = [];
            childMap[from].push(to);
            parentMap[to] = true;
        }
        PA.callGraphViz._childMap = childMap;
        PA.callGraphViz._parentMap = parentMap;
    },

    _getRootIds: function() {
        var nodes = PA.callGraphViz._allNodes;
        var parentMap = PA.callGraphViz._parentMap || {};
        var roots = [];
        for (var i = 0; i < nodes.length; i++) {
            if (!parentMap[nodes[i].id]) roots.push(nodes[i].id);
        }
        if (roots.length === 0 && nodes.length > 0) roots.push(nodes[0].id);
        return roots;
    },

    _applyVisibility: function() {
        var visibleIds = PA.callGraphViz._visibleIds;
        if (!visibleIds) {
            PA.callGraphViz._nodes = PA.callGraphViz._allNodes;
            PA.callGraphViz._edges = PA.callGraphViz._allEdges;
            return;
        }
        var nodeMap = {};
        PA.callGraphViz._nodes = PA.callGraphViz._allNodes.filter(function(n) {
            if (visibleIds.has(n.id)) { nodeMap[n.id] = true; return true; }
            return false;
        });
        PA.callGraphViz._edges = PA.callGraphViz._allEdges.filter(function(e) {
            return nodeMap[e.from] && nodeMap[e.to];
        });
    },

    expandNode: function(nodeId) {
        var childMap = PA.callGraphViz._childMap || {};
        var children = childMap[nodeId] || [];
        var roots = new Set(PA.callGraphViz._getRootIds());
        var vis = new Set(roots);
        vis.add(nodeId);
        for (var i = 0; i < children.length; i++) vis.add(children[i]);
        PA.callGraphViz._visibleIds = vis;
        PA.callGraphViz._expandedNode = nodeId;
        PA.callGraphViz._applyVisibility();
        PA.callGraphViz._updateControlsCount();
        if (PA.callGraphViz._animFrame) cancelAnimationFrame(PA.callGraphViz._animFrame);
        PA.callGraphViz._simulate();
    },

    collapseAll: function() {
        var roots = new Set(PA.callGraphViz._getRootIds());
        PA.callGraphViz._visibleIds = roots;
        PA.callGraphViz._expandedNode = null;
        PA.callGraphViz._applyVisibility();
        PA.callGraphViz._updateControlsCount();
        PA.callGraphViz._draw();
    },

    showAll: function() {
        PA.callGraphViz._visibleIds = null;
        PA.callGraphViz._expandedNode = null;
        PA.callGraphViz._applyVisibility();
        PA.callGraphViz._updateControlsCount();
        if (PA.callGraphViz._animFrame) cancelAnimationFrame(PA.callGraphViz._animFrame);
        PA.callGraphViz._simulate();
    },

    _updateControlsCount: function() {
        var ctrl = document.getElementById('graphControls');
        if (!ctrl) return;
        var span = ctrl.querySelector('span');
        if (span) span.textContent = PA.callGraphViz._nodes.length + ' nodes, ' + PA.callGraphViz._edges.length + ' edges';
    },

    _initCanvas: function() {
        var canvas = document.getElementById('graphCanvas');
        if (!canvas) return;
        var container = canvas.parentElement;
        canvas.width = container.clientWidth;
        canvas.height = container.clientHeight;
        PA.callGraphViz._canvas = canvas;
        PA.callGraphViz._ctx = canvas.getContext('2d');
        PA.callGraphViz._transform = { x: canvas.width / 2, y: canvas.height / 2, scale: 1 };

        canvas.addEventListener('mousedown', PA.callGraphViz._onMouseDown);
        canvas.addEventListener('mousemove', PA.callGraphViz._onMouseMove);
        canvas.addEventListener('mouseup', PA.callGraphViz._onMouseUp);
        canvas.addEventListener('wheel', PA.callGraphViz._onWheel, { passive: false });
        canvas.addEventListener('dblclick', PA.callGraphViz._onDblClick);

        var ro = new ResizeObserver(function() {
            var oldW = canvas.width;
            var oldH = canvas.height;
            var newW = container.clientWidth;
            var newH = container.clientHeight;
            if (newW === 0 || newH === 0) return;
            canvas.width = newW;
            canvas.height = newH;
            if (oldW === 0 || oldH === 0) {
                PA.callGraphViz._transform = { x: newW / 2, y: newH / 2, scale: 1 };
            }
            PA.callGraphViz._draw();
        });
        ro.observe(container);
    },

    _simulate: function() {
        var nodes = PA.callGraphViz._nodes;
        var edges = PA.callGraphViz._edges;
        var nodeMap = {};
        for (var i = 0; i < nodes.length; i++) nodeMap[nodes[i].id] = nodes[i];

        var iterations = 0;
        var maxIter = 200;

        function step() {
            if (iterations >= maxIter) { PA.callGraphViz._draw(); return; }
            iterations++;
            var alpha = 1 - iterations / maxIter;

            for (var i = 0; i < nodes.length; i++) {
                for (var j = i + 1; j < nodes.length; j++) {
                    var dx = nodes[j].x - nodes[i].x;
                    var dy = nodes[j].y - nodes[i].y;
                    var dist = Math.sqrt(dx * dx + dy * dy) || 1;
                    var force = 5000 / (dist * dist);
                    var fx = (dx / dist) * force * alpha;
                    var fy = (dy / dist) * force * alpha;
                    nodes[i].vx -= fx; nodes[i].vy -= fy;
                    nodes[j].vx += fx; nodes[j].vy += fy;
                }
            }

            for (var i = 0; i < edges.length; i++) {
                var src = nodeMap[edges[i].from];
                var tgt = nodeMap[edges[i].to];
                if (!src || !tgt) continue;
                var dx = tgt.x - src.x;
                var dy = tgt.y - src.y;
                var dist = Math.sqrt(dx * dx + dy * dy) || 1;
                var ideal = 120;
                var force = (dist - ideal) * 0.02 * alpha;
                var fx = (dx / dist) * force;
                var fy = (dy / dist) * force;
                src.vx += fx; src.vy += fy;
                tgt.vx -= fx; tgt.vy -= fy;
            }

            for (var i = 0; i < nodes.length; i++) {
                nodes[i].vx *= 0.8; nodes[i].vy *= 0.8;
                nodes[i].x += nodes[i].vx;
                nodes[i].y += nodes[i].vy;
            }

            PA.callGraphViz._draw();
            PA.callGraphViz._animFrame = requestAnimationFrame(step);
        }

        step();
    },

    _draw: function() {
        var ctx = PA.callGraphViz._ctx;
        var canvas = PA.callGraphViz._canvas;
        if (!ctx || !canvas) return;
        var t = PA.callGraphViz._transform;
        var nodes = PA.callGraphViz._nodes;
        var edges = PA.callGraphViz._edges;
        var nodeMap = {};
        for (var i = 0; i < nodes.length; i++) nodeMap[nodes[i].id] = nodes[i];

        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.save();
        ctx.translate(t.x, t.y);
        ctx.scale(t.scale, t.scale);

        for (var i = 0; i < edges.length; i++) {
            var e = edges[i];
            var src = nodeMap[e.from];
            var tgt = nodeMap[e.to];
            if (!src || !tgt) continue;

            var edgeDepthColor = PA.callGraphViz._getDepthColor(src.depth);
            var edgeStroke = e.backEdge ? 'rgba(239,68,68,0.5)' : edgeDepthColor.ring;
            var edgeAlpha = e.backEdge ? 0.5 : 0.25;

            ctx.beginPath();
            ctx.moveTo(src.x, src.y);
            ctx.lineTo(tgt.x, tgt.y);
            ctx.globalAlpha = edgeAlpha;
            ctx.strokeStyle = edgeStroke;
            ctx.lineWidth = Math.min(3, 0.5 + (e.count || 1) * 0.5);
            if (e.backEdge) ctx.setLineDash([4, 3]);
            else ctx.setLineDash([]);
            ctx.stroke();
            ctx.globalAlpha = 1;

            var dx = tgt.x - src.x;
            var dy = tgt.y - src.y;
            var dist = Math.sqrt(dx * dx + dy * dy) || 1;
            var arrowDist = dist - tgt.radius - 4;
            if (arrowDist > 10) {
                var ax = src.x + (dx / dist) * arrowDist;
                var ay = src.y + (dy / dist) * arrowDist;
                var angle = Math.atan2(dy, dx);
                ctx.beginPath();
                ctx.moveTo(ax, ay);
                ctx.lineTo(ax - 6 * Math.cos(angle - 0.4), ay - 6 * Math.sin(angle - 0.4));
                ctx.lineTo(ax - 6 * Math.cos(angle + 0.4), ay - 6 * Math.sin(angle + 0.4));
                ctx.closePath();
                ctx.globalAlpha = e.backEdge ? 0.7 : 0.45;
                ctx.fillStyle = edgeStroke;
                ctx.fill();
                ctx.globalAlpha = 1;
            }
        }
        ctx.setLineDash([]);

        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            var colorObj = PA.getSchemaColor(n.schema);
            var isSelected = PA.callGraphViz._selectedNode === n;
            var isHovered = PA.callGraphViz._hoveredNode === n;
            var typeLabel = PA.callGraphViz._getTypeLabel(n.objectType);
            var typeColor = PA.callGraphViz._getTypeColor(typeLabel);
            var depthColor = PA.callGraphViz._getDepthColor(n.depth);
            var isRoot = PA.callGraphViz._isRoot(n);

            if (isRoot) {
                ctx.save();
                ctx.shadowColor = 'rgba(245,158,11,0.6)';
                ctx.shadowBlur = 18;
                ctx.beginPath();
                ctx.arc(n.x, n.y, n.radius + 4, 0, Math.PI * 2);
                ctx.fillStyle = 'rgba(245,158,11,0.12)';
                ctx.fill();
                ctx.restore();
            } else if (isHovered || isSelected) {
                ctx.save();
                ctx.shadowColor = depthColor.glow;
                ctx.shadowBlur = 10;
                ctx.beginPath();
                ctx.arc(n.x, n.y, n.radius + 2, 0, Math.PI * 2);
                ctx.fillStyle = depthColor.glow;
                ctx.fill();
                ctx.restore();
            }

            ctx.beginPath();
            ctx.arc(n.x, n.y, n.radius, 0, Math.PI * 2);
            ctx.fillStyle = isSelected ? typeColor.fg : typeColor.bg;
            ctx.fill();
            ctx.strokeStyle = isRoot ? '#f59e0b' : depthColor.ring;
            ctx.lineWidth = isRoot ? 3 : (isHovered ? 2.5 : 1.5);
            ctx.stroke();

            var iconR = Math.max(7, n.radius * 0.55);
            ctx.fillStyle = isSelected ? '#fff' : typeColor.fg;
            ctx.font = 'bold ' + Math.max(7, iconR * 1.1) + 'px sans-serif';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillText(typeLabel, n.x, n.y);

            if (t.scale > 0.4 || isHovered || isSelected) {
                ctx.textBaseline = 'alphabetic';
                ctx.fillStyle = isSelected ? '#fff' : colorObj.fg;
                ctx.font = (isHovered || isSelected ? 'bold ' : '') + '10px sans-serif';
                ctx.textAlign = 'center';
                ctx.fillText(n.label, n.x, n.y + n.radius + 12);

                var lvlLabel = isRoot ? 'ROOT' : 'L' + n.depth;
                var lvlW = ctx.measureText(lvlLabel).width + 6;
                var lvlX = n.x - lvlW / 2;
                var lvlY = n.y - n.radius - 8;
                ctx.fillStyle = depthColor.ring;
                ctx.globalAlpha = 0.85;
                PA.callGraphViz._roundRect(ctx, lvlX, lvlY - 8, lvlW, 11, 3);
                ctx.fill();
                ctx.globalAlpha = 1;
                ctx.fillStyle = '#fff';
                ctx.font = 'bold 7px sans-serif';
                ctx.textBaseline = 'middle';
                ctx.fillText(lvlLabel, n.x, lvlY - 2.5);
            }
        }

        ctx.restore();
    },

    _screenToWorld: function(sx, sy) {
        var t = PA.callGraphViz._transform;
        return { x: (sx - t.x) / t.scale, y: (sy - t.y) / t.scale };
    },

    _findNodeAt: function(wx, wy) {
        var nodes = PA.callGraphViz._nodes;
        for (var i = nodes.length - 1; i >= 0; i--) {
            var dx = wx - nodes[i].x;
            var dy = wy - nodes[i].y;
            if (dx * dx + dy * dy <= nodes[i].radius * nodes[i].radius) return nodes[i];
        }
        return null;
    },

    _onMouseDown: function(e) {
        var rect = PA.callGraphViz._canvas.getBoundingClientRect();
        var w = PA.callGraphViz._screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
        var node = PA.callGraphViz._findNodeAt(w.x, w.y);
        if (node) {
            PA.callGraphViz._drag = { node: node, ox: node.x - w.x, oy: node.y - w.y, startX: e.clientX, startY: e.clientY, moved: false };
            PA.callGraphViz._selectedNode = node;
        } else {
            PA.callGraphViz._drag = { pan: true, sx: e.clientX, sy: e.clientY, tx: PA.callGraphViz._transform.x, ty: PA.callGraphViz._transform.y };
        }
        PA.callGraphViz._draw();
    },

    _onMouseMove: function(e) {
        var rect = PA.callGraphViz._canvas.getBoundingClientRect();
        var d = PA.callGraphViz._drag;
        if (d && d.node) {
            var dx = e.clientX - d.startX;
            var dy = e.clientY - d.startY;
            if (Math.abs(dx) > 3 || Math.abs(dy) > 3) d.moved = true;
            var w = PA.callGraphViz._screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
            d.node.x = w.x + d.ox;
            d.node.y = w.y + d.oy;
            PA.callGraphViz._draw();
        } else if (d && d.pan) {
            PA.callGraphViz._transform.x = d.tx + (e.clientX - d.sx);
            PA.callGraphViz._transform.y = d.ty + (e.clientY - d.sy);
            PA.callGraphViz._draw();
        } else {
            var w = PA.callGraphViz._screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
            var node = PA.callGraphViz._findNodeAt(w.x, w.y);
            if (node !== PA.callGraphViz._hoveredNode) {
                PA.callGraphViz._hoveredNode = node;
                PA.callGraphViz._canvas.style.cursor = node ? 'pointer' : 'grab';
                PA.callGraphViz._updateTooltip(node, e.clientX, e.clientY);
                PA.callGraphViz._draw();
            }
        }
    },

    _onMouseUp: function() {
        var d = PA.callGraphViz._drag;
        if (d && d.node && !d.moved) {
            if (PA.callGraphViz._expandedNode === d.node.id) {
                PA.callGraphViz.collapseAll();
            } else {
                PA.callGraphViz.expandNode(d.node.id);
            }
        }
        PA.callGraphViz._drag = null;
    },

    _onWheel: function(e) {
        e.preventDefault();
        var factor = e.deltaY > 0 ? 0.9 : 1.1;
        var t = PA.callGraphViz._transform;
        var rect = PA.callGraphViz._canvas.getBoundingClientRect();
        var mx = e.clientX - rect.left;
        var my = e.clientY - rect.top;
        t.x = mx - (mx - t.x) * factor;
        t.y = my - (my - t.y) * factor;
        t.scale *= factor;
        t.scale = Math.max(0.1, Math.min(5, t.scale));
        PA.callGraphViz._draw();
    },

    _onDblClick: function(e) {
        var rect = PA.callGraphViz._canvas.getBoundingClientRect();
        var w = PA.callGraphViz._screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
        var node = PA.callGraphViz._findNodeAt(w.x, w.y);
        if (node) PA.showProcedure(node.id);
    },

    _updateTooltip: function(node, cx, cy) {
        var tip = document.getElementById('graphTooltip');
        if (!tip) return;
        if (!node) { tip.style.display = 'none'; return; }

        tip.style.display = '';
        tip.style.left = (cx + 12) + 'px';
        tip.style.top = (cy - 10) + 'px';
        var depthC = PA.callGraphViz._getDepthColor(node.depth);
        var isRt = PA.callGraphViz._isRoot(node);
        var lvl = isRt ? 'ROOT' : 'L' + node.depth;
        tip.innerHTML = '<div style="display:flex;align-items:center;gap:6px;margin-bottom:3px">' +
            '<span style="background:' + depthC.ring + ';color:#fff;padding:1px 6px;border-radius:3px;font-size:9px;font-weight:700">' + lvl + '</span>' +
            '<strong>' + PA.esc(node.label) + '</strong></div>' +
            (node.schema ? '<span style="color:' + PA.getSchemaColor(node.schema).fg + '">Schema: ' + PA.esc(node.schema) + '</span><br>' : '') +
            (node.objectType ? 'Type: ' + PA.esc(node.objectType) + '<br>' : '') +
            'Depth: ' + node.depth +
            (node.loc > 0 ? ' | LOC: ' + node.loc : '') +
            '<br>In: ' + node.inEdges + ' | Out: ' + node.outEdges;
    },

    _getTypeLabel: function(objectType) {
        var t = (objectType || '').toUpperCase();
        if (t === 'FUNCTION') return 'F';
        if (t === 'TRIGGER') return 'T';
        if (t.indexOf('PACKAGE') !== -1) return 'PKG';
        return 'P';
    },

    _getTypeColor: function(typeLabel) {
        var map = {
            'P':   { bg: '#dbeafe', fg: '#2563eb' },
            'F':   { bg: '#dcfce7', fg: '#15803d' },
            'T':   { bg: '#fef3c7', fg: '#a16207' },
            'PKG': { bg: '#f3e8ff', fg: '#7e22ce' }
        };
        return map[typeLabel] || map['P'];
    },

    _getDepthColor: function(depth) {
        var palette = [
            { ring: '#f59e0b', glow: 'rgba(245,158,11,0.35)' },
            { ring: '#6366f1', glow: 'rgba(99,102,241,0.25)' },
            { ring: '#14b8a6', glow: 'rgba(20,184,166,0.2)' },
            { ring: '#f43f5e', glow: 'rgba(244,63,94,0.2)' },
            { ring: '#8b5cf6', glow: 'rgba(139,92,246,0.2)' },
            { ring: '#06b6d4', glow: 'rgba(6,182,212,0.15)' },
            { ring: '#84cc16', glow: 'rgba(132,204,22,0.15)' },
            { ring: '#f97316', glow: 'rgba(249,115,22,0.15)' }
        ];
        return palette[Math.min(depth, palette.length - 1)];
    },

    _isRoot: function(node) {
        return node.depth === 0 && node.inEdges === 0;
    },

    _renderControls: function() {
        var ctrl = document.getElementById('graphControls');
        if (!ctrl) return;
        var html = '';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.zoomIn()" data-tip="Zoom in">+</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.zoomOut()" data-tip="Zoom out">&minus;</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.fitAll()" data-tip="Fit graph to screen">Fit</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.resetLayout()" data-tip="Re-layout nodes">Reset</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.showAll()" data-tip="Show all nodes">All</button>';
        html += '<button class="btn btn-sm" onclick="PA.callGraphViz.collapseAll()" data-tip="Collapse to roots only">Collapse</button>';
        html += '<button class="btn btn-sm" id="cgFullscreenBtn" onclick="PA.callGraphFullscreen.toggle()" data-tip="Toggle fullscreen">&#x26F6;</button>';
        html += '<span class="cg-legend">';
        html += '<span class="cg-legend-item" style="color:#f59e0b" data-tip="Root entry point"><span class="cg-legend-dot" style="background:#fef3c7;border-color:#f59e0b;box-shadow:0 0 6px rgba(245,158,11,0.4)">&#9733;</span></span>';
        html += '<span class="cg-legend-item" style="color:#2563eb" data-tip="Procedure node"><span class="cg-legend-dot" style="background:#dbeafe;border-color:#2563eb">P</span></span>';
        html += '<span class="cg-legend-item" style="color:#15803d" data-tip="Function node"><span class="cg-legend-dot" style="background:#dcfce7;border-color:#15803d">F</span></span>';
        html += '<span class="cg-legend-item" style="color:#a16207" data-tip="Trigger node"><span class="cg-legend-dot" style="background:#fef3c7;border-color:#a16207">T</span></span>';
        html += '<span class="cg-legend-item" style="color:#7e22ce" data-tip="Package node"><span class="cg-legend-dot" style="background:#f3e8ff;border-color:#7e22ce">PKG</span></span>';
        html += '</span>';
        html += '<span class="cg-depth-legend">';
        var depthLabels = ['L0', 'L1', 'L2', 'L3', 'L4+'];
        var depthPalette = ['#f59e0b', '#6366f1', '#14b8a6', '#f43f5e', '#8b5cf6'];
        for (var dl = 0; dl < depthLabels.length; dl++) {
            html += '<span class="cg-depth-tag" style="background:' + depthPalette[dl] + '" data-tip="Depth level ' + dl + '">' + depthLabels[dl] + '</span>';
        }
        html += '</span>';
        html += '<span style="font-size:10px;color:var(--text-muted);margin-left:8px">' + PA.callGraphViz._nodes.length + ' nodes, ' + PA.callGraphViz._edges.length + ' edges</span>';
        ctrl.innerHTML = html;
        PA.callGraphFullscreen.init();
    },

    zoomIn: function() {
        if (!PA.callGraphViz._canvas) return;
        var t = PA.callGraphViz._transform;
        var cx = PA.callGraphViz._canvas.width / 2;
        var cy = PA.callGraphViz._canvas.height / 2;
        t.x = cx - (cx - t.x) * 1.2;
        t.y = cy - (cy - t.y) * 1.2;
        t.scale *= 1.2;
        PA.callGraphViz._draw();
    },

    zoomOut: function() {
        if (!PA.callGraphViz._canvas) return;
        var t = PA.callGraphViz._transform;
        var cx = PA.callGraphViz._canvas.width / 2;
        var cy = PA.callGraphViz._canvas.height / 2;
        t.x = cx - (cx - t.x) * 0.8;
        t.y = cy - (cy - t.y) * 0.8;
        t.scale *= 0.8;
        PA.callGraphViz._draw();
    },

    fitAll: function() {
        if (!PA.callGraphViz._canvas) return;
        var nodes = PA.callGraphViz._nodes;
        if (!nodes.length) return;
        var minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
        for (var i = 0; i < nodes.length; i++) {
            if (nodes[i].x < minX) minX = nodes[i].x;
            if (nodes[i].x > maxX) maxX = nodes[i].x;
            if (nodes[i].y < minY) minY = nodes[i].y;
            if (nodes[i].y > maxY) maxY = nodes[i].y;
        }
        var w = PA.callGraphViz._canvas.width;
        var h = PA.callGraphViz._canvas.height;
        var padding = 60;
        var scaleX = (w - padding * 2) / (maxX - minX || 1);
        var scaleY = (h - padding * 2) / (maxY - minY || 1);
        var scale = Math.min(scaleX, scaleY, 2);
        var cx = (minX + maxX) / 2;
        var cy = (minY + maxY) / 2;
        PA.callGraphViz._transform = { x: w / 2 - cx * scale, y: h / 2 - cy * scale, scale: scale };
        PA.callGraphViz._draw();
    },

    _roundRect: function(ctx, x, y, w, h, r) {
        ctx.beginPath();
        ctx.moveTo(x + r, y);
        ctx.lineTo(x + w - r, y);
        ctx.quadraticCurveTo(x + w, y, x + w, y + r);
        ctx.lineTo(x + w, y + h - r);
        ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
        ctx.lineTo(x + r, y + h);
        ctx.quadraticCurveTo(x, y + h, x, y + h - r);
        ctx.lineTo(x, y + r);
        ctx.quadraticCurveTo(x, y, x + r, y);
        ctx.closePath();
    },

    resetLayout: function() {
        if (!PA.callGraphViz._canvas) return;
        var nodes = PA.callGraphViz._nodes;
        for (var i = 0; i < nodes.length; i++) {
            nodes[i].x = Math.random() * 600 - 300;
            nodes[i].y = Math.random() * 400 - 200;
            nodes[i].vx = 0; nodes[i].vy = 0;
        }
        if (PA.callGraphViz._animFrame) cancelAnimationFrame(PA.callGraphViz._animFrame);
        PA.callGraphViz._simulate();
    }
};
