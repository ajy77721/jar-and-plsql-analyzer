window.HelpSystem = (function(){
    var _mode = 'biz';
    var _page = '';

    function init(page){
        _page = page || 'home';
        var btn = document.createElement('button');
        btn.className = 'help-btn';
        btn.title = 'Help & Documentation';
        btn.innerHTML = '?';
        btn.onclick = function(){ open(); };
        document.body.appendChild(btn);

        var ov = document.createElement('div');
        ov.className = 'help-overlay';
        ov.id = 'helpOverlay';
        ov.onclick = function(e){ if(e.target===ov) close(); };
        ov.innerHTML =
            '<div class="help-modal">' +
                '<div class="help-hdr">' +
                    '<span style="font-size:20px">&#128218;</span>' +
                    '<h2>Help &amp; Documentation</h2>' +
                    '<div class="help-mode-tabs">' +
                        '<button class="help-mode-tab active" data-mode="biz" onclick="HelpSystem.setMode(\'biz\')">Business Guide</button>' +
                        '<button class="help-mode-tab" data-mode="tech" onclick="HelpSystem.setMode(\'tech\')">Technical Guide</button>' +
                    '</div>' +
                    '<button class="help-close" onclick="HelpSystem.close()">&times;</button>' +
                '</div>' +
                '<div class="help-body" id="helpBody"></div>' +
            '</div>';
        document.body.appendChild(ov);
    }

    function open(){
        document.getElementById('helpOverlay').classList.add('open');
        render();
    }
    function close(){
        document.getElementById('helpOverlay').classList.remove('open');
    }
    function setMode(m){
        _mode = m;
        document.querySelectorAll('.help-mode-tab').forEach(function(t){
            t.classList.toggle('active', t.dataset.mode === m);
        });
        var body = document.getElementById('helpBody');
        body.dataset.mode = m;
        render();
    }
    function render(){
        var body = document.getElementById('helpBody');
        body.dataset.mode = _mode;
        if(_mode === 'biz') body.innerHTML = getBizContent();
        else body.innerHTML = getTechContent();
    }

    // ═══════════════════════════════════════
    //  BUSINESS GUIDE
    // ═══════════════════════════════════════
    function getBizContent(){
        var nav = '<div class="help-nav">' +
            '<a href="/jar/">JAR Analyzer</a>' +
            '<a href="/parser/">PL/SQL Parser</a>' +
            '<a href="/">Home Dashboard</a>' +
            '</div>';
        if(_page === 'jar') return nav + getJarBiz();
        if(_page === 'parser') return nav + getParserBiz();
        return nav + getHomeBiz();
    }

    function getHomeBiz(){
        return '' +
        '<h3>Welcome to Code Analyzer</h3>' +
        '<p>This platform provides deep static analysis for <strong>Java Spring Boot JARs</strong> and <strong>Oracle PL/SQL procedures</strong>. ' +
        'It helps development, QA, and architecture teams understand codebases without reading every line of code.</p>' +

        '<h3>Home Dashboard — What You See</h3>' +
        '<p>The home screen is your central hub. It shows two tool cards at the top, followed by four functional panels:</p>' +

        '<div class="help-card"><div class="help-card-title">1. Quick Submit Panel</div>' +
        '<p>Submit analysis jobs without navigating away from the home screen.</p>' +
        '<ul>' +
        '<li><strong>PL/SQL Parser tab</strong> — Enter one procedure name per line (e.g. <code>PG_RFUR_MGMNT.fn_generate_policy_endorsement</code>). ' +
        'Supports formats: <code>PKG.PROC</code>, <code>SCHEMA.PKG.PROC</code>, or standalone <code>PROCEDURE_NAME</code>. Click "Submit to Queue" and each line becomes a separate queue job. Duplicates are automatically archived with a timestamp.</li>' +
        '<li><strong>JAR Analyzer tab</strong> — Enter a local file path to a JAR (e.g. <code>C:\\path\\to\\app-0.0.1-SNAPSHOT.jar</code>). Choose Static or Claude mode. The server reads the file directly from the local filesystem.</li>' +
        '</ul></div>' +

        '<div class="help-card"><div class="help-card-title">2. Analysis Queue</div>' +
        '<p>Click to expand the queue modal. All analysis jobs (JAR uploads, PL/SQL analyses, Claude enrichments) run through a <strong>single unified queue</strong> — one job at a time, in order.</p>' +
        '<ul>' +
        '<li><strong>Running job</strong> — Shows name, type badge, elapsed time, progress bar (%), cancel button</li>' +
        '<li><strong>Queued jobs</strong> — Numbered list with move up/down buttons to reorder, cancel button per job. Click "Apply Order" to save new ordering</li>' +
        '<li><strong>History</strong> — Last 20 completed/failed/cancelled jobs with elapsed times</li>' +
        '</ul>' +
        '<p>The queue bar auto-refreshes every 3 seconds. Job types shown: JAR, PL/SQL, Parser, Claude Enrich, Claude Correct, Claude Full Scan.</p></div>' +

        '<div class="help-card"><div class="help-card-title">3. Progress Report</div>' +
        '<p>Side-by-side progress for both tools:</p>' +
        '<ul>' +
        '<li><strong>JAR Analyzer</strong> — Total JARs, endpoints/classes count, AI enriched count, running/pending/not-enriched breakdown, percentage bar</li>' +
        '<li><strong>PL/SQL Parser</strong> — Total analyses, procedures/tables count, lines of code, AI verified count, running/failed sessions, percentage bar</li>' +
        '</ul></div>' +

        '<div class="help-card"><div class="help-card-title">4. Claude Sessions</div>' +
        '<p>Shows all AI sessions across both tools in a table with columns:</p>' +
        '<table>' +
        '<tr><th>Column</th><th>Description</th></tr>' +
        '<tr><td><strong>Analyzer</strong></td><td>JAR or Parser (color-coded)</td></tr>' +
        '<tr><td><strong>ID</strong></td><td>Session identifier (monospace)</td></tr>' +
        '<tr><td><strong>Status</strong></td><td>RUNNING (blue), COMPLETE (green), FAILED (red), KILLED (orange)</td></tr>' +
        '<tr><td><strong>Type</strong></td><td>Type of AI analysis being performed</td></tr>' +
        '<tr><td><strong>Detail</strong></td><td>Target endpoint or analysis name</td></tr>' +
        '<tr><td><strong>Duration</strong></td><td>Elapsed time formatted (e.g. 2m 35s)</td></tr>' +
        '</table>' +
        '<p>Auto-refreshes every 30 seconds.</p></div>' +

        '<h3>Getting Started</h3>' +
        '<ul>' +
        '<li><strong>For Java</strong>: Click the "JAR / Java Analyzer" card to go to the JAR screen, or use Quick Submit with a local path</li>' +
        '<li><strong>For PL/SQL</strong>: Click the "PL/SQL Parse Analyzer" card, or type procedure names in Quick Submit</li>' +
        '<li>All analyses go through the unified queue — submit multiple and they run sequentially</li>' +
        '<li>Results persist on disk — revisit any completed analysis at any time</li>' +
        '<li>The floating chat button (bottom-right) lets you ask AI questions about any analysis</li>' +
        '</ul>';
    }

    // ──────────────────────────────────────
    //  JAR ANALYZER — BUSINESS
    // ──────────────────────────────────────
    function getJarBiz(){
        return '' +
        '<h3>JAR Analyzer — Complete Guide</h3>' +
        '<p>The JAR Analyzer decompiles Java Spring Boot applications and maps every REST endpoint, class hierarchy, call chain, ' +
        'database operation, aggregation pipeline, and complexity metric. Optionally, Claude AI enrichment adds business context and validates findings.</p>' +

        '<h3>How to Upload &amp; Analyze</h3>' +
        '<ul>' +
        '<li><strong>Upload button</strong> (sidebar) — Browse for a .jar file (up to 2GB). Choose analysis mode: <strong>Static</strong> (fast bytecode-only) or <strong>Claude</strong> (static + AI enrichment)</li>' +
        '<li><strong>Local path</strong> (home Quick Submit) — Enter a file path; the server reads it directly</li>' +
        '<li>After upload, the job enters the queue. A progress log panel shows real-time steps: Uploading → Queued → Processing → Analyzing classes → Building call graph → Complete</li>' +
        '<li>Completed JARs appear in the left sidebar with stats (class count, endpoint count, date). Click any JAR to load it.</li>' +
        '</ul>' +

        '<h3>Sidebar — JAR List</h3>' +
        '<p>The left sidebar lists all analyzed JARs. Each entry shows:</p>' +
        '<ul>' +
        '<li>Project/JAR name (clickable to load)</li>' +
        '<li>Class count + endpoint count</li>' +
        '<li>AI badge: IDLE / RUNNING / COMPLETE / FAILED (Claude enrichment status)</li>' +
        '<li>"C" or "C(2)" badge if Claude-corrected (with iteration number)</li>' +
        '<li>Relative date ("2h ago", "3 days ago")</li>' +
        '<li>Delete button (×) with confirmation</li>' +
        '</ul>' +

        '<h3>Top Bar Actions</h3>' +
        '<ul>' +
        '<li><strong>Sessions</strong> — Opens overlay showing all Claude sessions for this JAR</li>' +
        '<li><strong>Logs</strong> — Opens application log viewer panel</li>' +
        '<li><strong>Mode Toggle</strong> — Switch between Static / Corrected / Previous versions of the analysis</li>' +
        '<li><strong>Claude Button</strong> — "Enable Claude" (first run), "Re-scan Claude" (subsequent), hidden while running (shows progress bar instead)</li>' +
        '</ul>' +

        '<h3>Tab 1: Code Structure</h3>' +
        '<p>Browse the full class hierarchy of the decompiled JAR. Three view modes:</p>' +

        '<div class="help-card"><div class="help-card-title">Package View (default)</div>' +
        '<p>Groups classes by Java package. Main app classes first, then "Internal Dependencies" section with dependency JAR modules. ' +
        'Each module shows project name, domain badge (if configured), and class count. Expand packages to see classes, expand classes to see fields and methods.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Project View (IntelliJ-style)</div>' +
        '<p>Folder hierarchy: src/main/java → com → example → domain → ClassName.java. ' +
        'Single-child paths collapsed (com.example.app becomes one node). Each class shows stereotype badge (@Service, @Repository, etc.), type icon, and member count.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Visual Overview</div>' +
        '<p>Card grid grouped by stereotype: @RestController, @Service, @Repository, @Component, etc. Each card shows class name, stereotype, and member count. ' +
        'Quick visual scan of the application architecture.</p></div>' +

        '<p><strong>Actions:</strong> Search box (filters classes, methods, annotations), Expand All / Collapse All, Back/Forward/History navigation, ' +
        'JAR Source Filter bar (show/hide classes by source module). Click any class or method to see decompiled Java source in the right panel.</p>' +

        '<h3>Tab 2: Endpoint Flows</h3>' +
        '<p>All REST endpoints grouped by controller class.</p>' +
        '<ul>' +
        '<li><strong>Left pane</strong> — Endpoint list with filter search. Each shows: HTTP method badge (GET/POST/PUT/DELETE, color-coded), full path, method signature, return type</li>' +
        '<li><strong>Right pane</strong> — Click any endpoint to see its call chain visualization:</li>' +
        '</ul>' +
        '<table>' +
        '<tr><th>Section</th><th>What It Shows</th></tr>' +
        '<tr><td><strong>Header</strong></td><td>HTTP method + full path + method signature</td></tr>' +
        '<tr><td><strong>Call Chain</strong></td><td>Visual flow: Controller → Service → Repository with stereotype chips and arrows</td></tr>' +
        '<tr><td><strong>Collections</strong></td><td>MongoDB collections accessed, tagged as DATA (💾) or VIEW (👁️)</td></tr>' +
        '<tr><td><strong>Operation Flow</strong></td><td>Numbered step list showing: step #, [STEREOTYPE], ClassName.methodName (clickable), description, params, return type, dispatch badge (DYNAMIC/QUALIFIED/HEURISTIC/IFACE ONLY/@PRIMARY)</td></tr>' +
        '</table>' +
        '<p>Breadcrumb navigation with Back/Next/Root buttons for drilling into sub-calls.</p>' +

        '<h3>Tab 3: Summary — 11 Sub-Tabs</h3>' +
        '<p>The Summary tab is the most feature-rich screen, containing 11 specialized sub-tabs for migration analysis:</p>' +

        '<div class="help-card"><div class="help-card-title">3.1 Endpoint Report (default)</div>' +
        '<p>Card-based list of all REST endpoints. Each card shows: HTTP method badge, full path, method signature, quick stats ' +
        '(Collections, Views, DB Operations, Internal/External calls, LOC, Size S/M/L/XL), domain badge, performance badge.</p>' +
        '<p><strong>Sort by:</strong> Path, Domain, Collections, DB Ops, External calls, LOC, Size</p>' +
        '<p><strong>Filters:</strong> Domain pills, Advanced Filters panel with range sliders (Collections, Views, DB Ops, Internal, External, Methods, LOC) ' +
        'and category pills (HTTP Method, Size, Performance, Operations: READ/WRITE/UPDATE/DELETE/AGGREGATE)</p>' +
        '<p><strong>Pagination:</strong> 25 cards per page</p></div>' +

        '<div class="help-card"><div class="help-card-title">3.2 Collection Analysis</div>' +
        '<p>Table of all MongoDB collections with ownership and operations.</p>' +
        '<table>' +
        '<tr><th>Column</th><th>Description</th></tr>' +
        '<tr><td>Collection Name</td><td>With DATA/VIEW type badge</td></tr>' +
        '<tr><td>Domain</td><td>Owning business domain</td></tr>' +
        '<tr><td>Type</td><td>DATA or VIEW</td></tr>' +
        '<tr><td>Read / Write / Agg Ops</td><td>Operation counts</td></tr>' +
        '<tr><td>Detected Via</td><td>bytecode, annotation, or dynamic</td></tr>' +
        '<tr><td>Complexity</td><td>Low/Medium/High with score</td></tr>' +
        '<tr><td>Verification</td><td>IN_DB / NOT_IN_DB / NEED_REVIEW</td></tr>' +
        '</table>' +
        '<p>Toggle between Hierarchical View (grouped by domain) and flat Table View.</p></div>' +

        '<div class="help-card"><div class="help-card-title">3.3 External Dependencies</div>' +
        '<p>Cross-module JAR dependencies — which external libraries each endpoint calls.</p>' +
        '<p><strong>Columns:</strong> Module Name, Domain, Call Count, Called By (expandable endpoint list), Type (Library/Internal/External)</p>' +
        '<p>Expand any module to see all methods called within it with breadcrumb path from the endpoint controller.</p></div>' +

        '<div class="help-card"><div class="help-card-title">3.4 Distributed Transactions</div>' +
        '<p>Endpoints writing to multiple collections across different domains.</p>' +
        '<p><strong>Columns:</strong> Endpoint Name (clickable), Domain, Transaction Requirement badge (REQUIRED / ADVISORY / NONE)</p>' +
        '<p>REQUIRED = writes to 3+ collections across 2+ domains, needs @Transactional or compensation logic.</p></div>' +

        '<div class="help-card"><div class="help-card-title">3.5 Batch Jobs</div>' +
        '<p>Endpoints classified as batch/scheduler jobs (detected by naming patterns: job, batch, process).</p>' +
        '<p><strong>Columns:</strong> Batch Name, Primary Domain, Touches Domains (badge pills), Collections count, Methods count, Size (S/M/L/XL)</p></div>' +

        '<div class="help-card"><div class="help-card-title">3.6 Scheduled Jobs</div>' +
        '<p>@Scheduled annotated methods with execution patterns.</p>' +
        '<p><strong>Columns:</strong> Method Name, Class, Schedule Expression (cron/fixedRate/fixedDelay), Domain, Collections count, Methods count</p>' +
        '<p>Grouped by execution pattern (Cron / FixedRate / FixedDelay).</p></div>' +

        '<div class="help-card"><div class="help-card-title">3.7 Aggregation Flows</div>' +
        '<p>MongoDB aggregation pipelines detected in code.</p>' +
        '<p><strong>Columns:</strong> Pipeline ID, Collections (including $lookup joins), Pipeline Stages (count + types: $match, $group, $project, $sort, $lookup), ' +
        'Called From, Complexity, $Lookup Joins count, Dynamic (Yes/No)</p>' +
        '<p>Expand any row to see full pipeline structure with cross-domain $lookup joins highlighted in red.</p>' +
        '<p><strong>Filters:</strong> Collection, Pipeline stage type, Complexity level</p></div>' +

        '<div class="help-card"><div class="help-card-title">3.8 Dynamic Flows</div>' +
        '<p>Non-direct method dispatch — interface dispatch, reflection, dynamic queries. These are blind spots where runtime behavior may differ from static analysis.</p>' +
        '<p><strong>Columns:</strong> Dispatch Type (INTERFACE_DISPATCH/REFLECTION/DYNAMIC_QUERY/STRATEGY_PATTERN), Target Class, ' +
        'Implementations Found, Confidence (Low/Med/High), Called From, Collections Touched, Risk Level</p></div>' +

        '<div class="help-card"><div class="help-card-title">3.9 Verticalisation</div>' +
        '<p>Domain boundary violation analysis for microservice migration.</p>' +
        '<p><strong>Section A — Bean Crossing:</strong> Direct method calls across domain boundaries. Columns: Target Bean, Stereotype, Source Module, Source Domain, ' +
        'Call Count, Caller Endpoints, Recommendation (REST_API = must wrap in REST call)</p>' +
        '<p><strong>Section B — Data Crossing:</strong> Collections accessed across domain boundaries. Columns: Collection, Type, Owner Domain, ' +
        'Accessed By Domains, Access Type (READ/WRITE/AGGREGATE), Endpoints, Recommendation (REST_API or SHARED_DATA)</p></div>' +

        '<div class="help-card"><div class="help-card-title">3.10 Claude Insights</div>' +
        '<p>AI-powered analysis per endpoint (available after running Claude enrichment).</p>' +
        '<p><strong>Columns:</strong> Endpoint Name, Business Process (inferred), Risk Flags, Migration Difficulty (1-5), Key Insights, Recommendations</p>' +
        '<p>Expand for full Claude analysis text, suggested vertical assignment, and risk mitigation steps.</p></div>' +

        '<div class="help-card"><div class="help-card-title">3.11 Claude Corrections</div>' +
        '<p>Side-by-side comparison of static vs. Claude-corrected analysis.</p>' +
        '<p><strong>Columns:</strong> Endpoint Name, Added Count (collections Claude found but static missed), Removed Count (false positives), ' +
        'Verified Count (both agree), Verified %</p>' +
        '<p>Expand to see specific ADDED, REMOVED, and VERIFIED collections with confidence scores.</p></div>' +

        '<h3>Export</h3>' +
        '<p>Click the Export button in the Summary tab to download analysis results as JSON or Excel. Choose which sections to include: ' +
        'Endpoints, Collections, External Dependencies, Batch Jobs, Scheduled Jobs, Aggregation Flows.</p>';
    }

    // ──────────────────────────────────────
    //  PL/SQL PARSER — BUSINESS
    // ──────────────────────────────────────
    function getParserBiz(){
        return '' +
        '<h3>PL/SQL Parse Analyzer — Complete Guide</h3>' +
        '<p>The parser connects to an Oracle database, downloads PL/SQL source code, parses it with a full grammar, and builds ' +
        'a complete dependency tree showing which procedures call which, what tables are accessed (with operations), joins, cursors, sequences, and triggers.</p>' +

        '<h3>Home Screen — Starting an Analysis</h3>' +
        '<div class="help-card"><div class="help-card-title">Step-by-Step</div>' +
        '<ul>' +
        '<li><strong>Step 1 — Connection:</strong> Select a Project and Environment from the dropdowns. These map to Oracle database connections configured in <code>plsql-config.yaml</code>.</li>' +
        '<li><strong>Step 2 — Owner:</strong> Optionally select a specific DB schema owner, or leave as "Auto-detect (all schemas)" to let the system resolve it.</li>' +
        '<li><strong>Step 3 — Target:</strong> Enter the procedure/package name (e.g. <code>PKG_CUSTOMER</code> or <code>PKG.PROC_NAME</code>). Select the object type (PACKAGE, PROCEDURE, FUNCTION, TRIGGER). Click <strong>Analyze</strong>.</li>' +
        '</ul>' +
        '<p>A progress bar shows: Starting → Resolving dependencies → Downloading source → Parsing → Building output → Complete.</p></div>' +

        '<p><strong>Manage Connections</strong> — Click the gear button to open the connection manager. Add/edit/delete projects and their environments (JDBC URL, username, password). Test connections before saving.</p>' +
        '<p><strong>Analysis History</strong> — Lists all past analyses with name, date, procedure count, table count, LOC. Click to reload. Search box to filter. ' +
        'Performance Summary tab shows timing breakdowns for past runs.</p>' +

        '<h3>Analysis Screen Layout</h3>' +
        '<p>After analysis completes, the screen splits into:</p>' +
        '<ul>' +
        '<li><strong>Top Bar</strong> — Analysis name, stats (procs, tables, edges, LOC, max depth, errors), Claude mode toggle, action buttons (Pull, Refresh, Search, Export, Logs)</li>' +
        '<li><strong>Left Panel</strong> — Two tabs: "Call Flow" (procedures sorted by depth) and "Tables" (all tables). Filter box at top. Lazy-loaded in batches of 50.</li>' +
        '<li><strong>Right Panel</strong> — 8 main tabs with the analysis detail views</li>' +
        '</ul>' +

        '<h3>Left Panel — Call Flow Tab</h3>' +
        '<p>Lists all procedures/functions in the call tree. Each row shows:</p>' +
        '<ul>' +
        '<li>Schema badge (color-coded), Type icon (P/F/T), Procedure name</li>' +
        '<li>Depth badge (distance from entry point), LOC badge</li>' +
        '<li>Click any procedure to load its details in the right panel</li>' +
        '</ul>' +

        '<h3>Left Panel — Tables Tab</h3>' +
        '<p>Lists all tables discovered across the analysis. Each row shows:</p>' +
        '<ul>' +
        '<li>Table name (color-coded), Operation badges (S=SELECT, I=INSERT, U=UPDATE, D=DELETE, M=MERGE, C=CREATE, T=TRUNCATE), Access count</li>' +
        '<li>Click any table to open the Table Detail Modal</li>' +
        '</ul>' +

        '<h3>Right Panel Tab 1: Explore</h3>' +
        '<p>Three sub-tabs for navigating the call tree:</p>' +

        '<div class="help-card"><div class="help-card-title">Hierarchy (default)</div>' +
        '<p>Interactive depth-indented call tree with expand/collapse. Each row shows: depth indicator (L0, L1...), step counter, ' +
        'schema badge, procedure name (clickable to source), source line link, call type badge (INTERNAL/EXTERNAL/TRIGGER/DYNAMIC), LOC, complexity risk (L/M/H).</p>' +
        '<p><strong>Actions:</strong> Expand All, Collapse All, breadcrumb navigation for drill-down, search (expands all then highlights matches).</p></div>' +

        '<div class="help-card"><div class="help-card-title">Trace</div>' +
        '<p>Flat list of all procedures in the tree with same columns as Hierarchy. Paginated (200 per batch with "Show more").</p></div>' +

        '<div class="help-card"><div class="help-card-title">References</div>' +
        '<p>Shows calls made by the selected procedure. Filter by call type (INT/EXT). ' +
        'Columns: Name, Call type, Count, Line numbers. Click to navigate.</p></div>' +

        '<h3>Right Panel Tab 2: Table Ops</h3>' +
        '<p>All tables accessed across the entire call tree with full filtering and sorting.</p>' +
        '<table>' +
        '<tr><th>Column</th><th>Description</th></tr>' +
        '<tr><td>Table Name</td><td>Clickable — opens Table Detail Modal</td></tr>' +
        '<tr><td>Type</td><td>TABLE / VIEW / MATERIALIZED VIEW</td></tr>' +
        '<tr><td>Operations</td><td>Badge row: SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE, TRUNCATE, DROP</td></tr>' +
        '<tr><td>Count</td><td>Total access count</td></tr>' +
        '<tr><td>Triggers</td><td>Count of triggers found (clickable)</td></tr>' +
        '<tr><td>Procedures</td><td>Number of procedures accessing this table</td></tr>' +
        '<tr><td>Usage</td><td>Usage summary text</td></tr>' +
        '</table>' +
        '<p><strong>Filters:</strong> Operation pills (toggle SELECT/INSERT/UPDATE/DELETE/MERGE/CREATE/TRUNCATE/DROP), free-text search, column-level dropdown filters per column. All columns sortable.</p>' +
        '<p><strong>Row expansion:</strong> Click any row to see: Access Details (operation badge, procedure name + clickable, line number + clickable to source, WHERE filter text), ' +
        'Tables via Triggers (operations inherited from trigger analysis), Claude verification badges (if available).</p>' +

        '<h3>Right Panel Tab 3: Details</h3>' +
        '<p>Two sub-tabs:</p>' +

        '<div class="help-card"><div class="help-card-title">Sequences</div>' +
        '<p><strong>Columns:</strong> Sequence Name, Schema (color-coded), Operations (NEXTVAL/CURRVAL badges), Usage Count, Procedure Count</p>' +
        '<p><strong>Filters:</strong> Operation pills (NEXTVAL/CURRVAL), column filters on Sequence, Schema. All sortable.</p>' +
        '<p><strong>Row expansion:</strong> Usage details with operation, procedure (clickable), line number (clickable to source).</p></div>' +

        '<div class="help-card"><div class="help-card-title">Join Summary</div>' +
        '<p>All table joins found in SQL statements across the analysis.</p>' +
        '<p><strong>Columns:</strong> Left Table, Right Table, Join Types (INNER/LEFT/RIGHT/CROSS/FULL with colored badges), Usage Count, ON Predicate (preview)</p>' +
        '<p><strong>Filters:</strong> Join type pills, free-text search, column filters. All sortable.</p>' +
        '<p><strong>Row expansion:</strong> Each join occurrence with type badge, procedure (clickable), line number (clickable to source), "Show SQL" button that fetches and highlights the SQL statement.</p></div>' +

        '<h3>Right Panel Tab 4: Summary</h3>' +
        '<p>Three sub-tabs:</p>' +
        '<ul>' +
        '<li><strong>Dashboard</strong> — Object Types breakdown chart (type name, bar, count, %), Schemas breakdown chart (schema badge, bar, count, %), ' +
        'Highlights section: Most LOC, Deepest, Most Tables, Most Calls (all clickable to navigate)</li>' +
        '<li><strong>Claude Insights</strong> — AI-generated insights about the analysis (after Claude enrichment)</li>' +
        '<li><strong>Claude Corrections</strong> — Side-by-side static vs Claude comparison (after Claude enrichment)</li>' +
        '</ul>' +

        '<h3>Right Panel Tab 5: Complexity</h3>' +
        '<p>Procedure complexity analysis with weighted risk scoring.</p>' +
        '<table>' +
        '<tr><th>Column</th><th>Description</th></tr>' +
        '<tr><td>Procedure</td><td>Name (lock icon if encrypted)</td></tr>' +
        '<tr><td>Schema</td><td>Owner schema badge</td></tr>' +
        '<tr><td>LOC</td><td>Lines of code</td></tr>' +
        '<tr><td>Tables</td><td>Number of tables accessed</td></tr>' +
        '<tr><td>Dependencies</td><td>Number of calls made</td></tr>' +
        '<tr><td>Dynamic SQL</td><td>Count (highlighted if &gt; 0)</td></tr>' +
        '<tr><td>Depth</td><td>Call tree depth</td></tr>' +
        '<tr><td>Score</td><td>Weighted composite score</td></tr>' +
        '<tr><td>Risk</td><td>L / M / H badge with color</td></tr>' +
        '</table>' +
        '<p><strong>Filters:</strong> Risk pills (Low/Medium/High), search, column filters on all columns. Risk summary cards at top show count and % for each level (clickable to filter).</p>' +
        '<p><strong>Row expansion:</strong> Score Breakdown grid showing each factor (LOC, Tables, Calls, Cursors, Dynamic SQL, Depth) with: value, weight, contribution, bar chart visualization.</p>' +

        '<h3>Right Panel Tab 6: Graph</h3>' +
        '<p>Visual call graph diagram showing procedure-to-procedure dependencies as a directed graph.</p>' +

        '<h3>Right Panel Tab 7: Source</h3>' +
        '<p>PL/SQL source code viewer with syntax highlighting. Click any procedure or line number link anywhere in the UI to open source here.</p>' +
        '<p><strong>Features:</strong> Search in source (with match count and prev/next navigation), line numbers (clickable), sidebar panels: ' +
        'Parameters, Variables, Tables, Cursors, Calls Made, Called By (each filterable and clickable for navigation).</p>' +

        '<h3>Table Detail Modal</h3>' +
        '<p>Click any table name anywhere in the UI to open a detailed modal. Shows table name + schema in the header, with access/join/trigger counts.</p>' +

        '<div class="help-card"><div class="help-card-title">Tab: Accesses</div>' +
        '<p>Every operation on this table with sortable columns: Operation (SELECT/INSERT/UPDATE/DELETE/MERGE), ' +
        'Procedure name (clickable), Line number (clickable to source). Filterable by operation pills. ' +
        'If Claude enrichment exists, shows verification column with CONFIRMED/REMOVED/Not verified badges and accept/reject buttons.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Tab: Columns</div>' +
        '<p>Database column definitions fetched live from Oracle. Columns: # (position), Column Name (PK icon if primary key), ' +
        'Data Type (formatted with precision), Nullable (YES / NOT NULL), Default value. Sortable and filterable.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Tab: Indexes</div>' +
        '<p>All indexes on the table grouped by index name. Columns: Index Name, Uniqueness (UNIQUE/NONUNIQUE), Column list. ' +
        'Sortable and filterable by uniqueness.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Tab: Constraints</div>' +
        '<p>Primary keys, foreign keys, unique constraints, check constraints. Columns: Constraint Name, Type ' +
        '(P=Primary / U=Unique / R=Foreign Key / C=Check, color-coded), Columns, References (for FK). Sortable and filterable by type.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Tab: Joins</div>' +
        '<p>All joins involving this table. Columns: Join Type (INNER/LEFT/RIGHT/CROSS/FULL, colored badge), Joined Table, Alias, ' +
        'Condition (preview), Procedure (clickable), Line (clickable to source). Sortable and filterable.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Tab: Triggers</div>' +
        '<p>Triggers on this table. Columns: Schema badge, Trigger Name (clickable, opens trigger definition), Timing (BEFORE/AFTER), ' +
        'Event (INSERT/UPDATE/DELETE), Type, Source (DB/SRC badge), View Source and Definition buttons. Sortable and filterable.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Tab: Claude</div>' +
        '<p>AI verification status. Side-by-side comparison: Operation, Procedure, Line, Static badge, Claude Status ' +
        '(CONFIRMED/REMOVED/Not verified), Reason, Review buttons. Summary: confirmed/removed/new/unverified counts with overall status.</p></div>' +

        '<h3>Scope Controls</h3>' +
        '<p>Many tabs have scope toggle buttons: <strong>Direct</strong> (only the selected procedure), <strong>Subtree</strong> (selected + all its children), ' +
        '<strong>SubFlow</strong> (selected + descendants), <strong>Full</strong> (entire analysis). The scope determines which data rows appear.</p>' +

        '<h3>Export</h3>' +
        '<p>Click Export in the top bar. Choose format (JSON/CSV) and sections to include: Procedures, Table Operations, Joins, Cursors, Call Graph, Summary Stats.</p>' +

        '<h3>Claude AI Enrichment</h3>' +
        '<p>Click "Enable Claude" in the top bar to start AI verification. Claude reads the static analysis + PL/SQL source code, ' +
        'then confirms or corrects table operations, adds business context, and identifies patterns. Progress shown in real-time with percentage. ' +
        'Use the mode toggle (Static / Claude) to switch between views. Use Chunks button to inspect individual verification chunks.</p>';
    }


    // ═══════════════════════════════════════
    //  TECHNICAL GUIDE
    // ═══════════════════════════════════════
    function getTechContent(){
        var nav = '<div class="help-nav">' +
            '<a href="/jar/">JAR Analyzer</a>' +
            '<a href="/parser/">PL/SQL Parser</a>' +
            '<a href="/">Home Dashboard</a>' +
            '</div>';
        if(_page === 'jar') return nav + getJarTech();
        if(_page === 'parser') return nav + getParserTech();
        return nav + getHomeTech();
    }

    // ──────────────────────────────────────
    //  HOME — TECHNICAL
    // ──────────────────────────────────────
    function getHomeTech(){
        return '' +
        '<h3>Architecture Overview</h3>' +
        '<p>The platform is a <strong>multi-module Maven project</strong> running as a single Spring Boot 3.2.5 application on port 8083.</p>' +
        '<table>' +
        '<tr><th>Module</th><th>Purpose</th><th>Key Tech</th></tr>' +
        '<tr><td><code>jar-analyzer-core</code></td><td>Java bytecode analysis engine — decompiles JARs, scans classes, builds call graphs</td><td>ASM 9.7, CFR 0.152</td></tr>' +
        '<tr><td><code>plsql-parser</code></td><td>PL/SQL grammar parsing &amp; flow analysis — BFS dependency crawl, ANTLR4 parsing, schema resolution</td><td>ANTLR4 4.13.1, Oracle JDBC</td></tr>' +
        '<tr><td><code>plsql-config</code></td><td>Shared configuration management — YAML/JSON config loading, DB connection pooling</td><td>Jackson, SnakeYAML</td></tr>' +
        '<tr><td><code>unified-web</code></td><td>Spring Boot web app — REST APIs, static UI, queue orchestration, Claude integration</td><td>Spring Boot 3.2.5</td></tr>' +
        '</table>' +

        '<h3>Unified Queue System</h3>' +
        '<p><code>AnalysisQueueService</code> is a singleton running a single-worker-thread executor. All analysis jobs run sequentially through this queue.</p>' +
        '<table>' +
        '<tr><th>Job Type</th><th>Executor</th><th>What It Does</th></tr>' +
        '<tr><td><code>JAR_UPLOAD</code></td><td>JarAnalysisExecutor</td><td>Parse JAR bytecode → classes JSON, extract config, build call-graph index, persist</td></tr>' +
        '<tr><td><code>PARSER_ANALYSIS</code></td><td>ParserAnalysisExecutor</td><td>Run FlowAnalysisMain → BFS dependency crawl → parse PL/SQL → persist chunked output</td></tr>' +
        '<tr><td><code>PLSQL_ANALYSIS</code></td><td>PlsqlAnalysisExecutor</td><td>Legacy PL/SQL analyzer (DB source fetch + analysis)</td></tr>' +
        '<tr><td><code>CLAUDE_ENRICH</code></td><td>ClaudeAnalysisService</td><td>Chunk endpoints, send to Claude CLI, parse responses, merge</td></tr>' +
        '<tr><td><code>CLAUDE_CORRECT</code></td><td>ClaudeAnalysisService</td><td>Generate corrections for endpoints, compare static vs AI</td></tr>' +
        '<tr><td><code>CLAUDE_FULL_SCAN</code></td><td>ClaudeAnalysisService</td><td>Complete correct + merge cycle</td></tr>' +
        '</table>' +
        '<p><strong>Queue features:</strong> SSE real-time events (<code>/api/queue/events</code>), polling fallback (<code>/api/queue</code> every 3s), ' +
        'reorder support, cancel running/queued jobs, max 50 items in history.</p>' +

        '<h3>Data Flow — End to End</h3>' +
        '<div class="help-card">' +
        '<div class="help-card-title">JAR Pipeline (5 steps)</div>' +
        '<p>1. <strong>Parse JAR bytecode</strong> — <code>JarParserService.parseJarToFile()</code>: extract all .class files, decompile each with CFR 0.152, ' +
        'read bytecode structure with ASM 9.7 (methods, fields, annotations, inheritance)</p>' +
        '<p>2. <strong>Extract config</strong> — Read application.yaml/properties from JAR for MongoDB URI, Spring settings</p>' +
        '<p>3. <strong>Build call-graph index</strong> — <code>CallGraphService.buildIndex()</code>: trace method invocations to build directed call graph, ' +
        'identify REST endpoints via @RequestMapping/@GetMapping/etc., wire Spring beans by stereotype</p>' +
        '<p>4. <strong>Stream analysis results</strong> — Write endpoints, classes, call trees to analysis.json</p>' +
        '<p>5. <strong>Persist</strong> — Save all JSONs to <code>data/jar/{name}/</code>, store JAR file locally</p></div>' +

        '<div class="help-card">' +
        '<div class="help-card-title">PL/SQL Parser Pipeline (6 steps)</div>' +
        '<p>1. <strong>SchemaResolver Init</strong> — Runs CONNECT BY on ALL_DEPENDENCIES to discover the full transitive dependency tree. Populates objectToSchema, objectToType, ambiguousObjects maps. Caches results to TSV files on disk.</p>' +
        '<p>2. <strong>DependencyCrawler BFS</strong> — Starting from entry point (depth 0), processes a BFS queue: for each node → download source (ALL_SOURCE query) → parse with ANTLR4 → extract tables/calls/vars via PlSqlAnalysisVisitor → enqueue discovered dependencies at depth+1</p>' +
        '<p>3. <strong>Table Schema Resolution</strong> — For each unqualified table name: (a) type-qualified lookup in dictionary cache, (b) objectToSchema map from dependency crawl, (c) batch ALL_OBJECTS query for tables not in cache (1 query per 500 tables)</p>' +
        '<p>4. <strong>Trigger Re-Analysis</strong> — After main crawl, discovers triggers on all DML-accessed tables. Downloads trigger source, parses, and includes their operations in the result.</p>' +
        '<p>5. <strong>ChunkedFlowWriter</strong> — Streams output: api/index.json, api/tables/index.json, api/nodes/*.json, api/call_graph.json, chunks/*.json, sources/*.sql</p>' +
        '<p>6. <strong>Queue completion</strong> — Notifies queue service, updates progress to 100%, analysis available in UI</p></div>' +

        '<h3>Configuration Files</h3>' +
        '<table>' +
        '<tr><th>File</th><th>Purpose</th></tr>' +
        '<tr><td><code>application.properties</code></td><td>Spring Boot: port (8083), data directories, thread pools (source-fetch=8, trigger-resolve=4, metadata=4, claude-parallel=5), ' +
        'tree limits (max-depth=50, max-nodes=2000), Claude timeout/chunking settings, Tomcat tuning (50 threads, 4 spare)</td></tr>' +
        '<tr><td><code>config/plsql-config.yaml</code></td><td>Oracle DB connections: projects → environments → JDBC URL, schema, credentials. ' +
        'Supports multiple projects each with multiple environments (DEV/UAT/PROD)</td></tr>' +
        '<tr><td><code>config/domain-config.json</code></td><td>Domain mapping for JAR endpoint categorization — maps package paths to business domains</td></tr>' +
        '<tr><td><code>config/prompts/*.txt</code></td><td>Claude AI prompt templates for enrichment analysis</td></tr>' +
        '</table>' +

        '<h3>Key REST API Routes</h3>' +
        '<table>' +
        '<tr><th>Route</th><th>Method</th><th>Description</th></tr>' +
        '<tr><td><code>/api/jar/jars</code></td><td>POST</td><td>Upload JAR for analysis (multipart, up to 2GB)</td></tr>' +
        '<tr><td><code>/api/jar/jars</code></td><td>GET</td><td>List all analyzed JARs with stats</td></tr>' +
        '<tr><td><code>/api/jar/jars/{id}</code></td><td>GET</td><td>Stream full analysis JSON (gzip compressed)</td></tr>' +
        '<tr><td><code>/api/jar/jars/{id}/summary/*</code></td><td>GET</td><td>Lazy-loaded summary slices (headers, external-calls, dynamic-flows, aggregation-flows, beans)</td></tr>' +
        '<tr><td><code>/api/jar/jars/{id}/endpoints/by-index/{idx}/call-tree</code></td><td>GET</td><td>Endpoint call tree by index</td></tr>' +
        '<tr><td><code>/api/jar/jars/{id}/classes/tree</code></td><td>GET</td><td>Full class hierarchy tree</td></tr>' +
        '<tr><td><code>/api/parser/analyze</code></td><td>POST</td><td>Submit PL/SQL parse analysis to queue</td></tr>' +
        '<tr><td><code>/api/parser/analyses</code></td><td>GET</td><td>List all parser analyses</td></tr>' +
        '<tr><td><code>/api/parser/analyses/{name}/*</code></td><td>GET</td><td>Analysis data: index, nodes, tables, call-graph, source, procedures, joins, cursors, sequences</td></tr>' +
        '<tr><td><code>/api/plsql/db/table-info/{name}</code></td><td>GET</td><td>Live Oracle table metadata (columns, indexes, constraints)</td></tr>' +
        '<tr><td><code>/api/plsql/db/query</code></td><td>POST</td><td>Execute read-only SELECT query against Oracle</td></tr>' +
        '<tr><td><code>/api/queue</code></td><td>GET</td><td>Queue state (current job, queued list, history)</td></tr>' +
        '<tr><td><code>/api/queue/events</code></td><td>GET</td><td>SSE stream of real-time queue events</td></tr>' +
        '<tr><td><code>/api/sessions</code></td><td>GET</td><td>All Claude sessions across all analyzers</td></tr>' +
        '</table>' +

        '<h3>Storage Layout</h3>' +
        '<table>' +
        '<tr><th>Path</th><th>Contents</th></tr>' +
        '<tr><td><code>data/jar/{jar-name}/</code></td><td>analysis.json, call-graph.json, summary.json, claude-results/, decompiled source, version history</td></tr>' +
        '<tr><td><code>data/plsql-parse/{analysis}/</code></td><td>api/ (index.json, tables/index.json, nodes/*.json, call_graph.json), chunks/*.json, edges/*.json, sources/*.sql</td></tr>' +
        '<tr><td><code>cache/schema-resolver/</code></td><td>TSV cache of Oracle schema resolution (objects, types, dependencies) — avoids re-querying ALL_DEPENDENCIES</td></tr>' +
        '<tr><td><code>data/unified-analyzer.log</code></td><td>Rolling log file (10MB max, 10 history, 500MB cap)</td></tr>' +
        '</table>';
    }

    // ──────────────────────────────────────
    //  JAR — TECHNICAL
    // ──────────────────────────────────────
    function getJarTech(){
        return '' +
        '<h3>JAR Analysis Pipeline — Step by Step</h3>' +
        '<p>When a JAR is uploaded, <code>JarAnalysisExecutor</code> runs these 5 steps:</p>' +

        '<div class="help-card"><div class="help-card-title">Step 1: Parse JAR Bytecode</div>' +
        '<p><code>JarParserService.parseJarToFile()</code> — Extracts all .class files from the JAR. For each class:</p>' +
        '<ul>' +
        '<li><strong>CFR 0.152 decompilation</strong> — Converts bytecode to readable Java source</li>' +
        '<li><strong>ASM 9.7 bytecode scan</strong> — Reads class structure: superclass, interfaces, fields (name, type, annotations), methods (signature, parameters, return type, annotations, access flags), method body instructions (INVOKE* opcodes for call graph)</li>' +
        '<li>Extracts: Spring annotations (@RestController, @Service, @Repository, @Component, @RequestMapping, @Scheduled, etc.), JPA annotations (@Entity, @Table, @Column), MongoDB annotations (@Document, @Field)</li>' +
        '</ul></div>' +

        '<div class="help-card"><div class="help-card-title">Step 2: Extract Config</div>' +
        '<p>Reads application.yaml / application.properties from inside the JAR. Extracts MongoDB URI (for catalog verification), Spring profiles, and other config.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Step 3: Build Call-Graph Index</div>' +
        '<p><code>CallGraphService.buildIndex()</code> — Traces method invocations from ASM data to build directed call graph:</p>' +
        '<ul>' +
        '<li>Identifies REST endpoints: @RequestMapping, @GetMapping, @PostMapping, @PutMapping, @DeleteMapping, @PatchMapping with full path resolution (class-level + method-level)</li>' +
        '<li>Wires Spring beans: matches method invocations to bean implementations by type hierarchy</li>' +
        '<li>Handles dispatch modes: DIRECT (concrete call), QUALIFIED (@Qualifier), INTERFACE_DISPATCH (multiple implementations), HEURISTIC (best guess), @PRIMARY (Spring primary bean)</li>' +
        '<li>Detects MongoDB operations: MongoTemplate calls (find, save, aggregate, etc.), MongoRepository methods (findBy*, save, delete), aggregation pipeline stages ($match, $group, $lookup, etc.)</li>' +
        '<li>Detects JPA/Hibernate operations: @Query annotations, CrudRepository methods, custom repository implementations</li>' +
        '<li>Detects JDBC operations: JdbcTemplate calls, NamedParameterJdbcTemplate, stored procedure calls</li>' +
        '<li>Computes: endpoint-to-collection mapping, cross-domain calls, cyclomatic complexity, LOC per method/class</li>' +
        '</ul></div>' +

        '<div class="help-card"><div class="help-card-title">Step 4: Stream Analysis Results</div>' +
        '<p>Writes complete analysis to JSON files: all endpoints with their call trees, all classes with their members, all collections with operations, all beans with stereotypes.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Step 5: Persist to Disk</div>' +
        '<p>Saves to <code>data/jar/{name}/</code>: analysis.json, call-graph.json, summary.json (gzip-compressed for large JARs). Stores original JAR file. Version history maintained for Claude iterations.</p></div>' +

        '<h3>Key Backend Classes</h3>' +
        '<table>' +
        '<tr><th>Class</th><th>Role</th></tr>' +
        '<tr><td><code>JarParserService</code></td><td>Orchestrates full analysis: extract → decompile → scan → index</td></tr>' +
        '<tr><td><code>BytecodeClassParser</code></td><td>ASM 9.7 ClassVisitor — reads class structure, fields, methods, instructions</td></tr>' +
        '<tr><td><code>DecompilerService</code></td><td>CFR 0.152 wrapper — decompiles .class to Java source for code viewer</td></tr>' +
        '<tr><td><code>EndpointExtractor</code></td><td>Scans Spring @*Mapping annotations, resolves full REST paths</td></tr>' +
        '<tr><td><code>CallGraphBuilder</code></td><td>Builds method-level directed call graph from INVOKE* opcodes</td></tr>' +
        '<tr><td><code>PersistenceService</code></td><td>JSON file persistence, gzip compression, version rotation for Claude iterations</td></tr>' +
        '<tr><td><code>ClaudeAnalysisService</code></td><td>Chunks endpoints for Claude CLI, sends structured prompts, parses JSON responses, merges enrichment</td></tr>' +
        '<tr><td><code>MongoCatalogService</code></td><td>Connects to MongoDB (from extracted URI) to verify collection existence (IN_DB/NOT_IN_DB)</td></tr>' +
        '<tr><td><code>SourceEnrichmentService</code></td><td>Annotates endpoints with Claude-generated business descriptions and risk flags</td></tr>' +
        '</table>' +

        '<h3>Claude AI Enrichment Pipeline</h3>' +
        '<p>When triggered via "Enable Claude" or queue job:</p>' +
        '<ul>' +
        '<li><strong>Chunking</strong> — Groups endpoints into chunks that fit within ~180K chars context window. Max 50K chars per chunk, max 500 tree nodes per chunk.</li>' +
        '<li><strong>Prompt Construction</strong> — Uses templates from <code>config/prompts/*.txt</code>. Includes: endpoint metadata, call tree, collection operations, decompiled source snippets.</li>' +
        '<li><strong>Claude CLI Execution</strong> — Spawns <code>claude</code> CLI process per chunk. Parallel execution (default 5 parallel chunks). ' +
        'Timeout: 7200s per endpoint. Tools allowed: Read, Grep, Glob, Bash, Write, Edit.</li>' +
        '<li><strong>Response Parsing</strong> — Parses structured JSON responses: business process description, risk flags, migration difficulty, collection corrections (added/removed/verified).</li>' +
        '<li><strong>Merge &amp; Persist</strong> — Merges Claude results into analysis data. Version rotation: static → claude-v1 → claude-v2. Previous versions preserved for comparison.</li>' +
        '<li><strong>Session Management</strong> — Each CLI process tracked as a session (RUNNING/COMPLETE/FAILED/KILLED). Kill support for individual or all sessions.</li>' +
        '</ul>' +

        '<h3>Summary Sub-Tab Data Sources</h3>' +
        '<table>' +
        '<tr><th>Sub-Tab</th><th>API Endpoint</th><th>Computation</th></tr>' +
        '<tr><td>Endpoint Report</td><td><code>/api/jar/jars/{id}/summary/headers</code></td><td>Per-endpoint: count collections, DB ops, internal/external calls, compute LOC from call tree, classify size S/M/L/XL</td></tr>' +
        '<tr><td>Collection Analysis</td><td><code>/api/jar/jars/{id}/collections</code></td><td>Aggregate collection usage across all endpoints, cross-reference with MongoDB catalog for verification</td></tr>' +
        '<tr><td>External Deps</td><td><code>/api/jar/jars/{id}/summary/external-calls</code></td><td>Identify method calls to classes in different JAR modules (external dependencies)</td></tr>' +
        '<tr><td>Aggregation Flows</td><td><code>/api/jar/jars/{id}/summary/aggregation-flows</code></td><td>Detect MongoTemplate.aggregate() calls, parse pipeline stages, identify $lookup cross-collection joins</td></tr>' +
        '<tr><td>Dynamic Flows</td><td><code>/api/jar/jars/{id}/summary/dynamic-flows</code></td><td>Identify interface dispatch, reflection, dynamic queries where static analysis is uncertain</td></tr>' +
        '<tr><td>Verticalisation</td><td>Computed client-side</td><td>Cross-reference endpoint domains with collection ownership to find boundary violations</td></tr>' +
        '<tr><td>Claude Insights</td><td><code>/api/jar/jars/{id}/corrections</code></td><td>Claude-generated per-endpoint analysis: business process, risk, migration strategy</td></tr>' +
        '<tr><td>Claude Corrections</td><td><code>/api/jar/jars/{id}/corrections/{ep}</code></td><td>Diff: static collections vs Claude collections → added/removed/verified</td></tr>' +
        '</table>' +

        '<h3>JAR API Endpoints (39 total)</h3>' +
        '<table>' +
        '<tr><th>Method</th><th>Path</th><th>Purpose</th></tr>' +
        '<tr><td>POST</td><td><code>/api/jar/jars</code></td><td>Upload JAR (multipart, up to 2GB)</td></tr>' +
        '<tr><td>POST</td><td><code>/api/jar/jars/analyze-local</code></td><td>Analyze local JAR by file path</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars</code></td><td>List all JARs</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars/{id}</code></td><td>Full analysis JSON (streamed, gzip)</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars/{id}/summary/*</code></td><td>Lazy summary slices: headers, external-calls, dynamic-flows, aggregation-flows, beans</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars/{id}/endpoints/by-index/{idx}/call-tree</code></td><td>Endpoint call tree</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars/{id}/classes/tree</code></td><td>Class hierarchy</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars/{id}/classes/by-index/{idx}</code></td><td>Single class by index</td></tr>' +
        '<tr><td>POST</td><td><code>/api/jar/jars/export-excel</code></td><td>Export to Excel</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars/{id}/versions</code></td><td>Version info (static, claude-v1, etc.)</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars/{id}/catalog</code></td><td>MongoDB catalog (collections in DB)</td></tr>' +
        '<tr><td>POST</td><td><code>/api/jar/jars/{id}/claude-enrich-single</code></td><td>Enrich single endpoint</td></tr>' +
        '<tr><td>POST</td><td><code>/api/jar/jars/{id}/claude-rescan</code></td><td>Resume/rescan Claude</td></tr>' +
        '<tr><td>POST</td><td><code>/api/jar/jars/{id}/claude-full-scan</code></td><td>Full correct + merge</td></tr>' +
        '<tr><td>POST</td><td><code>/api/jar/jars/{id}/claude-correct</code></td><td>Generate corrections</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars/{id}/corrections</code></td><td>All corrections</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars/{id}/correction-logs/*</code></td><td>Correction log files</td></tr>' +
        '<tr><td>DELETE</td><td><code>/api/jar/jars/{id}</code></td><td>Delete JAR + all sessions</td></tr>' +
        '<tr><td>POST</td><td><code>/api/jar/jars/{id}/reanalyze</code></td><td>Re-analyze specific JAR</td></tr>' +
        '<tr><td>POST</td><td><code>/api/jar/jars/{id}/revert-claude</code></td><td>Revert to pre-Claude version</td></tr>' +
        '<tr><td>GET</td><td><code>/api/jar/jars/sessions</code></td><td>List all Claude sessions</td></tr>' +
        '<tr><td>POST</td><td><code>/api/jar/jars/sessions/{id}/kill</code></td><td>Kill specific session</td></tr>' +
        '</table>' +

        '<h3>Data Model — Files on Disk</h3>' +
        '<p>Each analyzed JAR produces files in <code>data/jar/{name}/</code>:</p>' +
        '<table>' +
        '<tr><th>File</th><th>Contents</th></tr>' +
        '<tr><td><code>analysis.json</code></td><td>Complete static analysis: all endpoints with call trees, all classes with members, all methods with instructions</td></tr>' +
        '<tr><td><code>call-graph.json</code></td><td>Directed graph of method calls: nodes (classes) + edges (method invocations)</td></tr>' +
        '<tr><td><code>summary.json</code></td><td>Gzip-compressed summary: endpoint count, class count, collection stats, domain breakdown</td></tr>' +
        '<tr><td><code>claude-results/</code></td><td>Per-chunk Claude responses, correction diffs, session logs</td></tr>' +
        '<tr><td><code>versions/</code></td><td>Previous analysis versions (pre-Claude, claude-v1, etc.) for rollback</td></tr>' +
        '</table>';
    }

    // ──────────────────────────────────────
    //  PARSER — TECHNICAL
    // ──────────────────────────────────────
    function getParserTech(){
        return '' +
        '<h3>PL/SQL Parser Pipeline — Step by Step</h3>' +
        '<p>When a parse analysis is submitted, <code>ParserAnalysisExecutor</code> invokes <code>FlowAnalysisMain.run()</code> which orchestrates the full pipeline:</p>' +

        '<div class="help-card"><div class="help-card-title">Step 1: Initialization</div>' +
        '<p>Parse CLI arguments (entry point, output dir, config path, max depth). Initialize DB connection manager from <code>plsql-config.yaml</code>. ' +
        'Clear disk cache if requested. Initialize all components: SchemaResolver, SourceDownloader, PlSqlParserEngine, DependencyCrawler.</p></div>' +

        '<div class="help-card"><div class="help-card-title">Step 2: SchemaResolver — Dependency Discovery</div>' +
        '<p>Resolves which Oracle schema owns each object in the dependency tree. Executed once at startup, results cached to disk.</p>' +
        '<ul>' +
        '<li><strong>Step 2a — Resolve seed owners:</strong> Batched ALL_OBJECTS query to find which schema owns the entry point object(s)</li>' +
        '<li><strong>Step 2b — Transitive dependencies:</strong> 1 CONNECT BY query PER SEED on ALL_DEPENDENCIES — discovers the full transitive dependency tree. Returns schema + type for all deps including tables/views. This is the most expensive query (~100-300ms for 50-100 deps).</li>' +
        '<li><strong>Step 2c — Reverse dependencies:</strong> 1 indexed query per seed — finds who depends on the entry point</li>' +
        '<li><strong>Step 2d — Direct deps:</strong> Batched IN clause query for BFS validation</li>' +
        '<li><strong>In-memory derivation:</strong> Table/view ownership and summary stats computed from query results, not additional DB queries (0ms)</li>' +
        '<li><strong>Disk cache:</strong> Saves objectToSchema, objectToType, dependencyCache maps as TSV files in <code>cache/schema-resolver/</code>. Next run skips all DB queries if cache exists.</li>' +
        '</ul>' +
        '<p><strong>Performance:</strong> ~200-500ms total for typical analysis (vs 10s+ for naive per-object approach)</p></div>' +

        '<div class="help-card"><div class="help-card-title">Step 3: DependencyCrawler BFS Traversal</div>' +
        '<p>The core analysis engine. Processes a BFS queue starting from the entry point:</p>' +
        '<ol>' +
        '<li>Parse entry point — Split "PKG.PROC" or "STANDALONE_FUNC"</li>' +
        '<li>Pre-warm transitive deps — <code>schemaResolver.getTransitiveDependencies(entryObj)</code></li>' +
        '<li>Initialize BFS queue — Add entry point as root (depth 0)</li>' +
        '<li><strong>For each node in queue:</strong>' +
        '<ul>' +
        '<li>Build visit key, check if already visited (dedup)</li>' +
        '<li>Validate depth (abort if &gt; maxDepth, default 50)</li>' +
        '<li>Check timeout (abort if elapsed &gt; timeoutPerEntryMs, default 7200s)</li>' +
        '<li><strong>Download source</strong> — <code>SourceDownloader.getSource(schema, objectName)</code> queries ALL_SOURCE, caches locally as .sql file</li>' +
        '<li><strong>Parse source</strong> — <code>PlSqlParserEngine.parse(source)</code> → ANTLR4 lexer+parser → ParseTree</li>' +
        '<li><strong>Extract analysis</strong> — <code>PlSqlAnalysisVisitor</code> walks the AST, extracts: table operations (SELECT/INSERT/UPDATE/DELETE/MERGE with line numbers), ' +
        'procedure/function calls (with parameters), variable declarations, cursor definitions, join conditions, sequence usage (NEXTVAL/CURRVAL), exception handlers, dynamic SQL (EXECUTE IMMEDIATE)</li>' +
        '<li><strong>Enqueue dependencies</strong> — For each external call to another schema/package, add to BFS queue at depth+1</li>' +
        '<li><strong>Write chunk</strong> — If ChunkedFlowWriter configured, write node data to <code>chunks/{nodeId}.json</code></li>' +
        '</ul></li>' +
        '</ol>' +
        '<p><strong>Limits:</strong> max-depth=50, max-nodes=2000, thread pool: source-fetch=8, trigger-resolve=4, metadata=4</p></div>' +

        '<div class="help-card"><div class="help-card-title">Step 4: Table Schema Resolution</div>' +
        '<p>After BFS completes, resolves the owning schema for each unqualified table name found by the parser:</p>' +
        '<ol>' +
        '<li><strong>Type-qualified lookup</strong> — Check <code>typeQualifiedToSchema["TABLE." + name]</code> from SchemaResolver cache</li>' +
        '<li><strong>Object-to-schema map</strong> — Check <code>objectToSchema[name]</code> from ALL_DEPENDENCIES transitive crawl</li>' +
        '<li><strong>Batch ALL_OBJECTS fallback</strong> — For tables not in any cache (common for tables only referenced in SQL within PL/SQL bodies, not in ALL_DEPENDENCIES): ' +
        'collects all unresolved names, runs 1 ALL_OBJECTS query per 500 tables with <code>WHERE OBJECT_NAME IN (...) AND OBJECT_TYPE IN (\'TABLE\',\'VIEW\',\'MATERIALIZED VIEW\')</code></li>' +
        '</ol></div>' +

        '<div class="help-card"><div class="help-card-title">Step 5: Trigger Re-Analysis</div>' +
        '<p>After all tables are resolved, discovers triggers on tables with DML operations:</p>' +
        '<ul>' +
        '<li>Queries ALL_TRIGGERS for each table with INSERT/UPDATE/DELETE operations</li>' +
        '<li>Downloads trigger source, parses with ANTLR4</li>' +
        '<li>Includes trigger table operations in the analysis result</li>' +
        '<li>Thread pool: trigger-resolve=4 parallel</li>' +
        '</ul></div>' +

        '<div class="help-card"><div class="help-card-title">Step 6: Output Persistence</div>' +
        '<p><code>ChunkedFlowWriter</code> produces the final output in <code>data/plsql-parse/{ENTRY_POINT}/</code>:</p>' +
        '<table>' +
        '<tr><th>File</th><th>Contents</th></tr>' +
        '<tr><td><code>api/index.json</code></td><td>Analysis metadata: entry point, total nodes, total tables, total edges, total LOC, max depth, errors, timing</td></tr>' +
        '<tr><td><code>api/tables/index.json</code></td><td>All tables with: name, schema, operations, access counts, trigger info, join participation</td></tr>' +
        '<tr><td><code>api/nodes/*.json</code></td><td>Per-procedure detail: tables accessed, calls made, variables, parameters, cursors, complexity score</td></tr>' +
        '<tr><td><code>api/call_graph.json</code></td><td>Edge list: [{source, target, callType, lineNumber}, ...]</td></tr>' +
        '<tr><td><code>chunks/*.json</code></td><td>Raw parse results per BFS node — intermediate data before aggregation</td></tr>' +
        '<tr><td><code>sources/*.sql</code></td><td>Downloaded PL/SQL source files from ALL_SOURCE</td></tr>' +
        '</table></div>' +

        '<h3>Key Backend Classes</h3>' +
        '<table>' +
        '<tr><th>Class</th><th>Role</th></tr>' +
        '<tr><td><code>FlowAnalysisMain</code></td><td>CLI entry point — parses args, orchestrates all pipeline steps in order</td></tr>' +
        '<tr><td><code>DependencyCrawler</code></td><td>BFS engine — manages visit queue, invokes source download + parse + extract per node, handles depth/timeout limits</td></tr>' +
        '<tr><td><code>SchemaResolver</code></td><td>Oracle dictionary resolution with disk cache — CONNECT BY on ALL_DEPENDENCIES, batch ALL_OBJECTS fallback</td></tr>' +
        '<tr><td><code>PlSqlParserEngine</code></td><td>ANTLR4 wrapper — instantiates lexer + parser, custom error handling (BailErrorStrategy), returns ParseTree</td></tr>' +
        '<tr><td><code>PlSqlAnalysisVisitor</code></td><td>AST visitor — walks ParseTree extracting: visitTableview_name (tables), visitFunction_call / visitProcedure_call (calls), visitCursor_declaration (cursors), visitJoin_clause (joins), visitSequence_reference (sequences)</td></tr>' +
        '<tr><td><code>SourceDownloader</code></td><td>Downloads PL/SQL source from ALL_SOURCE, caches as local .sql files, parallel fetch (8 threads)</td></tr>' +
        '<tr><td><code>ChunkedFlowWriter</code></td><td>Streams output to disk in chunks — handles analyses with 2000+ nodes without OOM</td></tr>' +
        '<tr><td><code>DbConnectionManager</code></td><td>Oracle JDBC connection pool management — creates/caches connections per project+environment</td></tr>' +
        '<tr><td><code>ClaudeVerificationService</code></td><td>Chunks analysis data for Claude CLI, sends verification prompts, aggregates results, manages version rotation</td></tr>' +
        '</table>' +

        '<h3>ANTLR4 Grammar</h3>' +
        '<p>Three grammar files cover Oracle PL/SQL from 8i through 23c:</p>' +
        '<ul>' +
        '<li><code>PlSqlLexer.g4</code> — Tokenizer with 500+ Oracle keywords</li>' +
        '<li><code>PlSqlParser.g4</code> — Full DML/DDL/PL/SQL block grammar covering: SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE, ALTER, DROP, ' +
        'EXECUTE IMMEDIATE, cursor operations, exception handlers, BULK COLLECT, FORALL, PIPE ROW, hierarchical queries (CONNECT BY), ' +
        'flashback queries (AS OF), MERGE with conditional INSERT/UPDATE, window functions, JSON operations, Oracle 23c syntax</li>' +
        '<li><code>PlSqlBaseVisitor</code> — Generated visitor base class (all 200+ visit methods)</li>' +
        '</ul>' +
        '<p>Test suite: <strong>395 tests, zero failures</strong> covering edge cases: flashback queries, MERGE statements, hierarchical queries, ' +
        'Oracle 23c syntax, encrypted packages, dynamic SQL, bulk operations, pipelined functions.</p>' +

        '<h3>Parser API Endpoints (30+ total)</h3>' +
        '<table>' +
        '<tr><th>Method</th><th>Path</th><th>Purpose</th></tr>' +
        '<tr><td>POST</td><td><code>/api/parser/analyze</code></td><td>Submit parse analysis (queued)</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses</code></td><td>List all analyses</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/index</code></td><td>Analysis metadata + stats</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/node/{file}</code></td><td>Per-procedure detail (tables, calls, vars)</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/tables</code></td><td>All tables with operations</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/call-graph</code></td><td>Call graph edges</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/source/{file}</code></td><td>PL/SQL source code</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/procedures</code></td><td>All procedures with stats</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/joins</code></td><td>All joins</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/cursors</code></td><td>All cursors</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/sequences</code></td><td>All sequences</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/call-tree/{nodeId}</code></td><td>Call tree from node</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/call-tree/{nodeId}/callers</code></td><td>Who calls this node</td></tr>' +
        '<tr><td>POST</td><td><code>/api/parser/analyses/{name}/claude/verify</code></td><td>Start Claude verification</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/claude/progress</code></td><td>Verification progress</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/claude/result</code></td><td>Verification result</td></tr>' +
        '<tr><td>GET</td><td><code>/api/parser/analyses/{name}/claude/chunks/*</code></td><td>Chunk details and summaries</td></tr>' +
        '<tr><td>POST</td><td><code>/api/parser/analyses/{name}/claude/review</code></td><td>Save user review decisions</td></tr>' +
        '<tr><td>POST</td><td><code>/api/parser/analyses/{name}/claude/apply</code></td><td>Apply accepted changes</td></tr>' +
        '</table>' +

        '<h3>Database API Endpoints (10 total)</h3>' +
        '<table>' +
        '<tr><th>Method</th><th>Path</th><th>Purpose</th></tr>' +
        '<tr><td>GET</td><td><code>/api/plsql/db/users</code></td><td>List Oracle DB users (with project/env filter)</td></tr>' +
        '<tr><td>POST</td><td><code>/api/plsql/db/test</code></td><td>Test Oracle connection</td></tr>' +
        '<tr><td>GET</td><td><code>/api/plsql/db/objects/{username}</code></td><td>List objects in schema</td></tr>' +
        '<tr><td>GET</td><td><code>/api/plsql/db/source/{user}/{obj}</code></td><td>Fetch PL/SQL source from ALL_SOURCE</td></tr>' +
        '<tr><td>GET</td><td><code>/api/plsql/db/table-info/{name}</code></td><td>Table metadata: columns (name, type, nullable, default, PK), constraints (P/U/R/C with references), indexes (name, uniqueness, column list)</td></tr>' +
        '<tr><td>POST</td><td><code>/api/plsql/db/query</code></td><td>Execute read-only SELECT (query runner feature)</td></tr>' +
        '<tr><td>GET</td><td><code>/api/plsql/db/packages/{user}</code></td><td>List packages with procedure counts</td></tr>' +
        '<tr><td>GET</td><td><code>/api/plsql/db/package/{user}/{pkg}</code></td><td>List procedures in package</td></tr>' +
        '<tr><td>GET</td><td><code>/api/plsql/db/find/{input}</code></td><td>Smart object finder (SCHEMA.PKG.PROC)</td></tr>' +
        '<tr><td>GET</td><td><code>/api/plsql/db/cached-source</code></td><td>Get cached source for viewer</td></tr>' +
        '</table>' +

        '<h3>UI Table Framework</h3>' +
        '<p><code>tableFramework.js</code> powers all sortable/filterable tables across the parser UI:</p>' +
        '<ul>' +
        '<li><strong>Sortable headers</strong> — Click any column header to sort (asc/desc toggle). Sort state persisted per table instance.</li>' +
        '<li><strong>Column-level filters</strong> — Dropdown filter per column (multi-select values extracted from data). Filter icon appears in column header.</li>' +
        '<li><strong>Row detail expansion</strong> — Click row to expand detail view. Lazy-loaded (fetches data on demand for large datasets).</li>' +
        '<li><strong>Pagination</strong> — "Show more" button with configurable batch size. Shows "X shown of Y total".</li>' +
        '<li><strong>Extra filters</strong> — Custom filter functions (e.g., operation pills in Table Ops).</li>' +
        '<li><strong>Count callbacks</strong> — Updates badge counts as filters change.</li>' +
        '</ul>' +
        '<p>Table IDs: <code>tda</code> (accesses), <code>tdj</code> (joins), <code>tdt</code> (triggers), ' +
        '<code>tdmc</code> (DB columns), <code>tdmi</code> (DB indexes), <code>tdmk</code> (DB constraints)</p>' +

        '<h3>Complexity Scoring Algorithm</h3>' +
        '<p>Each procedure gets a weighted composite score based on 6 factors:</p>' +
        '<table>' +
        '<tr><th>Factor</th><th>What It Measures</th><th>Weight</th></tr>' +
        '<tr><td>LOC</td><td>Lines of code in procedure body</td><td>Configurable (API: <code>/api/parser/config/complexity</code>)</td></tr>' +
        '<tr><td>Tables</td><td>Number of distinct tables accessed</td><td>Configurable</td></tr>' +
        '<tr><td>Calls</td><td>Number of procedure/function calls made</td><td>Configurable</td></tr>' +
        '<tr><td>Cursors</td><td>Number of cursor operations</td><td>Configurable</td></tr>' +
        '<tr><td>Dynamic SQL</td><td>EXECUTE IMMEDIATE count</td><td>Configurable</td></tr>' +
        '<tr><td>Depth</td><td>Call tree depth from entry point</td><td>Configurable</td></tr>' +
        '</table>' +
        '<p>Risk thresholds: Low (&lt; T1), Medium (T1–T2), High (&gt; T2). Thresholds loaded from backend configuration.</p>';
    }

    return { init:init, open:open, close:close, setMode:setMode };
})();
