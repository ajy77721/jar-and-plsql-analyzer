PA.currentScreen = 'home';
PA.analysisData = null;
PA.currentDetail = null;
PA._currentRightTab = 'callTrace';
PA._skipSourceAutoLoad = false;
PA._rightTabLoaded = {};

PA.showScreen = function(name) {
    document.querySelectorAll('.screen').forEach(function(s) { s.classList.remove('active'); });
    var el = document.getElementById(name === 'analysis' ? 'screenAnalysis' : 'screenHome');
    if (el) el.classList.add('active');
    PA.currentScreen = name;
};

PA.goHome = function() {
    PA.currentDetail = null;
    PA.context = { procId: null, procDetail: null, scopedTables: [], callTreeNodeIds: null };
    PA._rightTabLoaded = {};
    PA._cachedTablesIndex = null;
    PA.callTrace.treeData = null;
    PA.callTrace.breadcrumbStack = [];
    PA.refs._loaded = false;
    PA.refs.callsItems = [];
    PA.refs.callersItems = [];
    PA.sourceModal._history = [];
    PA.sourceModal._historyFwd = [];

    PA._updateDetailHeader(null);
    var clearIds = ['ctContainer', 'traceContainer', 'refsContainer', 'srcContainer',
        'toContainer', 'joinContainer', 'curContainer', 'stmtContainer',
        'exhContainer', 'summaryContainer', 'graphContainer'];
    for (var i = 0; i < clearIds.length; i++) {
        var el = document.getElementById(clearIds[i]);
        if (el) el.innerHTML = '';
    }
    var srcPath = document.getElementById('srcPath');
    if (srcPath) srcPath.textContent = 'No file open';
    var srcDetailBar = document.getElementById('srcDetailBar');
    if (srcDetailBar) { srcDetailBar.style.display = 'none'; srcDetailBar.innerHTML = ''; }
    var srcSearchBar = document.getElementById('srcSearchBar');
    if (srcSearchBar) srcSearchBar.style.display = 'none';

    PA.showScreen('home');
    PA.loadHistory();
    if (window.location.hash !== '#/home' && window.location.hash !== '') {
        history.pushState(null, '', '#/home');
    }
};

PA.goAnalysis = function() { PA.showScreen('analysis'); };

PA._handleRoute = function() {
    var hash = window.location.hash || '';

    var procMatch = hash.match(/^#\/analysis\/([^/]+)\/proc\/(.+)$/);
    if (procMatch) {
        var name = decodeURIComponent(procMatch[1]);
        var procId = decodeURIComponent(procMatch[2]);
        if (!PA.analysisData || PA.analysisData.name !== name) {
            PA.loadAnalysis(name, { fromRoute: true, procId: procId });
        } else if (PA.context.procId !== procId) {
            PA.showProcedure(procId, { fromRoute: true });
        }
        return;
    }

    var analysisMatch = hash.match(/^#\/analysis\/([^/]+)$/);
    if (analysisMatch) {
        var aName = decodeURIComponent(analysisMatch[1]);
        if (!PA.analysisData || PA.analysisData.name !== aName) {
            PA.loadAnalysis(aName, { fromRoute: true });
        }
        return;
    }

    if (hash === '#/home' || !hash) {
        if (PA.currentScreen === 'analysis') PA.goHome();
    }
};
