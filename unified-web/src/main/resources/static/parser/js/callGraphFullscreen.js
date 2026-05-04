window.PA = window.PA || {};

PA.callGraphFullscreen = {
    _active: false,

    toggle: function() {
        var view = document.getElementById('viewCallGraphViz');
        if (!view) return;

        PA.callGraphFullscreen._active = !PA.callGraphFullscreen._active;

        if (PA.callGraphFullscreen._active) {
            view.classList.add('cg-fullscreen');
            document.body.style.overflow = 'hidden';
        } else {
            view.classList.remove('cg-fullscreen');
            document.body.style.overflow = '';
        }

        var btn = document.getElementById('cgFullscreenBtn');
        if (btn) btn.innerHTML = PA.callGraphFullscreen._active ? '&#x2716;' : '&#x26F6;';

        var canvas = document.getElementById('graphCanvas');
        if (canvas && canvas.parentElement) {
            canvas.width = canvas.parentElement.clientWidth;
            canvas.height = canvas.parentElement.clientHeight;
        }

        PA.callGraphViz._draw();
    },

    onKeyDown: function(e) {
        if (e.key !== 'Escape') return;
        var modal = document.getElementById('cgModalOverlay');
        if (modal && modal.style.display !== 'none') {
            PA.callGraphModal.close();
            return;
        }
        if (PA.callGraphFullscreen._active) {
            PA.callGraphFullscreen.toggle();
        }
    },

    init: function() {
        if (PA.callGraphFullscreen._inited) return;
        PA.callGraphFullscreen._inited = true;
        document.addEventListener('keydown', PA.callGraphFullscreen.onKeyDown);
    }
};
