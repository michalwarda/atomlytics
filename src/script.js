(function(window) {
    'use strict';

    class Atomlytics {
        constructor() {
            if (Atomlytics.instance) {
                return Atomlytics.instance;
            }
            Atomlytics.instance = this;
            const scriptTag = document.currentScript;
            this.endpoint = scriptTag.src.substring(0, scriptTag.src.lastIndexOf('/'));
        }

        track(eventName, props = {}) {
            const data = {
                n: eventName,
                u: window.location.href,
                r: props
            };

            fetch(this.endpoint + '/api/event', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data),
                keepalive: true // Ensures the request is sent even if the page is unloading
            }).catch(err => {
                console.error('Atomlytics error:', err);
            });
        }
    }
    // Monkey patch history.pushState to track page views
    const originalPushState = window.history.pushState;
    window.history.pushState = function() {
        originalPushState.apply(this, arguments);
        // After state is pushed, track the pageview
        instance.track('pageview');
    };

    // Also patch replaceState to be thorough
    const originalReplaceState = window.history.replaceState;
    window.history.replaceState = function() {
        originalReplaceState.apply(this, arguments);
        instance.track('pageview'); 
    };

    // Handle back/forward navigation
    window.addEventListener('popstate', () => {
        instance.track('pageview');
    });

    // Create singleton instance
    const instance = new Atomlytics();
    Object.freeze(instance);

    // Add to window
    window.atomlytics = instance;

    // Auto-track page views
    document.addEventListener('DOMContentLoaded', () => {
        instance.track('pageview');
    });
})(window); 