(function (window) {
  "use strict";

  let lastPageviewUrl = null;
  let endpoint = null;

  // Handle existing queue
  const existingQueue = (window.atomlytics && window.atomlytics.q) || [];

  // Initialize the analytics
  function initAtomlytics() {
    if (window.atomlytics && !Array.isArray(window.atomlytics)) {
      return window.atomlytics;
    }

    const scriptTag = document.currentScript;
    endpoint = scriptTag.src.substring(0, scriptTag.src.lastIndexOf("/"));

    const instance = {
      track: trackEvent,
    };

    Object.freeze(instance);
    return instance;
  }

  // Track events function
  function trackEvent(eventName, props = {}) {
    const currentPathname = window.location.pathname;
    // Skip if it's a pageview event with the same pathname as last time
    if (eventName === "pageview" && lastPageviewUrl === currentPathname) {
      return;
    }

    // Update lastPageviewUrl if this is a pageview event
    if (eventName === "pageview") {
      lastPageviewUrl = currentPathname;
    }

    const data = {
      n: eventName,
      u: window.location.href,
      r: document.referrer || null,
      p: props,
    };

    const xhr = new XMLHttpRequest();
    xhr.open("POST", endpoint + "/api/event", true);
    xhr.setRequestHeader("Content-Type", "application/json");

    xhr.onerror = function () {
      console.error("Atomlytics error:", xhr.statusText);
    };

    try {
      xhr.send(JSON.stringify(data));
    } catch (err) {
      console.error("Atomlytics error:", err);
    }
  }

  // Create instance
  const instance = initAtomlytics();

  // Replace any existing queue with the real implementation
  window.atomlytics = instance;

  // Process existing queue
  for (let i = 0; i < existingQueue.length; i++) {
    const [eventName, props] = existingQueue[i];
    trackEvent(eventName, props);
  }

  // Monkey patch history.pushState to track page views
  const originalPushState = window.history.pushState;
  window.history.pushState = function () {
    originalPushState.apply(this, arguments);
    // After state is pushed, track the pageview
    instance.track("pageview");
  };

  // Also patch replaceState to be thorough
  const originalReplaceState = window.history.replaceState;
  window.history.replaceState = function () {
    originalReplaceState.apply(this, arguments);
    instance.track("pageview");
  };

  // Handle back/forward navigation
  window.addEventListener("popstate", () => {
    instance.track("pageview");
  });

  // Auto-track page views
  document.addEventListener("DOMContentLoaded", () => {
    instance.track("pageview");
  });
})(window);
