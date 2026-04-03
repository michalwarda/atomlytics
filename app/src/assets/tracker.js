(function (window) {
  "use strict";

  let lastPageviewPath = null;
  let endpoint = null;
  const queuedCalls = (window.atomlytics && window.atomlytics.q) || [];

  function initAtomlytics() {
    if (window.atomlytics && !Array.isArray(window.atomlytics)) {
      return window.atomlytics;
    }

    const scriptTag = document.currentScript;
    endpoint = scriptTag.src.substring(0, scriptTag.src.lastIndexOf("/"));

    const api = {
      track: trackEvent,
    };

    Object.freeze(api);
    return api;
  }

  function trackEvent(eventName, props = {}) {
    const currentPath = window.location.pathname;
    if (eventName === "pageview" && currentPath === lastPageviewPath) {
      return;
    }

    if (eventName === "pageview") {
      lastPageviewPath = currentPath;
    }

    const payload = {
      n: eventName,
      u: window.location.href,
      r: document.referrer || null,
      p: props,
    };

    const request = new XMLHttpRequest();
    request.open("POST", endpoint + "/api/event", true);
    request.setRequestHeader("Content-Type", "application/json");
    request.onerror = function () {
      console.error("Atomlytics error:", request.statusText);
    };

    try {
      request.send(JSON.stringify(payload));
    } catch (error) {
      console.error("Atomlytics error:", error);
    }
  }

  const atomlytics = initAtomlytics();
  window.atomlytics = atomlytics;

  for (let index = 0; index < queuedCalls.length; index += 1) {
    const [eventName, props] = queuedCalls[index];
    trackEvent(eventName, props);
  }

  const originalPushState = window.history.pushState;
  window.history.pushState = function () {
    originalPushState.apply(this, arguments);
    atomlytics.track("pageview");
  };

  const originalReplaceState = window.history.replaceState;
  window.history.replaceState = function () {
    originalReplaceState.apply(this, arguments);
    atomlytics.track("pageview");
  };

  window.addEventListener("popstate", function () {
    atomlytics.track("pageview");
  });

  document.addEventListener("DOMContentLoaded", function () {
    atomlytics.track("pageview");
  });
})(window);
