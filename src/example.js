!(function () {
  "use strict";
  var l = window.location,
    r = window.document,
    t = r.currentScript,
    o = t.getAttribute("data-api") || new URL(t.src).origin + "/api/event",
    s = t.getAttribute("data-domain");
  t.hasAttribute("data-allow-fetch");
  function p(t, e, a) {
    e && console.warn("Ignoring Event: " + e), a && a.callback && a.callback();
  }
  function e(t, e) {
    if (
      /^localhost$|^127(\.[0-9]+){0,2}\.[0-9]+$|^\[::1?\]$/.test(l.hostname) ||
      "file:" === l.protocol
    )
      return p(0, "localhost", e);
    if (
      (window._phantom ||
        window.__nightmare ||
        window.navigator.webdriver ||
        window.Cypress) &&
      !window.__plausible
    )
      return p(0, null, e);
    try {
      if ("true" === window.localStorage.plausible_ignore)
        return p(0, "localStorage flag", e);
    } catch (t) {}
    var a,
      n,
      i = {};
    (i.n = t),
      (i.u = l.href),
      (i.d = s),
      (i.r = r.referrer || null),
      e && e.meta && (i.m = JSON.stringify(e.meta)),
      e && e.props && (i.p = e.props),
      (t = o),
      (i = i),
      (a = e),
      (n = new XMLHttpRequest()).open("POST", t, !0),
      n.setRequestHeader("Content-Type", "text/plain"),
      n.send(JSON.stringify(i)),
      (n.onreadystatechange = function () {
        4 === n.readyState &&
          a &&
          a.callback &&
          a.callback({ status: n.status });
      });
  }
  var a = (window.plausible && window.plausible.q) || [];
  window.plausible = e;
  for (var n, i = 0; i < a.length; i++) e.apply(this, a[i]);
  function c() {
    n !== l.pathname && ((n = l.pathname), e("pageview"));
  }
  function u() {
    c();
  }
  var d,
    t = window.history;
  t.pushState &&
    ((d = t.pushState),
    (t.pushState = function () {
      d.apply(this, arguments), u();
    }),
    window.addEventListener("popstate", u)),
    "prerender" === r.visibilityState
      ? r.addEventListener("visibilitychange", function () {
          n || "visible" !== r.visibilityState || c();
        })
      : c();
  var f = 1;
  function w(t) {
    var e, a, n, i, r;
    function o() {
      n || ((n = !0), (window.location = a.href));
    }
    ("auxclick" === t.type && t.button !== f) ||
      ((e = (function (t) {
        for (
          ;
          t &&
          (void 0 === t.tagName ||
            !(e = t) ||
            !e.tagName ||
            "a" !== e.tagName.toLowerCase() ||
            !t.href);

        )
          t = t.parentNode;
        var e;
        return t;
      })(t.target)) &&
        e.href &&
        e.href.split("?")[0],
      (r = e) &&
        r.href &&
        r.host &&
        r.host !== l.host &&
        ((r = t),
        (t = { name: "Outbound Link: Click", props: { url: (a = e).href } }),
        (n = !1),
        !(function (t, e) {
          if (!t.defaultPrevented)
            return (
              (e = !e.target || e.target.match(/^_(self|parent|top)$/i)),
              (t =
                !(t.ctrlKey || t.metaKey || t.shiftKey) && "click" === t.type),
              e && t
            );
        })(r, a)
          ? ((i = { props: t.props }), plausible(t.name, i))
          : ((i = { props: t.props, callback: o }),
            plausible(t.name, i),
            setTimeout(o, 5e3),
            r.preventDefault())));
  }
  r.addEventListener("click", w), r.addEventListener("auxclick", w);
})();
