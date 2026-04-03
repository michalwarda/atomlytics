const endpoint = process.env.SEED_ENDPOINT ?? "http://app:3000/api/event";
const domain = process.env.DEV_SEED_DOMAIN ?? "demo.atomlytics.localhost";

const pages = [
  "/",
  "/pricing",
  "/docs",
  "/docs/leptos",
  "/docs/postgresql",
  "/blog/fresh-ui",
  "/integrations",
  "/contact",
];

const referrers = [
  null,
  "https://www.google.com/search?q=atomlytics",
  "https://news.ycombinator.com/",
  "https://twitter.com/openai/status/1",
  "https://www.linkedin.com/feed/",
];

const eventNames = [
  "signup_clicked",
  "docs_search",
  "cta_pressed",
  "pricing_toggled",
  "contact_opened",
];

const userAgents = [
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
  "Mozilla/5.0 (iPad; CPU OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
];

const locations = [
  { country: "United States", region: "California", city: "San Francisco" },
  { country: "Germany", region: "Berlin", city: "Berlin" },
  { country: "Poland", region: "Mazovia", city: "Warsaw" },
  { country: "Japan", region: "Tokyo", city: "Tokyo" },
  { country: "Brazil", region: "Sao Paulo", city: "Sao Paulo" },
];

const visitors = Array.from({ length: 28 }, (_, index) => ({
  ip: `203.0.113.${20 + index}`,
  userAgent: userAgents[index % userAgents.length],
  page: pages[index % pages.length],
  referrer: referrers[index % referrers.length],
  location: locations[index % locations.length],
}));

function randomItem(values) {
  return values[Math.floor(Math.random() * values.length)];
}

function randomDelay() {
  return 500 + Math.floor(Math.random() * 1400);
}

function pageUrl(path) {
  const query =
    Math.random() > 0.65
      ? "?utm_source=dev-seed&utm_medium=compose&utm_campaign=realtime-demo"
      : "";
  return `https://${domain}${path}${query}`;
}

async function sendEvent(visitor, payload) {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "user-agent": visitor.userAgent,
      "x-forwarded-for": visitor.ip,
      "x-atomlytics-demo-country": visitor.location.country,
      "x-atomlytics-demo-region": visitor.location.region,
      "x-atomlytics-demo-city": visitor.location.city,
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`seed request failed: ${response.status} ${body}`);
  }
}

async function waitForApp() {
  while (true) {
    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          n: "pageview",
          u: pageUrl("/"),
          r: null,
          p: { bootstrap: true },
        }),
      });
      if (response.ok || response.status === 400 || response.status === 500) {
        return;
      }
    } catch (_) {
      // ignore until the app is reachable
    }

    await new Promise((resolve) => setTimeout(resolve, 1500));
  }
}

async function loop() {
  await waitForApp();
  console.log(`Seeding realtime events for ${domain} via ${endpoint}`);

  while (true) {
    const visitor = randomItem(visitors);
    const sendPageview = Math.random() > 0.35;

    if (sendPageview) {
      visitor.page = randomItem(pages);
      visitor.referrer = randomItem(referrers);
      await sendEvent(visitor, {
        n: "pageview",
        u: pageUrl(visitor.page),
        r: visitor.referrer,
        p: {
          seeded: true,
          layout: randomItem(["grid", "split", "stacked"]),
        },
      });
    } else {
      await sendEvent(visitor, {
        n: randomItem(eventNames),
        u: pageUrl(visitor.page),
        r: visitor.referrer,
        p: {
          seeded: true,
          tier: randomItem(["free", "pro", "team"]),
          source: "dev-compose",
        },
      });
    }

    await new Promise((resolve) => setTimeout(resolve, randomDelay()));
  }
}

loop().catch((error) => {
  console.error(error);
  process.exit(1);
});
