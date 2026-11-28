// Drives browser-page.mjs in headless Chromium. The page is served from
// http://127.0.0.1:9 (intercepted, no server) so every call to kord is a real cross-origin request.
const path = require('path');
const fs = require('fs');
const req = require('module').createRequire(path.join(process.env.FIREBASE_NODE_MODULES, '..', 'resolve.js'));
const { chromium } = req('playwright');
const [port, bundle] = [+process.argv[2], process.argv[3]];
(async () => {
  // Local Network Access blocks a page reaching loopback; production is
  // public https on both sides, so the check does not apply there.
  const browser = await chromium.launch({ args: ['--disable-features=LocalNetworkAccessChecks,PrivateNetworkAccessRespectPreflightResults,BlockInsecurePrivateNetworkRequests'] });
  const page = await browser.newPage();
  page.on('console', (m) => { if (m.type() === 'error' && !m.text().startsWith('Failed to load')) console.log('console:', m.text().slice(0, 300)); });
  await page.route('http://127.0.0.1:9/**', (r) => r.request().url().endsWith('/b.js')
    ? r.fulfill({ contentType: 'text/javascript', body: fs.readFileSync(bundle) })
    : r.fulfill({ contentType: 'text/html', body: '<!doctype html><script src="/b.js"></script>' }));
  await page.goto('http://127.0.0.1:9/');
  let failed = 0;
  for (const lp of [false, true]) {
    console.log(lp ? '-- long polling' : '-- streaming');
    const { log, fails } = await page.evaluate(([p, l]) => window.runKorBrowserTest(p, l), [port, lp]);
    log.forEach((l) => console.log(l));
    failed += fails.length;
  }
  await browser.close();
  console.log(failed ? `${failed} FAILED` : 'all passed');
  process.exit(failed ? 1 : 0);
})();
