#!/usr/bin/env node
'use strict';

// kor-functions — run a firebase-functions v2 codebase against Kor.
//
//   kor-functions --source ./functions --project my-proj [--list]
//                 [--feed http://127.0.0.1:6566] [--kor 127.0.0.1:6565]
//                 [--state ./.kor-functions.json] [--only a,b*] [--except c]
//                 [--hybrid users,posts | KOR_COLLECTIONS=users,posts]
//                 [--no-triggers] [--no-schedules] [--metrics 127.0.0.1:6567]
//
// The codebase is loaded exactly as Cloud Functions would load it (its
// package.json "main"). Nothing in it changes.

const http = require('node:http');
const path = require('node:path');
const { createRequire } = require('node:module');
const { discover, select } = require('../lib/discover');
const { Runtime } = require('../lib/runtime');
const hybrid = require('../lib/hybrid');

function parseArgs(argv) {
  const o = {
    feed: 'http://127.0.0.1:6566', kor: '127.0.0.1:6565', only: [], except: [],
    triggers: true, schedules: true, list: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => {
      if (i + 1 >= argv.length) throw new Error(`${a} needs a value`);
      return argv[++i];
    };
    const list = (s) => s.split(',').map((x) => x.trim()).filter(Boolean);
    switch (a) {
      case '--source': o.source = next(); break;
      case '--project': o.project = next(); break;
      case '--feed': o.feed = next(); break;
      case '--kor': o.kor = next(); break;
      case '--state': o.state = next(); break;
      case '--only': o.only.push(...list(next())); break;
      case '--except': o.except.push(...list(next())); break;
      case '--hybrid': o.hybrid = list(next()); break;
      case '--metrics': o.metrics = next(); break;
      case '--concurrency': o.concurrency = Number(next()); break;
      case '--no-triggers': o.triggers = false; break;
      case '--no-schedules': o.schedules = false; break;
      case '--list': o.list = true; break;
      case '-h': case '--help': o.help = true; break;
      default: throw new Error(`unknown argument ${a}`);
    }
  }
  return o;
}

const USAGE = `usage: kor-functions --source <functions dir> --project <id> [options]

  --list                 print the functions found and how each would be served, then exit
  --feed URL             kord admin listener (default http://127.0.0.1:6566)
  --kor HOST:PORT        kord gRPC, for --hybrid (default 127.0.0.1:6565)
  --state FILE           cursor + schedule bookkeeping (default <source>/.kor-functions.json)
  --only a,b*            serve only these functions (trailing * = prefix)
  --except a,b*          serve all but these
  --hybrid a,b           collections Kor owns; the rest stay on Google Firestore
                         (default: $KOR_COLLECTIONS; unset = no routing, SDK unchanged)
  --no-triggers          schedules only
  --no-schedules         triggers only
  --concurrency N        documents handled in parallel (default 8)
  --metrics HOST:PORT    serve /metrics and /healthz (loopback)`;

async function main() {
  const o = parseArgs(process.argv.slice(2));
  if (o.help || !o.source) {
    console.log(USAGE);
    process.exit(o.help ? 0 : 2);
  }
  const source = path.resolve(o.source);
  const req = createRequire(path.join(source, 'package.json'));
  const pkg = req('./package.json');
  if (o.project) {
    process.env.GCLOUD_PROJECT = process.env.GCLOUD_PROJECT || o.project;
    process.env.GOOGLE_CLOUD_PROJECT = process.env.GOOGLE_CLOUD_PROJECT || o.project;
  }
  const project = o.project || process.env.GCLOUD_PROJECT || process.env.GOOGLE_CLOUD_PROJECT;
  if (!project && !o.list) throw new Error('--project is required (or GCLOUD_PROJECT)');
  // firebase-functions reads this to decide it is not being analysed by the deploy CLI.
  process.env.FUNCTIONS_CONTROL_API = process.env.FUNCTIONS_CONTROL_API || 'false';

  const mod = req(path.resolve(source, pkg.main || 'index.js'));
  const all = discover(mod);
  const chosen = select(all, { only: o.only, except: o.except });

  if (o.list) {
    for (const d of all) {
      const on = chosen.includes(d);
      let how;
      if (d.type === 'firestore') how = `firestore ${d.kind} ${d.document}${d.retry ? ' (retry)' : ''}`;
      else if (d.type === 'schedule') how = `schedule "${d.schedule}" ${d.timeZone}`;
      else how = `NOT SUPPORTED (${d.kind}) — keep it on Cloud Functions`;
      console.log(`${on && d.type !== 'unsupported' ? '✓' : ' '} ${d.name.padEnd(44)} ${how}`);
    }
    return;
  }

  const unsupported = chosen.filter((d) => d.type === 'unsupported');
  if (unsupported.length && (o.only.length)) {
    throw new Error(`cannot serve: ${unsupported.map((d) => `${d.name} (${d.kind})`).join(', ')}`);
  }
  const served = chosen.filter((d) => d.type !== 'unsupported');
  if (!served.length) throw new Error('no servable functions selected');

  const owned = o.hybrid || (process.env.KOR_COLLECTIONS || '').split(',').map((s) => s.trim()).filter(Boolean);
  let router = null;
  if (owned.length) {
    const { getApps, initializeApp, getApp } = req('firebase-admin/app');
    const { getFirestore, Firestore } = req('firebase-admin/firestore');
    if (!getApps().length) initializeApp();
    const google = getFirestore(getApp());
    const [host, port] = o.kor.split(':');
    const kor = new Firestore({ projectId: project, host, port: Number(port || 6565), ssl: false, preferRest: false });
    router = hybrid.install(google, kor, {
      collections: owned,
      log: (msg) => process.stderr.write(JSON.stringify({ time: new Date().toISOString(), level: 'warn', msg }) + '\n'),
    });
  }

  const rt = new Runtime({
    feedUrl: o.feed,
    stateFile: path.resolve(o.state || path.join(source, '.kor-functions.json')),
    project,
    functions: served,
    concurrency: o.concurrency,
    triggers: o.triggers,
    schedules: o.schedules,
  });

  if (o.metrics) serveMetrics(o.metrics, rt, router);
  let stopping = false;
  for (const sig of ['SIGTERM', 'SIGINT']) {
    process.on(sig, () => {
      if (stopping) process.exit(1);
      stopping = true;
      rt.stop();
      // In-flight handlers get a grace period; the cursor was saved per page.
      setTimeout(() => process.exit(0), 25000).unref();
    });
  }
  await rt.start();
  process.exit(0);
}

function serveMetrics(addr, rt, router) {
  const [host, port] = addr.split(':');
  http.createServer((req, res) => {
    if (req.url === '/healthz') {
      res.writeHead(200).end('ok\n');
      return;
    }
    if (req.url !== '/metrics') {
      res.writeHead(404).end();
      return;
    }
    const m = rt.metrics;
    const lines = [
      `kor_functions_events_total ${m.events}`,
      `kor_functions_invocations_total ${m.invocations}`,
      `kor_functions_failures_total ${m.failures}`,
      `kor_functions_events_without_images_total ${m.skippedNoImages}`,
      `kor_functions_scheduled_runs_total ${m.scheduled}`,
      `kor_functions_feed_lag_seconds ${m.lagSeconds}`,
    ];
    if (router) {
      for (const [k, v] of Object.entries(router.stats)) lines.push(`kor_functions_hybrid_requests_total{target="${k}"} ${v}`);
    }
    res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' }).end(lines.join('\n') + '\n');
  }).listen(Number(port), host || '127.0.0.1');
}

main().catch((e) => {
  console.error(`kor-functions: ${e && e.stack ? e.stack : e}`);
  process.exit(1);
});
