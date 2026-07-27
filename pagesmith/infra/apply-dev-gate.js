#!/usr/bin/env node
// Turn a built staticwebapp.config.json into the DEV (private) variant: a login wall.
// Every route requires the `editor` role; `/login.html` is the only public route; an
// unauthorized request (401) is redirected to that front door.
//
// The DEV Static Web App deploys with this applied; the PROD app uses the config as-is
// (public). Same source files, config specialised per-site at deploy time:
//
//   cp -r app /tmp/app-deploy && <inject __STORAGE_ACCOUNT__>
//   node infra/apply-dev-gate.js /tmp/app-deploy/staticwebapp.config.json   # DEV only
//   swa deploy /tmp/app-deploy --api-location ./api ... --deployment-token <DEV token>
//
// (Invite-accepted == `editor` role == gets past the wall == allowed to edit.)
const fs = require('fs');
const f = process.argv[2];
if (!f) { console.error('usage: node apply-dev-gate.js <staticwebapp.config.json>'); process.exit(1); }
const c = JSON.parse(fs.readFileSync(f, 'utf8'));
c.routes = (c.routes || []).map(r => r.statusCode ? r : Object.assign({}, r, { allowedRoles: ['editor'] })); // keep pure status-code rules (e.g. the GitHub-login 404 block) as-is
c.routes.unshift({ route: '/login.html', allowedRoles: ['anonymous', 'authenticated'] });
c.routes.push({ route: '/*', allowedRoles: ['editor'] });
c.responseOverrides = Object.assign({}, c.responseOverrides, { '401': { statusCode: 302, redirect: '/login.html' } });
fs.writeFileSync(f, JSON.stringify(c, null, 2));
console.log('[dev-gate] all routes → editor; public front door /login.html; 401 → /login.html');
