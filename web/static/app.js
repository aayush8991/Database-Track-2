async function callApi(path, opts={}){
  // Attach Authorization header if token present
  const token = localStorage.getItem('dashboard_token');
  opts.headers = opts.headers || {};
  if(token){ opts.headers['Authorization'] = 'Bearer ' + token }
  const timeoutMs = Number(opts.timeoutMs || 30000);
  delete opts.timeoutMs;
  const controller = new AbortController();
  const timer = setTimeout(()=>controller.abort(), timeoutMs);
  opts.signal = controller.signal;
  let res;
  try{
    res = await fetch(path, opts);
    clearTimeout(timer);
  }catch(e){
    clearTimeout(timer);
    if(String(e).toLowerCase().includes('abort')){
      return {error:'timeout', detail:`Request timed out after ${timeoutMs} ms`};
    }
    return {error:'network-error', detail: String(e)};
  }
  try{
    const j=await res.json();
    if(j && typeof j === 'object'){ j._http_status = res.status; }
    // If token is invalid/expired, clear stale token and reset auth UI
    if(res.status === 401){
      localStorage.removeItem('dashboard_token');
      try{ setAuthUI(null); }catch(_e){}
    }
    return j;
  }catch(e){
    if(res.status === 401){
      localStorage.removeItem('dashboard_token');
      try{ setAuthUI(null); }catch(_e){}
    }
    return {error:'invalid-json',text:await res.text()}
  }
}

function showResult(title, obj){
  const el=document.getElementById('result');
  el.innerHTML=`<div class="card"><strong>${title}</strong><pre>${JSON.stringify(obj,null,2)}</pre></div>`;
}

function showHome(){
  const acid = document.getElementById('acid-section');
  const session = document.getElementById('session-section');
  const trace = document.getElementById('trace-section');
  const json = document.getElementById('json-panel');
  if(acid) acid.style.display = 'none';
  if(session) session.style.display = 'none';
  if(trace) trace.style.display = 'none';
  if(json) json.style.display = 'none';
  showResult('Home', {message:'Select a sidebar action: ACID Testing, JSON Query, Docs, or entity views.'});
}

function openAcidSection(){
  const acid = document.getElementById('acid-section');
  const session = document.getElementById('session-section');
  const trace = document.getElementById('trace-section');
  const json = document.getElementById('json-panel');
  if(acid) acid.style.display = 'block';
  if(session) session.style.display = 'none';
  if(trace) trace.style.display = 'none';
  if(json) json.style.display = 'none';
  showResult('ACID Testing', {status:'ready', note:'Run an individual property test or Run All.'});
  if(acid) acid.scrollIntoView({behavior:'smooth', block:'start'});
}

function openJsonPanel(){
  const acid = document.getElementById('acid-section');
  const session = document.getElementById('session-section');
  const trace = document.getElementById('trace-section');
  const json = document.getElementById('json-panel');
  if(acid) acid.style.display = 'none';
  if(session) session.style.display = 'none';
  if(trace) trace.style.display = 'none';
  if(json) json.style.display = 'block';
  showResult('JSON Query', {status:'ready', hint:'Paste JSON and click Run Query.'});
  if(json){
    json.scrollIntoView({behavior:'smooth', block:'start'});
    setTimeout(()=>document.getElementById('json-input')?.focus(), 50);
  }
}

function toTable(headers, rows){
  let html = '<table class="acid-table compact"><thead><tr>';
  for(const h of headers) html += `<th>${h}</th>`;
  html += '</tr></thead><tbody>';
  for(const row of rows){
    html += '<tr>';
    for(const h of headers){
      const v = row?.[h];
      html += `<td>${typeof v === 'object' ? JSON.stringify(v) : String(v ?? '')}</td>`;
    }
    html += '</tr>';
  }
  html += '</tbody></table>';
  return html;
}

async function refreshSessionMonitor(){
  const out = await callApi('/api/session-monitor?limit=50');
  const summary = document.getElementById('session-summary');
  const activeWrap = document.getElementById('session-active-wrap');
  const callsWrap = document.getElementById('session-calls-wrap');
  if(!summary || !activeWrap || !callsWrap){
    showResult('Session Monitor', out);
    return;
  }

  if(out && out.detail){
    summary.textContent = `Error: ${out.detail}`;
    activeWrap.innerHTML = '';
    callsWrap.innerHTML = '';
    return;
  }

  const active = out.active_sessions || [];
  const calls = out.recent_calls || [];
  summary.textContent = `Active sessions: ${active.length} · Recent calls shown: ${calls.length}`;

  const activeHeaders = ['username','role','auth_type','first_seen','last_seen','expires_in_sec'];
  const callHeaders = ['ts','username','method','path','status_code','duration_ms'];
  activeWrap.innerHTML = active.length ? toTable(activeHeaders, active) : '<div class="muted">No active sessions.</div>';
  callsWrap.innerHTML = calls.length ? toTable(callHeaders, calls) : '<div class="muted">No recent calls.</div>';
}

async function refreshQueryTrace(){
  const out = await callApi('/api/query-trace?limit=50');
  const summary = document.getElementById('trace-summary');
  const wrap = document.getElementById('trace-wrap');
  if(!summary || !wrap){
    showResult('Query Trace', out);
    return;
  }

  if(out && out.detail){
    summary.textContent = `Error: ${out.detail}`;
    wrap.innerHTML = '';
    return;
  }

  const traces = out.traces || [];
  summary.textContent = `Recent traces shown: ${traces.length}`;
  const rows = traces.map(t => ({
    ts: t.ts,
    username: t.username,
    endpoint: t.endpoint,
    operation: t.operation,
    routed_backends: Array.isArray(t.routed_backends) ? t.routed_backends.join('+') : String(t.routed_backends || ''),
    summary: t.summary,
    duration_ms: t.duration_ms,
    status: t.status,
    result_count: t.result_count ?? '',
    error: t.error ?? ''
  }));
  const headers = ['ts','username','endpoint','operation','routed_backends','summary','duration_ms','status','result_count','error'];
  wrap.innerHTML = rows.length ? toTable(headers, rows) : '<div class="muted">No trace data yet.</div>';
}

function openSessionSection(){
  const acid = document.getElementById('acid-section');
  const session = document.getElementById('session-section');
  const trace = document.getElementById('trace-section');
  const json = document.getElementById('json-panel');
  if(acid) acid.style.display = 'none';
  if(session) session.style.display = 'block';
  if(trace) trace.style.display = 'none';
  if(json) json.style.display = 'none';
  showResult('Session Monitor', {status:'loading'});
  if(session) session.scrollIntoView({behavior:'smooth', block:'start'});
  refreshSessionMonitor();
}

function openTraceSection(){
  const acid = document.getElementById('acid-section');
  const session = document.getElementById('session-section');
  const trace = document.getElementById('trace-section');
  const json = document.getElementById('json-panel');
  if(acid) acid.style.display = 'none';
  if(session) session.style.display = 'none';
  if(trace) trace.style.display = 'block';
  if(json) json.style.display = 'none';
  showResult('Query Trace', {status:'loading'});
  if(trace) trace.scrollIntoView({behavior:'smooth', block:'start'});
  refreshQueryTrace();
}

function setAcidStatus(property, statusText){
  const badge = document.getElementById(`acid-status-${property}`);
  if(!badge) return;
  const s = String(statusText || '').toUpperCase();
  badge.className = 'acid-badge';
  if(s === 'PASS') badge.classList.add('pass');
  if(s === 'FAIL') badge.classList.add('fail');
  if(s === 'RUNNING') badge.classList.add('running');
  badge.textContent = s || 'UNKNOWN';
}

function setAcidBody(property, lines){
  const body = document.getElementById(`acid-body-${property}`);
  if(!body) return;
  if(Array.isArray(lines)){
    body.innerHTML = lines.map(x => `<div>${x}</div>`).join('');
  } else {
    body.textContent = String(lines || '');
  }
}

function summarizeDetails(details){
  if(!details || typeof details !== 'object') return ['No details'];
  const out = [];
  for(const k of Object.keys(details).slice(0,3)){
    const v = details[k];
    out.push(`${k}: ${typeof v === 'object' ? JSON.stringify(v).slice(0,90) : String(v)}`);
  }
  return out.length ? out : ['No details'];
}

function renderAcidSingle(property, payload){
  const status = payload?.status || (payload?.passed ? 'PASS' : 'FAIL');
  setAcidStatus(property, status);
  if(payload?.details && typeof payload.details === 'object'){
    setAcidBody(property, summarizeDetails(payload.details));
    return;
  }

  const lines = [];
  if(payload?.detail) lines.push(`detail: ${payload.detail}`);
  if(payload?.error) lines.push(`error: ${payload.error}`);
  if(payload?._http_status) lines.push(`http_status: ${payload._http_status}`);
  setAcidBody(property, lines.length ? lines : ['No details']);
}

async function runAcidProperty(property, path){
  setAcidStatus(property, 'RUNNING');
  setAcidBody(property, 'Running...');
  const timeoutMap = {
    atomicity: 45000,
    consistency: 45000,
    isolation: 70000,
    durability: 70000,
  };
  const r = await callApi(path, {method:'POST', timeoutMs: timeoutMap[property] || 45000});
  renderAcidSingle(property, r);
  showResult(`${property} test`, r);
}

document.getElementById('acid-run-atomicity').addEventListener('click', async ()=>{
  await runAcidProperty('atomicity', '/api/tools/acid/atomicity');
});

document.getElementById('acid-run-consistency').addEventListener('click', async ()=>{
  await runAcidProperty('consistency', '/api/tools/acid/consistency');
});

document.getElementById('acid-run-isolation').addEventListener('click', async ()=>{
  await runAcidProperty('isolation', '/api/tools/acid/isolation');
});

document.getElementById('acid-run-durability').addEventListener('click', async ()=>{
  await runAcidProperty('durability', '/api/tools/acid/durability');
});

document.getElementById('acid-run-all').addEventListener('click', async ()=>{
  const summary = document.getElementById('acid-all-summary');
  setAcidStatus('atomicity', 'RUNNING');
  setAcidStatus('consistency', 'RUNNING');
  setAcidStatus('isolation', 'RUNNING');
  setAcidStatus('durability', 'RUNNING');
  if(summary) summary.textContent = 'Running all tests...';

  const r = await callApi('/api/tools/acid/all', {method:'POST', timeoutMs: 45000});
  if(r && Array.isArray(r.results)){
    for(const item of r.results){
      const t = String(item?.test || '').toLowerCase();
      if(['atomicity','consistency','isolation','durability'].includes(t)){
        renderAcidSingle(t, item);
      }
    }
    const s = r.summary || {};
    if(summary) summary.textContent = `Overall: ${r.status || 'UNKNOWN'} · Passed ${s.passed_count ?? 0}/${s.total_count ?? 0} · ${s.duration_ms ?? 0} ms`;
    showResult('All ACID tests', {status: r.status, passed: s.passed_count, total: s.total_count});
  } else {
    if(summary) summary.textContent = 'Run failed.';
    showResult('All ACID tests failed', r);
  }
});

document.getElementById('acid-run-failure-injection').addEventListener('click', async ()=>{
  const summary = document.getElementById('acid-all-summary');
  if(summary) summary.textContent = 'Running failure-injection scenarios...';
  const r = await callApi('/api/tools/acid/failure-injection', {method:'POST', timeoutMs: 50000});
  if(summary){
    const total = r?.details?.total_count ?? 0;
    const passed = r?.details?.passed_count ?? 0;
    summary.textContent = `Failure Injection: ${r?.status || 'UNKNOWN'} · Passed ${passed}/${total}`;
  }
  showResult('Failure Injection Result', r);
});

document.getElementById('btn-acid-open').addEventListener('click', ()=>{
  openAcidSection();
});

document.getElementById('btn-session-open').addEventListener('click', ()=>{
  openSessionSection();
});

document.getElementById('btn-trace-open').addEventListener('click', ()=>{
  openTraceSection();
});

document.getElementById('session-refresh').addEventListener('click', async ()=>{
  await refreshSessionMonitor();
});

document.getElementById('trace-refresh').addEventListener('click', async ()=>{
  await refreshQueryTrace();
});

// Auth: login/logout behavior
function setAuthUI(username){
  if(username){
    document.getElementById('auth-who').textContent = username;
    document.getElementById('auth-login').style.display = 'none';
    document.getElementById('auth-logout').style.display = 'inline-block';
  } else {
    document.getElementById('auth-who').textContent = '';
    document.getElementById('auth-login').style.display = 'inline-block';
    document.getElementById('auth-logout').style.display = 'none';
  }
}

document.getElementById('auth-login').addEventListener('click', async ()=>{
  const user = document.getElementById('auth-user').value.trim();
  const pass = document.getElementById('auth-pass').value;
  if(!user || !pass){ alert('enter user and pass'); return }
  const r = await callApi('/api/login', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({username:user,password:pass})});
  if(r && r.token){
    localStorage.setItem('dashboard_token', r.token);
    setAuthUI(r.username);
    showResult('Login', {ok:true, user:r.username, role:r.role});
  } else {
    showResult('Login failed', r);
  }
});

document.getElementById('auth-logout').addEventListener('click', ()=>{
  localStorage.removeItem('dashboard_token');
  setAuthUI(null);
  showResult('Logged out', {});
});

// Toggle password visibility
const toggleBtn = document.getElementById('auth-toggle');
if(toggleBtn){
  toggleBtn.addEventListener('click', ()=>{
    const pw = document.getElementById('auth-pass');
    if(pw.type === 'password'){
      pw.type = 'text'; toggleBtn.textContent = 'Hide';
    } else {
      pw.type = 'password'; toggleBtn.textContent = 'Show';
    }
    pw.focus();
  });
}

// Delegated handler fallback: ensure toggle works even if above attach failed
document.addEventListener('click', (ev)=>{
  const t = ev.target;
  if(t && t.id === 'auth-toggle'){
    const pw = document.getElementById('auth-pass');
    if(!pw) return;
    if(pw.type === 'password'){
      pw.type = 'text'; t.textContent = 'Hide';
    } else { pw.type = 'password'; t.textContent = 'Show'; }
    pw.focus();
  }
});

// Initialize auth UI from stored token
try{
  const tok = localStorage.getItem('dashboard_token');
  if(tok){
    // Do not assume token is valid; backend will clear UI on first 401
    setAuthUI('session');
  } else {
    setAuthUI(null);
  }
}catch(e){ setAuthUI(null) }
// Show inline JSON query panel (supports paste/copy)
document.getElementById('btn-json').addEventListener('click', async ()=>{
  openJsonPanel();
});

// Run JSON query from panel
document.getElementById('json-run').addEventListener('click', async ()=>{
  const txt = document.getElementById('json-input').value.trim();
  if(!txt){ alert('Please enter a JSON query'); return }
  let qobj;
  try{ qobj = JSON.parse(txt) }catch(e){ alert('Invalid JSON: '+e.message); return }

  // Logical query payloads should go to /query
  if(qobj && typeof qobj === 'object' && qobj.operation && qobj.entity){
    showResult('Running Logical Query', {status:'running'});
    const r = await callApi('/query', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify(qobj)
    });
    showResult('Logical Query Result', r);
    document.getElementById('result')?.scrollIntoView({behavior:'smooth', block:'start'});
    return;
  }

  const coll = document.getElementById('json-collection').value.trim() || 'unstructured_data';
  const lim = parseInt(document.getElementById('json-limit').value) || 50;
  showResult('Running JSON Query', {collection:coll, query:qobj, limit:lim});

  const r = await callApi('/api/tools/json-query',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({collection:coll, query:qobj, limit:lim})
  });
  showResult('JSON Query Result', r);
  document.getElementById('result')?.scrollIntoView({behavior:'smooth', block:'start'});
});

// Preview JSON query (estimated/actual count)
document.getElementById('json-preview').addEventListener('click', async ()=>{
  const txt = document.getElementById('json-input').value.trim();
  if(!txt){ alert('Please enter a JSON query'); return }
  let qobj;
  try{ qobj = JSON.parse(txt) }catch(e){ alert('Invalid JSON: '+e.message); return }

  if(qobj && typeof qobj === 'object' && qobj.operation && qobj.entity){
    showResult('Logical Query Preview', {
      info:'Preview endpoint is for raw Mongo queries. For logical payloads, click Run Query to execute /query.'
    });
    document.getElementById('result')?.scrollIntoView({behavior:'smooth', block:'start'});
    return;
  }

  const coll = document.getElementById('json-collection').value.trim() || 'unstructured_data';
  const lim = parseInt(document.getElementById('json-limit').value) || 50;
  showResult('Query Preview', {collection:coll, query:qobj});
  const r = await callApi('/api/tools/json-query-preview',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({collection:coll, query:qobj, limit:lim})
  });
  showResult('Query Preview Result', r);
  document.getElementById('result')?.scrollIntoView({behavior:'smooth', block:'start'});
});

// Clear textarea
document.getElementById('json-clear').addEventListener('click', ()=>{
  document.getElementById('json-input').value = '';
  document.getElementById('json-input').focus();
});

// Pretty-print on paste (helps with copy/paste messy JSON)
document.getElementById('json-input').addEventListener('paste', (ev)=>{
  // Allow paste to happen, then format
  setTimeout(()=>{
    const v = document.getElementById('json-input').value;
    try{
      const o = JSON.parse(v);
      document.getElementById('json-input').value = JSON.stringify(o, null, 2);
    }catch(e){ /* ignore - user may paste non-json */ }
  }, 50);
});

document.getElementById('btn-docs').addEventListener('click', async ()=>{
  const r = await callApi('/api/tools/docs');
  showResult('Documentation', r);
});

document.querySelectorAll('.nav-btn').forEach(btn=>btn.addEventListener('click', async (e)=>{
  const name=e.target.dataset.view; showResult(name, {loading:true});
  if(name === 'home'){
    showHome();
    return;
  }
  // non-home nav opens main result area views, hide tool sections
  const acid = document.getElementById('acid-section');
  const session = document.getElementById('session-section');
  const trace = document.getElementById('trace-section');
  const json = document.getElementById('json-panel');
  if(acid) acid.style.display = 'none';
  if(session) session.style.display = 'none';
  if(trace) trace.style.display = 'none';
  if(json) json.style.display = 'none';
  // fetch list placeholders
  const maps={'logs':'/api/logs','users':'/api/users','alerts':'/api/alerts'};
  // entity viewer for logical entities
  const entityViews = new Set(['readings','sensors','users','alerts']);
  if(entityViews.has(name)){
    // build a logical query to POST /query
    const q = { operation: 'read', entity: name };
    const r = await callApi('/query', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(q)});
    renderEntityResults(name, r.results || []);
    return;
  }
  const r = await callApi(maps[name]); showResult(name, r);
}));


function renderEntityResults(entity, rows){
  const el=document.getElementById('result');
  if(!Array.isArray(rows)){
    el.innerHTML = `<div class="card"><strong>${entity}</strong><pre>${JSON.stringify(rows,null,2)}</pre></div>`;
    return;
  }
  if(rows.length === 0){
    el.innerHTML = `<div class="card"><strong>${entity}</strong><div class="muted">No results</div></div>`;
    return;
  }
  // build columns from keys of first row
  const cols = Object.keys(rows[0]);
  let html = `<div class="card"><strong>${entity}</strong><table class="table"><thead><tr>`;
  for(const c of cols) html += `<th>${c}</th>`;
  html += `</tr></thead><tbody>`;
  for(const r of rows){
    html += '<tr>';
    for(const c of cols){
      let v = r[c];
      if(typeof v === 'object') v = JSON.stringify(v);
      html += `<td>${String(v ?? '')}</td>`;
    }
    html += '</tr>';
  }
  html += `</tbody></table></div>`;
  el.innerHTML = html;
}

// ====== Logical Query Builder ======

function openLogicalQueryPanel(){
  const acid = document.getElementById('acid-section');
  const session = document.getElementById('session-section');
  const trace = document.getElementById('trace-section');
  const json = document.getElementById('json-panel');
  const logical = document.getElementById('logical-query-panel');
  
  if(acid) acid.style.display = 'none';
  if(session) session.style.display = 'none';
  if(trace) trace.style.display = 'none';
  if(json) json.style.display = 'none';
  if(logical) logical.style.display = 'block';
  
  showResult('Logical Query Builder', {status:'ready', hint:'Build and execute queries without knowing about backends (SQL/MongoDB).'});
  if(logical) logical.scrollIntoView({behavior:'smooth', block:'start'});
}

function updateLogicalQuerySections(){
  const op = (document.getElementById('lq-operation')?.value || '').toLowerCase();
  const filterLabel = document.getElementById('lq-filter-label');
  const filterText = document.getElementById('lq-filter');
  const dataLabel = document.getElementById('lq-data-label');
  const dataText = document.getElementById('lq-data');

  if(op === 'insert') {
    if(filterLabel) filterLabel.style.display = 'none';
    if(filterText) filterText.style.display = 'none';
    if(dataLabel) dataLabel.style.display = 'block';
    if(dataText) dataText.style.display = 'block';
  } else if(op === 'read' || op === 'delete') {
    if(filterLabel) filterLabel.style.display = 'block';
    if(filterText) filterText.style.display = 'block';
    if(dataLabel) dataLabel.style.display = 'none';
    if(dataText) dataText.style.display = 'none';
  } else if(op === 'update') {
    if(filterLabel) filterLabel.style.display = 'block';
    if(filterText) filterText.style.display = 'block';
    if(dataLabel) dataLabel.style.display = 'block';
    if(dataText) dataText.style.display = 'block';
  }
}

async function executeLogicalQuery(){
  const operation = (document.getElementById('lq-operation')?.value || 'read').toLowerCase();
  const status = document.getElementById('lq-status');
  const resultsDiv = document.getElementById('lq-results');
  const resultsContent = document.getElementById('lq-results-content');
  
  let payload = { operation };
  
  const filterText = document.getElementById('lq-filter')?.value?.trim();
  if (filterText && (operation === 'read' || operation === 'delete' || operation === 'update')) {
    try {
      payload.filter = JSON.parse(filterText);
    } catch(e) {
      if(status) status.textContent = `Error parsing filter JSON: ${e.message}`;
      return;
    }
  }

  const dataText = document.getElementById('lq-data')?.value?.trim();
  if (dataText && (operation === 'insert' || operation === 'update')) {
    try {
      payload.data = JSON.parse(dataText);
    } catch(e) {
      if(status) status.textContent = `Error parsing data JSON: ${e.message}`;
      return;
    }
  }
  
  if(status) status.textContent = 'Executing...';
  
  const result = await callApi('/query-crud', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  
  if(status) status.textContent = `Completed (${result._http_status || 'unknown'})`;
  
  // Format results
  if(resultsDiv) resultsDiv.style.display = 'block';
  
  if(resultsContent) resultsContent.textContent = JSON.stringify(result, null, 2);
}

function formatResultsAsTable(rows){
  if(!Array.isArray(rows) || rows.length === 0){
    return '<div class="muted">No results found</div>';
  }
  
  // Get all unique columns from all rows
  const columns = new Set();
  for(const row of rows){
    if(typeof row === 'object'){
      for(const key of Object.keys(row)){
        columns.add(key);
      }
    }
  }
  const cols = Array.from(columns).sort();
  
  // Build table
  let html = '<table class="results-table" style="width:100%;border-collapse:collapse;margin-top:8px">';
  
  // Header
  html += '<thead style="background:rgba(255,255,255,0.05);border-bottom:2px solid rgba(255,255,255,0.1)">';
  html += '<tr>';
  for(const col of cols){
    html += `<th style="padding:12px;text-align:left;font-weight:600;border-right:1px solid rgba(255,255,255,0.05)">${escapeHtml(col)}</th>`;
  }
  html += '</tr>';
  html += '</thead>';
  
  // Body
  html += '<tbody>';
  for(let i = 0; i < rows.length; i++){
    const row = rows[i];
    html += `<tr style="border-bottom:1px solid rgba(255,255,255,0.05);${i % 2 === 0 ? 'background:rgba(255,255,255,0.02)' : ''}">`;
    for(const col of cols){
      let value = row[col];
      if(value === null || value === undefined){
        value = '<span class="muted">null</span>';
      } else if(typeof value === 'object'){
        value = `<code style="background:rgba(0,0,0,0.3);padding:2px 4px;border-radius:3px;font-size:0.85em">${escapeHtml(JSON.stringify(value))}</code>`;
      } else {
        value = escapeHtml(String(value));
      }
      html += `<td style="padding:12px;border-right:1px solid rgba(255,255,255,0.05);font-size:0.95em">${value}</td>`;
    }
    html += '</tr>';
  }
  html += '</tbody>';
  html += '</table>';
  
  return html;
}

function escapeHtml(text){
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function previewLogicalQuery(){
  const operation = (document.getElementById('lq-operation')?.value || 'read').toLowerCase();
  
  let payload = { operation };

  const filterText = document.getElementById('lq-filter')?.value?.trim();
  if (filterText && (operation === 'read' || operation === 'delete' || operation === 'update')) {
    try {
      payload.filter = JSON.parse(filterText);
    } catch(e) {
      alert(`Error parsing filter JSON: ${e.message}`);
      return;
    }
  }

  const dataText = document.getElementById('lq-data')?.value?.trim();
  if (dataText && (operation === 'insert' || operation === 'update')) {
    try {
      payload.data = JSON.parse(dataText);
    } catch(e) {
      alert(`Error parsing data JSON: ${e.message}`);
      return;
    }
  }
  
  alert('Query payload to be sent:\n\n' + JSON.stringify(payload, null, 2));
}

// Attach event listeners
document.addEventListener('DOMContentLoaded', ()=>{
  // Logical Query Builder
  const btnLogical = document.getElementById('btn-logical-query');
  if(btnLogical) btnLogical.addEventListener('click', openLogicalQueryPanel);
  
  const opSelect = document.getElementById('lq-operation');
  if(opSelect) opSelect.addEventListener('change', updateLogicalQuerySections);
  
  const execBtn = document.getElementById('lq-execute');
  if(execBtn) execBtn.addEventListener('click', executeLogicalQuery);
  
  const previewBtn = document.getElementById('lq-preview');
  if(previewBtn) previewBtn.addEventListener('click', previewLogicalQuery);
  
  const clearBtn = document.getElementById('lq-clear');
  if(clearBtn) clearBtn.addEventListener('click', ()=>{
    document.getElementById('lq-filter').value = '';
    document.getElementById('lq-data').value = '';
    document.getElementById('lq-results').style.display = 'none';
    document.getElementById('lq-status').textContent = '';
  });
});

// Default landing view
showHome();
