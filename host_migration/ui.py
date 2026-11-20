"""Per-node scene window: stdlib HTTP server + canvas page.

Shows host identity, epoch, migration phase, the live scene (players,
platform, door, gear), and a result area with session-continuity proof.
Controls are driven by the migration kernel (node.ui_action); failures are
returned and logged back into kernel events.
"""

from __future__ import annotations

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PAGE = """<!doctype html>
<html><head><meta charset=utf-8><title>host-migration node</title>
<style>
 body{font:13px monospace;background:#14161a;color:#dde;margin:12px}
 #head{display:flex;gap:14px;align-items:baseline;flex-wrap:wrap}
 .badge{padding:2px 8px;border-radius:4px;background:#333}
 .host{background:#274e13}.client{background:#1c3a5e}
 .migr{background:#5e4a1c}.failed{background:#5e1c1c}
 canvas{background:#0b0d10;border:1px solid #333;margin-top:8px}
 button{margin:2px;padding:4px 10px;background:#26303a;color:#dde;
        border:1px solid #445;cursor:pointer}
 #result{margin-top:8px;padding:8px;border:1px solid #2a4a2a;white-space:pre-wrap}
 #events{margin-top:8px;color:#9ab;max-height:180px;overflow:auto}
 #audit{margin-top:8px;color:#9ab;max-height:220px;overflow:auto}
 table{border-collapse:collapse;font-size:12px}
 td,th{border:1px solid #334;padding:1px 6px;text-align:left}
 .migrated{color:#8f8}.resent{color:#fc8}.dropped{color:#f88}
 .err{color:#f88}.ok{color:#8f8}
</style></head><body>
<div id=head>
 <b>node <span id=id></span></b>
 <span class=badge id=role></span><span class=badge id=phase></span>
 <span>epoch <b id=epoch></b></span><span>host <b id=host></b></span>
 <span>tick <b id=tick></b></span><span>hash <b id=hash></b></span>
 <span>session <b id=session></b></span>
 <span id=buf></span>
</div>
<canvas id=cv width=480 height=320></canvas>
<div>
 <button onclick=act('move_left')>&larr; move</button>
 <button onclick=act('move_right')>move &rarr;</button>
 <button onclick=act('stop')>stop</button>
 <button onclick=act('press')>press button (5-tick action)</button>
 <button onclick=act('pickup')>pick up item</button>
 <button onclick=act('handoff_log')>handoff (revocable log)</button>
 <button onclick=act('handoff_frozen')>handoff (frozen once)</button>
 <button onclick=act('exit')>voluntary exit (host)</button>
 <button onclick=crash()>simulate host crash</button>
</div>
<div id=result></div>
<div id=audit></div>
<div id=events></div>
<script>
async function act(a){
 const r=await fetch('/api/action',{method:'POST',body:JSON.stringify({action:a})});
 const j=await r.json();
 if(!j.ok) note('CONTROL REJECTED: '+j.error);
}
async function crash(){ await fetch('/api/crash',{method:'POST'}); }
function note(m){document.getElementById('result').dataset.note=m;}
function draw(w){
 const c=document.getElementById('cv').getContext('2d');
 c.clearRect(0,0,480,320);
 if(!w){c.fillStyle='#666';c.fillText('no world state yet',10,20);return;}
 const p=w.platform; c.fillStyle='#8af';
 c.fillRect(p.x-30,p.y,60,8);
 const d=w.door; c.fillStyle='#fa8';
 c.fillRect(380,280-100*(d.open/100)-0,14,100*(d.open/100));
 c.strokeStyle='#fa8'; c.strokeRect(380,180,14,100);
 const g=w.gear; c.strokeStyle='#af8'; c.beginPath();
 c.arc(g.x,g.y,16,0,7); c.moveTo(g.x,g.y);
 c.lineTo(g.x+16*Math.cos(g.angle*Math.PI/180),g.y+16*Math.sin(g.angle*Math.PI/180));
 c.stroke();
 const b=w.button; c.fillStyle=(w.tick<b.pressed_until)?'#ff4':'#774';
 c.fillRect(b.x-8,b.y-4,16,4);
 for(const id in w.players){const q=w.players[id];
  c.fillStyle=q.action?'#f8f':'#4cf';
  c.beginPath();c.arc(q.x,q.y-8,8,0,7);c.fill();
  c.fillStyle='#9ab';c.fillText(id,q.x-10,q.y+8);}
 c.fillStyle='#9ab';
 c.fillText('press_count='+w.press_count,10,16);
}
async function poll(){
 try{
  const r=await fetch('/api/state'); const s=await r.json();
  id.textContent=s.id; epoch.textContent=s.epoch; host.textContent=s.host_id;
  tick.textContent=s.tick; hash.textContent=s.hash;
  session.textContent=(s.session||'').slice(0,18);
  role.textContent=s.role.toUpperCase();
  role.className='badge '+(s.role==='host'?'host':'client');
  phase.textContent=s.phase;
  phase.className='badge '+(s.phase.includes('failed')?'failed':
    (s.phase==='hosting'||s.phase==='client')?(s.role==='host'?'host':'client'):'migr');
  buf.textContent=s.buffering?('BUFFERING inputs ('+s.buffer_mode+'), queued='+s.unacked):'';
  draw(s.world);
  let out='';
  if(s.migration){
   const m=s.migration;
   out+='MIGRATION ['+m.reason+'] epoch '+m.from_epoch+' -> '+m.to_epoch+'\\n'+
    '  boundary tick '+m.boundary_tick+' -> resumed at '+m.resume_tick+'\\n'+
    '  hash verified: '+m.hash_verified+'  boundary sealed: '+m.boundary_sealed+
    '  world restored: '+m.world_restored+'  clock ticked: '+m.ticked+'\\n'+
    '  entities preserved: '+m.entities+'  session: '+m.session+'\\n'+
    '  COMPLETE: '+(m.complete?'YES (verified state + sealed boundary + clock resumed)':'not yet');
  }
  for(const c of s.continuity){
   out+='\\nSESSION SURVIVED '+c.session.slice(0,18)+'  epoch '+c.from_epoch+'->'+c.to_epoch+
    '  boundary '+c.boundary_tick+' resumed '+c.resumed_at+
    '  hash_ok='+c.hash_ok+'  entities='+c.entities+'  replayed_inputs='+c.replayed_inputs+
    '  ('+c.reason+')';
  }
  if(s.handoff){
   const h=s.handoff;
   out+='\\nHANDOFF#'+h.id+' ['+h.strategy+'] stage='+h.stage+
    ' epoch '+h.from_epoch+'->'+h.to_epoch+'  boundary tick '+h.boundary_tick+
    '  publish_count='+h.publish_count+'\\n'+
    '  confirmed boundary: '+JSON.stringify(h.confirmed)+
    '  unconfirmed: '+JSON.stringify((h.unconfirmed||[]).map(u=>u.cid+':'+u.seq))+
    '  carry='+h.carry_unconfirmed+'\\n'+
    '  candidates '+JSON.stringify(h.candidates)+'  rejected '+JSON.stringify(h.rejected);
   for(const st of (h.steps||[]))
    out+='\\n   stage '+st.stage+' :: '+st.note;
  }
  const inv=s.inventory||{};
  out+='\\nITEMS created='+s.items_created+'  '+Object.keys(inv).map(k=>k+'=['+inv[k].join(',')+']').join('  ');
  if(s.handoff_log && s.handoff_log.length){
   out+='\\nHANDOFF LOG (retained):';
   for(const a of s.handoff_log)
    out+='\\n  #'+a.id+' '+a.strategy+' -> '+a.candidate+
     ' rejected='+JSON.stringify(a.rejected)+' stage='+a.stage+
     ' publishes='+a.publish_count+(a.done?' (done)':' (open)');
  }
  const noteEl=document.getElementById('result');
  if(noteEl.dataset.note){out=noteEl.dataset.note+'\\n\\n'+out;noteEl.dataset.note='';}
  if(out) result.textContent=out;
  const rows=(s.input_log||[]).slice().reverse().map(e=>{
   const src=e.source==='migrated'?'migrated':(e.source==='client_resend'?'resent':'direct');
   const st=e.status==='applied'?'ok':(e.status==='dropped_duplicate'?'dropped':'');
   return '<tr class='+st+'><td>'+e.epoch+'</td><td>'+e.host+'</td>'+
    '<td>'+e.cid+'</td><td>'+e.seq+'</td><td>'+e.action+'</td>'+
    '<td>'+e.status+'</td><td class='+src+'>'+e.source+'</td><td>'+
    (e.boundary_tick==null?'-':e.boundary_tick)+'</td></tr>';}).join('');
  audit.innerHTML='<table><tr><th>ep</th><th>host</th><th>player</th><th>seq</th>'+
   '<th>action</th><th>status</th><th>source</th><th>boundary</th></tr>'+rows+'</table>';
  events.innerHTML=s.events.map(e=>{
   const cl=e.kind==='error'?'err':(e.kind==='migrate'?'ok':'');
   return '<div class='+cl+'>['+e.kind+'] '+e.msg+'</div>';}).join('');
 }catch(e){}
 setTimeout(poll,300);
}
poll();
</script></body></html>"""


class _Handler(BaseHTTPRequestHandler):
    node = None
    loop = None

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/":
            body = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/api/state":
            self._json(self.node.ui_state())
        else:
            self._json({"ok": False, "error": "not found"}, 404)

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(n) or b"{}")
        if self.path == "/api/action":
            fut = asyncio.run_coroutine_threadsafe(
                self.node.ui_action(body.get("action", "")), self.loop)
            try:
                self._json(fut.result(timeout=5))
            except Exception as e:  # failure feeds back to the window
                self._json({"ok": False, "error": str(e)})
        elif self.path == "/api/crash":
            self._json({"ok": True})
            # Abrupt, realistic crash: the process dies without goodbye.
            self.loop.call_later(
                0.2, lambda: __import__("os")._exit(1))
        else:
            self._json({"ok": False, "error": "not found"}, 404)

    def log_message(self, *a):
        pass


def start_ui(node, loop, port: int) -> ThreadingHTTPServer:
    handler = type("Handler", (_Handler,), {"node": node, "loop": loop})
    srv = ThreadingHTTPServer(("127.0.0.1", port), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv
