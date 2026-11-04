// Servidor Lutadores 2D: multiplayer (ver jogadores no mesmo mapa) + contas com save online.
const http=require('http'),fs=require('fs'),crypto=require('crypto'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||8080;
const SB_URL=(process.env.SUPABASE_URL||'').replace(/\/$/,''),SB_KEY=process.env.SUPABASE_KEY||'';
const FILE=process.env.DATA_FILE||'data.json';
const ADMIN_KEY=process.env.ADMIN_KEY||'';
const hash=c=>crypto.createHash('sha256').update(String(c)).digest('hex');

/* ---------- armazenamento: Supabase (se configurado) ou arquivo ---------- */
let mem={};
if(!SB_URL){try{mem=JSON.parse(fs.readFileSync(FILE,'utf8'))}catch(e){}}
let dirty=false;
if(!SB_URL)setInterval(()=>{if(dirty){dirty=false;try{fs.writeFileSync(FILE,JSON.stringify(mem))}catch(e){console.log('erro ao gravar',e.message)}}},5000);
const flush=()=>{if(dirty&&!SB_URL){dirty=false;try{fs.writeFileSync(FILE,JSON.stringify(mem))}catch(e){}}};
process.on('SIGTERM',()=>{flush();process.exit(0)});
// chaves novas (sb_secret_...) vão só em apikey; chaves antigas (service_role, formato JWT) também em Authorization
const sbH=SB_KEY.startsWith('sb_')?{apikey:SB_KEY,'Content-Type':'application/json'}:{apikey:SB_KEY,Authorization:'Bearer '+SB_KEY,'Content-Type':'application/json'};
const store={
 async get(h){
  if(!SB_URL)return mem[h]||null;
  const r=await fetch(`${SB_URL}/rest/v1/saves?id=eq.${h}&select=data,ts`,{headers:sbH});
  if(!r.ok)throw new Error('db '+r.status);const j=await r.json();return j[0]||null;
 },
 async set(h,data,ts){
  if(!SB_URL){mem[h]={data,ts};dirty=true;return}
  const r=await fetch(`${SB_URL}/rest/v1/saves`,{method:'POST',headers:{...sbH,Prefer:'resolution=merge-duplicates'},body:JSON.stringify({id:h,data,ts})});
  if(!r.ok)throw new Error('db '+r.status);
 }
};

/* ---------- ranking global: força, dinheiro e rodadas da Trial Infinita ---------- */
let RK={},rkDirty=false,rkReady=false;
(async()=>{try{const r=await store.get('rank');if(r&&r.data&&typeof r.data==='object')RK=r.data}catch(e){console.log('rank',e.message)}rkReady=true})();
setInterval(async()=>{if(!rkDirty||!rkReady)return;rkDirty=false;try{await store.set('rank',RK,Date.now())}catch(e){rkDirty=true;console.log('erro rank',e.message)}},10000);
const rkTop=me=>{const L=Object.entries(RK),mk=f=>{const s=L.filter(([k,o])=>(o[f]||0)>0).sort((a,b)=>b[1][f]-a[1][f]);return{l:s.slice(0,50).map(([k,o])=>({n:o.n,v:o[f],me:k===me?1:0})),pos:me?s.findIndex(([k])=>k===me)+1:0,tot:s.length}};return{f:mk('p'),d:mk('c'),r:mk('r')}};

/* ---------- sessão única por conta: só um aparelho por vez grava o save ---------- */
const OWN={};// hash da conta -> sid do aparelho dono
/* ---------- eventos globais e histórico ADM (guardados no mesmo banco, linhas 'events' e 'adminlog') ---------- */
let EV={evd:{},evauto:true},LOGS=[],evDirty=false,logDirty=false;
(async()=>{try{const r=await store.get('events');if(r&&r.data&&typeof r.data==='object')EV={evd:r.data.evd||{},evauto:r.data.evauto!==false}}catch(e){}try{const r=await store.get('adminlog');if(r&&Array.isArray(r.data))LOGS=r.data}catch(e){}})();
setInterval(async()=>{if(evDirty){evDirty=false;try{await store.set('events',EV,Date.now())}catch(e){evDirty=true}}if(logDirty){logDirty=false;try{await store.set('adminlog',LOGS,Date.now())}catch(e){logDirty=true}}},4000);
const addLog=(action,details)=>{LOGS.unshift({at:Date.now(),action:String(action).slice(0,160),details:details||{}});if(LOGS.length>300)LOGS.length=300;logDirty=true};
const safeEq=(a,b)=>{a=Buffer.from(String(a));b=Buffer.from(String(b));return a.length===b.length&&crypto.timingSafeEqual(a,b)};
const num=(v,mx)=>{v=+v;return Number.isFinite(v)&&v>=0?Math.min(v,mx):0};

/* ---------- API HTTP ---------- */
const lim={};// limites simples por IP
const ok=(k,max,ms)=>{const n=Date.now(),a=(lim[k]=(lim[k]||[]).filter(t=>n-t<ms));if(a.length>=max)return false;a.push(n);return true};
const cors={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'content-type','Access-Control-Allow-Methods':'POST,GET,OPTIONS'};
const send=(r,c,o)=>{r.writeHead(c,{...cors,'content-type':'application/json'});r.end(JSON.stringify(o))};
const body=q=>new Promise((res,rej)=>{let b='',n=0;q.on('data',d=>{n+=d.length;if(n>3e6){rej(new Error('grande'));q.destroy();return}b+=d});q.on('end',()=>{try{res(JSON.parse(b||'{}'))}catch(e){rej(e)}});q.on('error',rej)});
const srv=http.createServer(async(q,r)=>{
 if(q.method==='OPTIONS'){r.writeHead(204,cors);return r.end()}
 const ip=(q.headers['x-forwarded-for']||q.socket.remoteAddress||'').split(',')[0].trim();
 try{
  if(q.url==='/'){r.writeHead(200,{...cors,'content-type':'text/plain'});return r.end('Servidor Lutadores 2D ok. Jogadores: '+wss.clients.size+' | Banco: '+(SB_URL?'Supabase':'arquivo'))}
  if(q.method!=='POST')return send(r,404,{e:'?'});
  const b=await body(q);
  if(q.url==='/api/new'){
   if(!ok('new'+ip,5,36e5))return send(r,429,{e:'Muitas contas criadas. Tente mais tarde.'});
   const code=crypto.randomBytes(12).toString('base64url'),ts=Date.now();
   await store.set(hash(code),null,ts);return send(r,200,{code,ts});
  }
  if(q.url==='/api/top'){
   if(!ok('tp'+ip,30,6e4))return send(r,429,{e:'Devagar.'});
   const c=String(b.code||'');return send(r,200,rkTop(c.length>=10&&c.length<=40?hash(c).slice(0,16):''));
  }
  if(q.url==='/api/events')return send(r,200,{events:EV});
  if(q.url.startsWith('/api/admin/')){
   if(!ok('adm'+ip,60,6e4))return send(r,429,{e:'Devagar.'});
   if(!ADMIN_KEY)return send(r,503,{e:'ADMIN_KEY não está configurada no servidor (Render > Environment).'});
   if(!ok('admf'+ip,20,6e4)||!safeEq(b.adminKey||'',ADMIN_KEY)){return send(r,403,{e:'Chave administrativa inválida.'})}
   const u=q.url.slice(11);
   if(u==='log'){addLog(b.action||'Ação ADM',b.details);return send(r,200,{ok:1})}
   if(u==='logs')return send(r,200,{logs:LOGS.slice(0,100)});
   if(u==='search'){
    const t=String(b.name||'').toLowerCase().trim(),seen={},out=[];
    Object.entries(RK).forEach(([k,o])=>{if(o.h&&(!t||String(o.n).toLowerCase().includes(t))){seen[o.h]=1;out.push({id:o.h,name:o.n})}});
    wss.clients.forEach(c=>{if(c.acc&&!seen[c.acc]&&(!t||String(c.d.n).toLowerCase().includes(t))){seen[c.acc]=1;out.push({id:c.acc,name:c.d.n+' (online)'})}});
    return send(r,200,{players:out.slice(0,50)});
   }
   if(u==='events'){
    if(b.action==='set'){const d=String(b.date||'');if(!/^\d{4}-\d{2}-\d{2}$/.test(d))return send(r,400,{e:'Data inválida.'});let v=b.event;if(v&&typeof v==='object')v={n:String(v.n||'Evento').slice(0,24),e:String(v.e||'🎉').slice(0,8),coin:Math.min(100,Math.max(.1,+v.coin||1)),dmg:Math.min(100,Math.max(.1,+v.dmg||1)),spd:Math.min(100,Math.max(.1,+v.spd||1))};else v=String(v||'none').slice(0,30);EV.evd[d]=v;addLog('Evento marcado',{date:d,event:v})}
    else if(b.action==='delete'){delete EV.evd[String(b.date||'')];addLog('Evento removido',{date:b.date})}
    else if(b.action==='auto'){EV.evauto=!!b.enabled;addLog('Rotação automática',{enabled:EV.evauto})}
    else return send(r,400,{e:'Ação desconhecida.'});
    evDirty=true;return send(r,200,{events:EV});
   }
   if(u==='grant'){
    const id=String(b.target||'');if(!/^[0-9a-f]{64}$/.test(id))return send(r,400,{e:'Jogador inválido.'});
    const cur=await store.get(id);if(!cur||!cur.data)return send(r,404,{e:'Conta sem save (o jogador ainda não salvou nada).'});
    const d=cur.data,qty=Math.max(1,Math.min(200,Math.floor(+b.qty)||1)),t=String(b.type||''),it=String(b.item||'');
    d.nuid=d.nuid||(d.bag.reduce((m,x)=>Math.max(m,x.u||0),0)+1);d.ngr=d.ngr||1;d.nart=d.nart||1;d.gear=d.gear||[];d.rel=d.rel||[];d.arts=d.arts||[];d.pots=d.pots||{};
    const rar=Math.max(0,Math.min(12,Math.floor(+b.rarity)||0));
    if(t==='coins'){const a=num(b.amount,1e36);if(!a)return send(r,400,{e:'Quantidade inválida.'});d.coins=Math.min(1e36,(+d.coins||0)+a*qty)}
    else if(t==='character'){if(!it||it.length>60)return send(r,400,{e:'Personagem inválido.'});const lv=Math.max(1,Math.min(10000,Math.floor(+b.level)||1)),p=(!b.passive||b.passive==='_n'||b.passive==='_r')?null:String(b.passive).slice(0,40);if(d.bag.length+qty>200000)return send(r,400,{e:'Bolsa cheia.'});for(let i=0;i<qty;i++)d.bag.push({u:d.nuid++,id:it,lv,p,sh:!!b.shiny})}
    else if(t==='gear'){if(!['arma','anel','amu','bota'].includes(it))return send(r,400,{e:'Equipamento inválido.'});for(let i=0;i<qty;i++)d.gear.push({u:d.ngr++,t:it,r:rar,q:1,keep:1})}
    else if(t==='relic'){if(!['dmg','atk','luck','coin','move','all'].includes(it))return send(r,400,{e:'Relíquia inválida.'});for(let i=0;i<qty;i++)d.rel.push({u:d.ngr++,t:it,r:rar,q:1,keep:1})}
    else if(t==='potion'){if(!['pw','pl','pc','pa','pm'].includes(it))return send(r,400,{e:'Poção inválida.'});const k=it+Math.max(0,Math.min(2,Math.floor(+b.tier)||0));d.pots[k]=(d.pots[k]||0)+qty}
    else if(t==='artifact'){if(!['dmg','luck','coin','atk','move'].includes(it))return send(r,400,{e:'Artefato inválido.'});for(let i=0;i<qty;i++)d.arts.push({u:d.nart++,t:it,r:Math.min(4,rar)})}
    else return send(r,400,{e:'Tipo de recompensa desconhecido.'});
    const ts=Math.max(Date.now(),cur.ts+1);await store.set(id,d,ts);
    addLog('Recompensa concedida',{target:id.slice(0,8),type:t,item:it,qty,amount:b.amount,level:b.level,rarity:b.rarity,tier:b.tier});
    return send(r,200,{ok:1,ts});
   }
   return send(r,404,{e:'Rota ADM desconhecida.'});
  }
  const code=String(b.code||'');if(code.length<10||code.length>40)return send(r,400,{e:'Código inválido.'});
  const h=hash(code),cur=await store.get(h);
  if(!cur)return send(r,404,{e:'Conta não encontrada.'});
  if(q.url==='/api/load'){if(!ok('ld'+h,30,6e4))return send(r,429,{e:'Devagar.'});if(b.sid)OWN[h]=String(b.sid).slice(0,40);return send(r,200,{data:cur.data,ts:cur.ts})}
  if(q.url==='/api/save'){
   if(!ok('sv'+h,12,6e4))return send(r,429,{e:'Devagar.'});
   if(b.sid){const o=OWN[h];if(!o)OWN[h]=String(b.sid).slice(0,40);else if(o!==String(b.sid))return send(r,409,{taken:1,e:'Esta conta foi aberta em outro aparelho/aba. Este aqui parou de salvar.',ts:cur.ts})}
   if(+b.base!==+cur.ts)return send(r,409,{e:'Conflito: este progresso é mais antigo que o do servidor.',ts:cur.ts});
   const d=b.data;
   if(!d||typeof d!=='object'||!Array.isArray(d.bag)||d.bag.length>200000||typeof d.coins!=='number'||!(d.coins>=0)||d.coins>1e36)return send(r,400,{e:'Save inválido.'});
   const ts=Math.max(Date.now(),cur.ts+1);await store.set(h,d,ts);return send(r,200,{ts});
  }
  if(q.url==='/api/rank'){
   if(!ok('rk'+h,12,6e4))return send(r,429,{e:'Devagar.'});
   const n=String(b.n||'Jogador').replace(/[\u0000-\u001f\u007f<>&]/g,' ').trim().slice(0,14)||'Jogador';
   const num=(v,mx)=>{v=+v;return Number.isFinite(v)&&v>=0?Math.min(v,mx):0};
   const k=h.slice(0,16),o=RK[k]||{p:0,c:0,r:0};
   RK[k]={h,n,p:Math.max(o.p||0,num(b.p,1e300)),c:Math.max(o.c||0,num(b.c,1e36)),r:Math.max(o.r||0,Math.floor(num(b.r,5000))),t:Date.now()};
   const ks=Object.keys(RK);if(ks.length>3000)ks.sort((x,y)=>RK[x].t-RK[y].t).slice(0,ks.length-3000).forEach(x=>delete RK[x]);
   rkDirty=true;return send(r,200,rkTop(k));
  }
  send(r,404,{e:'?'});
 }catch(e){console.log('erro',e.message);send(r,500,{e:'Erro no servidor.'})}
});

/* ---------- WebSocket: jogadores no mesmo mapa ---------- */
const wss=new WebSocketServer({server:srv,maxPayload:1024});
let nid=1;const hist=[];
const sendAll=(f,o)=>{const s=JSON.stringify(o);wss.clients.forEach(c=>{if(c.readyState===1&&f(c))c.send(s)})};
wss.on('connection',ws=>{
 ws.id=nid++;ws.lc=0;ws.alive=true;ws.on('pong',()=>{ws.alive=true});if(hist.length)ws.send(JSON.stringify({t:'h',h:hist}));
 ws.d={n:'Jogador',m:-1,x:0,y:0,e:'🧑‍🎤',t:[],f:1};ws.last=0;
 ws.on('message',raw=>{
  let m;try{m=JSON.parse(raw)}catch(e){return}
  const now=Date.now();
  if(m.t==='a'){
   const c=String(m.code||'');if(c.length<10||c.length>40)return;const h=hash(c);ws.acc=h;
   wss.clients.forEach(o=>{if(o!==ws&&o.acc===h){try{o.send(JSON.stringify({t:'k'}))}catch(e){}o.acc=null;setTimeout(()=>{try{o.terminate()}catch(e){}},300)}});
   return;
  }
  if(m.t==='c'){
   const x=String(m.x||'').replace(/[\u0000-\u001f\u007f]/g,' ').trim().slice(0,120);if(!x)return;
   if(now-ws.lc<1200){ws.send(JSON.stringify({t:'c',sys:1,ch:m.ch==='g'?'g':'m',n:'Sistema',x:'Calma! Espere um pouco para mandar outra mensagem.'}));return}
   ws.lc=now;const g=m.ch==='g',msg={t:'c',ch:g?'g':'m',n:ws.d.n,x,i:ws.id};
   if(g){hist.push(msg);if(hist.length>40)hist.shift();sendAll(()=>true,msg)}
   else if(ws.d.m>=0)sendAll(c=>c.d.m===ws.d.m,msg);
   return;
  }
  if(now-ws.last<30)return;ws.last=now;
  if(m.t!=='p')return;
  const d=ws.d;
  d.n=String(m.n||'Jogador').slice(0,14);d.e=String(m.e||'🧑‍🎤').slice(0,8);
  d.t=Array.isArray(m.tm)?m.tm.slice(0,16).map(x=>String(x).slice(0,8)):[];d.f=m.f<0?-1:1;
  d.m=Number.isFinite(m.m)?m.m|0:-1;d.x=Math.max(0,Math.min(9e4,+m.x||0));d.y=Math.max(0,Math.min(9e4,+m.y||0));
 });
});
setInterval(()=>{wss.clients.forEach(c=>{if(!c.alive){c.terminate();return}c.alive=false;try{c.ping()}catch(e){}})},20000);
setInterval(()=>{
 const by={};
 wss.clients.forEach(c=>{(by[c.d.m]=by[c.d.m]||[]).push(c)});
 for(const k in by){
  const list=by[k];
  list.forEach(c=>{
   const o=list.filter(x=>x!==c&&!(c.acc&&x.acc===c.acc)).slice(0,40).map(x=>({i:x.id,n:x.d.n,e:x.d.e,x:Math.round(x.d.x),y:Math.round(x.d.y),tm:x.d.t,f:x.d.f}));
   if(c.readyState===1)c.send(JSON.stringify({t:'s',o,tot:wss.clients.size}));
  });
 }
},50);
srv.listen(PORT,()=>console.log('Servidor na porta '+PORT));
