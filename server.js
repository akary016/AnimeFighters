// Servidor Lutadores 2D: multiplayer (ver jogadores no mesmo mapa) + contas com save online.
const http=require('http'),fs=require('fs'),crypto=require('crypto'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||8080;
const SB_URL=(process.env.SUPABASE_URL||'').replace(/\/$/,''),SB_KEY=process.env.SUPABASE_KEY||'';
const FILE=process.env.DATA_FILE||'data.json';
const hash=c=>crypto.createHash('sha256').update(String(c)).digest('hex');

/* ---------- armazenamento: Supabase (se configurado) ou arquivo ---------- */
let mem={};
if(!SB_URL){try{mem=JSON.parse(fs.readFileSync(FILE,'utf8'))}catch(e){}}
let dirty=false;
if(!SB_URL)setInterval(()=>{if(dirty){dirty=false;try{fs.writeFileSync(FILE,JSON.stringify(mem))}catch(e){console.log('erro ao gravar',e.message)}}},5000);
const flush=()=>{if(dirty&&!SB_URL){dirty=false;try{fs.writeFileSync(FILE,JSON.stringify(mem))}catch(e){}}};
process.on('SIGTERM',()=>{flush();process.exit(0)});
const sbH={apikey:SB_KEY,Authorization:'Bearer '+SB_KEY,'Content-Type':'application/json'};
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
  const code=String(b.code||'');if(code.length<10||code.length>40)return send(r,400,{e:'Código inválido.'});
  const h=hash(code),cur=await store.get(h);
  if(!cur)return send(r,404,{e:'Conta não encontrada.'});
  if(q.url==='/api/load'){if(!ok('ld'+h,30,6e4))return send(r,429,{e:'Devagar.'});return send(r,200,{data:cur.data,ts:cur.ts})}
  if(q.url==='/api/save'){
   if(!ok('sv'+h,12,6e4))return send(r,429,{e:'Devagar.'});
   if(+b.base!==+cur.ts)return send(r,409,{e:'Conflito: este progresso é mais antigo que o do servidor.',ts:cur.ts});
   const d=b.data;
   if(!d||typeof d!=='object'||!Array.isArray(d.bag)||d.bag.length>200000||typeof d.coins!=='number'||!(d.coins>=0)||d.coins>1e36)return send(r,400,{e:'Save inválido.'});
   const ts=Math.max(Date.now(),cur.ts+1);await store.set(h,d,ts);return send(r,200,{ts});
  }
  send(r,404,{e:'?'});
 }catch(e){console.log('erro',e.message);send(r,500,{e:'Erro no servidor.'})}
});

/* ---------- WebSocket: jogadores no mesmo mapa ---------- */
const wss=new WebSocketServer({server:srv,maxPayload:1024});
let nid=1;
wss.on('connection',ws=>{
 ws.id=nid++;ws.d={n:'Jogador',m:-1,x:0,y:0,e:'🧑‍🎤'};ws.last=0;
 ws.on('message',raw=>{
  let m;try{m=JSON.parse(raw)}catch(e){return}
  const now=Date.now();if(now-ws.last<60)return;ws.last=now;
  if(m.t!=='p')return;
  const d=ws.d;
  d.n=String(m.n||'Jogador').slice(0,14);d.e=String(m.e||'🧑‍🎤').slice(0,8);
  d.m=Number.isFinite(m.m)?m.m|0:-1;d.x=Math.max(0,Math.min(9e4,+m.x||0));d.y=Math.max(0,Math.min(9e4,+m.y||0));
 });
});
setInterval(()=>{
 const by={};
 wss.clients.forEach(c=>{(by[c.d.m]=by[c.d.m]||[]).push(c)});
 for(const k in by){
  const list=by[k];
  list.forEach(c=>{
   const o=list.filter(x=>x!==c).slice(0,40).map(x=>({i:x.id,n:x.d.n,e:x.d.e,x:Math.round(x.d.x),y:Math.round(x.d.y)}));
   if(c.readyState===1)c.send(JSON.stringify({t:'s',o,tot:wss.clients.size}));
  });
 }
},120);
srv.listen(PORT,()=>console.log('Servidor na porta '+PORT));
