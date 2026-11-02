// Servidor Lutadores 2D: multiplayer, contas online e administração segura.
const http=require('http'),fs=require('fs'),crypto=require('crypto'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||8080;
const SB_URL=(process.env.SUPABASE_URL||'').replace(/\/$/,''),SB_KEY=process.env.SUPABASE_KEY||'';
const FILE=process.env.DATA_FILE||'data.json';
const ADMIN_KEY=String(process.env.ADMIN_KEY||'');
const hash=c=>crypto.createHash('sha256').update(String(c)).digest('hex');
const safeEq=(a,b)=>{const x=Buffer.from(String(a)),y=Buffer.from(String(b));return x.length===y.length&&x.length>0&&crypto.timingSafeEqual(x,y)};
/* armazenamento: Supabase (se configurado) ou arquivo */
let mem={};if(!SB_URL){try{mem=JSON.parse(fs.readFileSync(FILE,'utf8'))}catch(e){}}
let dirty=false;
if(!SB_URL)setInterval(()=>{if(dirty){dirty=false;try{fs.writeFileSync(FILE,JSON.stringify(mem))}catch(e){console.log('erro ao gravar',e.message)}}},5000);
const flush=()=>{if(dirty&&!SB_URL){dirty=false;try{fs.writeFileSync(FILE,JSON.stringify(mem))}catch(e){}}};
process.on('SIGTERM',()=>{flush();process.exit(0)});
const sbH=SB_KEY.startsWith('sb_')?{apikey:SB_KEY,'Content-Type':'application/json'}:{apikey:SB_KEY,Authorization:'Bearer '+SB_KEY,'Content-Type':'application/json'};
const store={async get(h){if(!SB_URL)return mem[h]||null;const r=await fetch(`${SB_URL}/rest/v1/saves?id=eq.${encodeURIComponent(h)}&select=data,ts`,{headers:sbH});if(!r.ok)throw new Error('db '+r.status);const j=await r.json();return j[0]||null},async set(h,data,ts){if(!SB_URL){mem[h]={data,ts};dirty=true;return}const r=await fetch(`${SB_URL}/rest/v1/saves`,{method:'POST',headers:{...sbH,Prefer:'resolution=merge-duplicates'},body:JSON.stringify({id:h,data,ts})});if(!r.ok)throw new Error('db '+r.status)}};
/* índice privado para pesquisa administrativa, eventos globais e auditoria */
const readObj=async(k,def={})=>{const x=await store.get(k);return x&&x.data&&typeof x.data==='object'?x.data:def};
const writeObj=async(k,data)=>store.set(k,data,Date.now());
let RK={},rkDirty=false,rkReady=false;
(async()=>{try{const r=await store.get('rank');if(r&&r.data&&typeof r.data==='object')RK=r.data}catch(e){console.log('rank',e.message)}rkReady=true})();
setInterval(async()=>{if(!rkDirty||!rkReady)return;rkDirty=false;try{await store.set('rank',RK,Date.now())}catch(e){rkDirty=true;console.log('erro rank',e.message)}},10000);
const rkTop=me=>{const L=Object.entries(RK),mk=f=>{const s=L.filter(([k,o])=>(o[f]||0)>0).sort((a,b)=>b[1][f]-a[1][f]);return{l:s.slice(0,50).map(([k,o])=>({n:o.n,v:o[f],me:k===me?1:0})),pos:me?s.findIndex(([k])=>k===me)+1:0,tot:s.length}};return{f:mk('p'),d:mk('c'),r:mk('r')}};
const lim={};const ok=(k,max,ms)=>{const n=Date.now(),a=(lim[k]=(lim[k]||[]).filter(t=>n-t<ms));if(a.length>=max)return false;a.push(n);return true};
const cors={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'content-type','Access-Control-Allow-Methods':'POST,GET,OPTIONS'};
const send=(r,c,o)=>{r.writeHead(c,{...cors,'content-type':'application/json'});r.end(JSON.stringify(o))};
const body=q=>new Promise((res,rej)=>{let b='',n=0;q.on('data',d=>{n+=d.length;if(n>3e6){rej(new Error('grande'));q.destroy();return}b+=d});q.on('end',()=>{try{res(JSON.parse(b||'{}'))}catch(e){rej(e)}});q.on('error',rej)});
const cleanName=v=>String(v||'Jogador').replace(/[\u0000-\u001f\u007f<>&]/g,' ').trim().slice(0,14)||'Jogador';
const adminOK=b=>ADMIN_KEY&&safeEq(b.adminKey||'',ADMIN_KEY);
async function audit(action,details={}){const a=await readObj('admin_logs',[]);a.push({at:Date.now(),action:String(action).slice(0,100),details});if(a.length>2000)a.splice(0,a.length-2000);await writeObj('admin_logs',a)}
async function playerIndex(id,name,ts){const p=await readObj('players',{});p[id]={id,name:cleanName(name),updatedAt:ts||Date.now()};await writeObj('players',p)}
/* Migra contas já existentes para o índice de busca, sem expor códigos de acesso. */
const playersIndexReady=(async()=>{try{
 const idx=await readObj('players',{});
 if(!SB_URL){for(const [id,row] of Object.entries(mem)){if(/^[a-f0-9]{64}$/.test(id)){const d=row&&row.data;idx[id]={id,name:cleanName(d&&d.cfg&&d.cfg.mpname||idx[id]&&idx[id].name||'Jogador'),updatedAt:row&&row.ts||Date.now()}}}}
 else {for(let offset=0;offset<1000000;offset+=1000){const r=await fetch(`${SB_URL}/rest/v1/saves?select=id,data,ts&order=id.asc`,{headers:{...sbH,Range:`${offset}-${offset+999}`}});if(!r.ok)throw new Error('db index '+r.status);const rows=await r.json();for(const row of rows){if(/^[a-f0-9]{64}$/.test(String(row.id))){const d=row.data;idx[row.id]={id:row.id,name:cleanName(d&&d.cfg&&d.cfg.mpname||idx[row.id]&&idx[row.id].name||'Jogador'),updatedAt:row.ts||Date.now()}}}if(rows.length<1000)break}}
 const latest=await readObj('players',{});Object.assign(idx,latest);await writeObj('players',idx);console.log('índice de jogadores pronto:',Object.keys(idx).length);
 }catch(e){console.log('não foi possível reconstruir índice de jogadores:',e.message)}})();
const srv=http.createServer(async(q,r)=>{
 if(q.method==='OPTIONS'){r.writeHead(204,cors);return r.end()}
 const ip=(q.headers['x-forwarded-for']||q.socket.remoteAddress||'').split(',')[0].trim();
 try{
  if(q.url==='/'){r.writeHead(200,{...cors,'content-type':'text/plain'});return r.end('Servidor Lutadores 2D ok. Jogadores: '+wss.clients.size+' | Banco: '+(SB_URL?'Supabase':'arquivo'))}
  if(q.method!=='POST')return send(r,404,{e:'?'});
  const b=await body(q);
  if(q.url==='/api/events'){const ev=await readObj('global_events',{evd:{},evauto:true});return send(r,200,{events:ev})}
  if(q.url.startsWith('/api/admin/')){
   if(!ADMIN_KEY)return send(r,503,{e:'Configure ADMIN_KEY nas variáveis de ambiente do Render.'});
   if(!adminOK(b))return send(r,403,{e:'Chave administrativa inválida.'});
   if(q.url==='/api/admin/search'){
    if(!ok('as'+ip,60,6e4))return send(r,429,{e:'Muitas pesquisas. Aguarde.'});
    const term=String(b.name||'').trim().toLocaleLowerCase('pt-BR');if(term.length<2)return send(r,400,{e:'Digite pelo menos 2 letras do nome.'});
    await playersIndexReady;const p=await readObj('players',{});const list=Object.values(p).filter(x=>x&&x.id&&String(x.name||'').toLocaleLowerCase('pt-BR').includes(term)).sort((a,b)=>a.name.localeCompare(b.name,'pt-BR')).slice(0,50);
    return send(r,200,{players:list.map(x=>({id:x.id,name:x.name,updatedAt:x.updatedAt||0}))});
   }
   if(q.url==='/api/admin/logs'){
    const logs=await readObj('admin_logs',[]);return send(r,200,{logs:logs.slice(-100).reverse()});
   }
   if(q.url==='/api/admin/events'){
    const ev=await readObj('global_events',{evd:{},evauto:true});
    if(b.action==='set'){
     const k=String(b.date||'');if(!/^\d{4}-\d{2}-\d{2}$/.test(k))return send(r,400,{e:'Data inválida.'});
     let v=b.event;if(v!=='none'&&typeof v!=='string'&&(!v||typeof v!=='object'))return send(r,400,{e:'Evento inválido.'});
     if(v==='none')v='none';if(typeof v==='object'){v={n:String(v.n||'Evento especial').slice(0,24),e:String(v.e||'🎉').slice(0,4),coin:Math.max(0,Math.min(1e6,+v.coin||1)),dmg:Math.max(0,Math.min(1e6,+v.dmg||1)),spd:Math.max(0,Math.min(1e6,+v.spd||1))}}
     ev.evd=ev.evd||{};ev.evd[k]=v;
     await writeObj('global_events',ev);await audit('Evento global definido',{date:k,event:v});return send(r,200,{events:ev});
    }
    if(b.action==='delete'){
     const k=String(b.date||'');if(ev.evd)delete ev.evd[k];await writeObj('global_events',ev);await audit('Evento global removido',{date:k});return send(r,200,{events:ev});
    }
    if(b.action==='auto'){
     ev.evauto=!!b.enabled;await writeObj('global_events',ev);await audit('Rotação global de eventos alterada',{enabled:ev.evauto});return send(r,200,{events:ev});
    }
    return send(r,200,{events:ev});
   }
   if(q.url==='/api/admin/grant'){
    const id=String(b.target||'');if(!/^[a-f0-9]{64}$/.test(id))return send(r,400,{e:'Jogador inválido.'});
    const cur=await store.get(id);if(!cur)return send(r,404,{e:'Conta não encontrada.'});
    const d=cur.data&&typeof cur.data==='object'?cur.data:{};
    d.bag=Array.isArray(d.bag)?d.bag:[];d.coins=Number.isFinite(+d.coins)?Math.max(0,+d.coins):0;d.dex=d.dex&&typeof d.dex==='object'?d.dex:{};d.nuid=Math.max(1,Math.floor(+d.nuid||1));d.gear=Array.isArray(d.gear)?d.gear:[];d.rel=Array.isArray(d.rel)?d.rel:[];d.pots=d.pots&&typeof d.pots==='object'?d.pots:{};d.arts=Array.isArray(d.arts)?d.arts:[];d.ngr=Math.max(1,Math.floor(+d.ngr||1));d.nart=Math.max(1,Math.floor(+d.nart||1));
    const type=String(b.type||''),qty=Math.max(1,Math.min(200,Math.floor(+b.qty||1)));let desc='';
    if(type==='coins'){
     const amount=+b.amount;if(!Number.isFinite(amount)||amount<0||amount>1e36)return send(r,400,{e:'Quantidade de moedas inválida.'});d.coins=Math.min(999e63,d.coins+amount);desc={type,amount};
    }else if(type==='character'){
     const cid=String(b.id||'').slice(0,100);if(!/^[a-zA-Z0-9_-]+$/.test(cid))return send(r,400,{e:'ID de personagem inválido.'});if(d.bag.length+qty>200000)return send(r,400,{e:'Bolsa excederia o limite.'});
     const lv=Math.max(1,Math.min(10000,Math.floor(+b.level||1))),pass=b.passive=== '_n'?null:(b.passive?String(b.passive).slice(0,100):null),sh=!!b.shiny;
     for(let i=0;i<qty;i++){d.bag.push({u:d.nuid++,id:cid,lv,p:pass,sh});const x=d.dex[cid]||(d.dex[cid]={n:0,sh:false});x.n=(x.n||0)+1;if(sh)x.sh=true}desc={type,id:cid,qty,level:lv,passive:pass,shiny:sh};
    }else if(type==='gear'){
     const t=String(b.item||''),rar=Math.max(0,Math.min(6,Math.floor(+b.rarity||0)));if(!['arma','anel','amu','bota'].includes(t))return send(r,400,{e:'Tipo de equipamento inválido.'});if(d.gear.length+qty>10000)return send(r,400,{e:'Limite de equipamentos excedido.'});for(let i=0;i<qty;i++)d.gear.push({u:d.ngr++,t,r:rar,q:1});desc={type,item:t,rarity:rar,qty};
    }else if(type==='relic'){
     const t=String(b.item||''),rar=Math.max(0,Math.min(6,Math.floor(+b.rarity||0)));if(!['dmg','atk','luck','coin','move','all'].includes(t))return send(r,400,{e:'Tipo de relíquia inválido.'});if(d.rel.length+qty>10000)return send(r,400,{e:'Limite de relíquias excedido.'});for(let i=0;i<qty;i++)d.rel.push({u:d.ngr++,t,r:rar,q:1});desc={type,item:t,rarity:rar,qty};
    }else if(type==='potion'){
     const t=String(b.item||''),tier=Math.max(0,Math.min(2,Math.floor(+b.tier||0)));if(!['pw','pl','pc','pa','pm'].includes(t))return send(r,400,{e:'Poção inválida.'});const k=t+tier;d.pots[k]=(d.pots[k]||0)+qty;desc={type,item:k,qty};
    }else if(type==='artifact'){
     const t=String(b.item||''),rar=Math.max(0,Math.min(4,Math.floor(+b.rarity||0)));if(!['dmg','luck','coin','atk','move'].includes(t))return send(r,400,{e:'Artefato inválido.'});if(d.arts.length+qty>10000)return send(r,400,{e:'Limite de artefatos excedido.'});for(let i=0;i<qty;i++)d.arts.push({u:d.nart++,t,r:rar});desc={type,item:t,rarity:rar,qty};
    }else return send(r,400,{e:'Tipo de recompensa desconhecido.'});
    const ts=Math.max(Date.now(),(cur.ts||0)+1);await store.set(id,d,ts);const idx=await readObj('players',{});const nm=idx[id]&&idx[id].name||'Jogador';await audit('Recompensa concedida',{target:id,targetName:nm,reward:desc});return send(r,200,{ts,description:desc});
   }
   if(q.url==='/api/admin/log'){
    await audit(String(b.action||'Ação ADM'),b.details&&typeof b.details==='object'?b.details:{});return send(r,200,{ok:true});
   }
   return send(r,404,{e:'?' });
  }
  if(q.url==='/api/new'){
   if(!ok('new'+ip,5,36e5))return send(r,429,{e:'Muitas contas criadas. Tente mais tarde.'});
   const code=crypto.randomBytes(12).toString('base64url'),ts=Date.now(),id=hash(code);await store.set(id,null,ts);await playerIndex(id,'Jogador',ts);return send(r,200,{code,ts});
  }
  if(q.url==='/api/top'){
   if(!ok('tp'+ip,30,6e4))return send(r,429,{e:'Devagar.'});const c=String(b.code||'');return send(r,200,rkTop(c.length>=10&&c.length<=40?hash(c).slice(0,16):''));
  }
  const code=String(b.code||'');if(code.length<10||code.length>40)return send(r,400,{e:'Código inválido.'});const h=hash(code),cur=await store.get(h);if(!cur)return send(r,404,{e:'Conta não encontrada.'});
  if(q.url==='/api/load'){if(!ok('ld'+h,30,6e4))return send(r,429,{e:'Devagar.'});return send(r,200,{data:cur.data,ts:cur.ts})}
  if(q.url==='/api/save'){
   if(!ok('sv'+h,12,6e4))return send(r,429,{e:'Devagar.'});if(+b.base!==+cur.ts)return send(r,409,{e:'Conflito: este progresso é mais antigo que o do servidor.',ts:cur.ts});
   const d=b.data;if(!d||typeof d!=='object'||!Array.isArray(d.bag)||d.bag.length>200000||typeof d.coins!=='number'||!(d.coins>=0)||d.coins>1e36)return send(r,400,{e:'Save inválido.'});
   const ts=Math.max(Date.now(),cur.ts+1);await store.set(h,d,ts);await playerIndex(h,d.cfg&&d.cfg.mpname,ts);return send(r,200,{ts});
  }
  if(q.url==='/api/rank'){
   if(!ok('rk'+h,12,6e4))return send(r,429,{e:'Devagar.'});const n=cleanName(b.n),num=(v,mx)=>{v=+v;return Number.isFinite(v)&&v>=0?Math.min(v,mx):0};const k=h.slice(0,16),o=RK[k]||{p:0,c:0,r:0};RK[k]={n,p:Math.max(o.p||0,num(b.p,1e300)),c:Math.max(o.c||0,num(b.c,1e36)),r:Math.max(o.r||0,Math.floor(num(b.r,5000))),t:Date.now()};const ks=Object.keys(RK);if(ks.length>3000)ks.sort((x,y)=>RK[x].t-RK[y].t).slice(0,ks.length-3000).forEach(x=>delete RK[x]);rkDirty=true;await playerIndex(h,n,Date.now());return send(r,200,rkTop(k));
  }
  send(r,404,{e:'?'});
 }catch(e){console.log('erro',e.message);send(r,500,{e:'Erro no servidor.'})}
});
/* WebSocket: jogadores no mesmo mapa */
const wss=new WebSocketServer({server:srv,maxPayload:1024});let nid=1;const hist=[];
const sendAll=(f,o)=>{const s=JSON.stringify(o);wss.clients.forEach(c=>{if(c.readyState===1&&f(c))c.send(s)})};
wss.on('connection',ws=>{ws.id=nid++;ws.lc=0;if(hist.length)ws.send(JSON.stringify({t:'h',h:hist}));ws.d={n:'Jogador',m:-1,x:0,y:0,e:'🧑‍🎤',t:[],f:1};ws.last=0;ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(e){return}const now=Date.now();if(m.t==='c'){const x=String(m.x||'').replace(/[\u0000-\u001f\u007f]/g,' ').trim().slice(0,120);if(!x)return;if(now-ws.lc<1200){ws.send(JSON.stringify({t:'c',sys:1,ch:m.ch==='g'?'g':'m',n:'Sistema',x:'Calma! Espere um pouco para mandar outra mensagem.'}));return}ws.lc=now;const g=m.ch==='g',msg={t:'c',ch:g?'g':'m',n:ws.d.n,x,i:ws.id};if(g){hist.push(msg);if(hist.length>40)hist.shift();sendAll(()=>true,msg)}else if(ws.d.m>=0)sendAll(c=>c.d.m===ws.d.m,msg);return}if(now-ws.last<30)return;ws.last=now;if(m.t!=='p')return;const d=ws.d;d.n=String(m.n||'Jogador').slice(0,14);d.e=String(m.e||'🧑‍🎤').slice(0,8);d.t=Array.isArray(m.tm)?m.tm.slice(0,16).map(x=>String(x).slice(0,8)):[];d.f=m.f<0?-1:1;d.m=Number.isFinite(m.m)?m.m|0:-1;d.x=Math.max(0,Math.min(9e4,+m.x||0));d.y=Math.max(0,Math.min(9e4,+m.y||0))})});
setInterval(()=>{const by={};wss.clients.forEach(c=>{(by[c.d.m]=by[c.d.m]||[]).push(c)});for(const k in by){const list=by[k];list.forEach(c=>{const o=list.filter(x=>x!==c).slice(0,40).map(x=>({i:x.id,n:x.d.n,e:x.d.e,x:Math.round(x.d.x),y:Math.round(x.d.y),tm:x.d.t,f:x.d.f}));if(c.readyState===1)c.send(JSON.stringify({t:'s',o,tot:wss.clients.size}))})}},50);
srv.listen(PORT,()=>console.log('Servidor na porta '+PORT));
