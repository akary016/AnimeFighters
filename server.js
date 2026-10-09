// Servidor multiplayer simples: mostra jogadores do mesmo mapa em tempo real.
const http=require('http'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||8080;
const srv=http.createServer((q,r)=>{r.writeHead(200,{'content-type':'text/plain'});r.end('Servidor Lutadores 2D ok. Jogadores: '+wss.clients.size)});
const wss=new WebSocketServer({server:srv});
let nid=1;
wss.on('connection',ws=>{
 ws.id=nid++;ws.d={n:'Jogador',m:-1,x:0,y:0,e:'🧑‍🎤'};ws.last=0;
 ws.on('message',raw=>{
  if(raw.length>400)return;
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
