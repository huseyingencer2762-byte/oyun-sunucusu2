/* Ülke Fethi oyun sunucusu — paket kurulumu GEREKTİRMEZ (sadece Node.js 18+).
   Oyun istemcisi MQTT 3.1.1'i WebSocket üzerinden konuşuyor; bu sunucu odaları, eşleşmeyi ve oyuncu mesajlarını taşır.
   Çalıştırma:  node server.js         (PORT ortam değişkeni yoksa 8080) */
const http=require('http'), crypto=require('crypto');
const PORT=process.env.PORT||8080;
const subs=new Map();                 // konu -> Set<istemci>
const clients=new Set();
const srv=http.createServer((req,res)=>{ res.writeHead(200,{'content-type':'text/plain; charset=utf-8'}); res.end('ulke-fethi sunucusu çalışıyor · bağlı oyuncu: '+clients.size+'\n'); });

/* ---- WebSocket (RFC 6455), yalnızca ikili çerçeveler ---- */
function frame(payload){ const n=payload.length; let h;
  if(n<126) h=Buffer.from([0x82,n]); else if(n<65536){ h=Buffer.alloc(4); h[0]=0x82; h[1]=126; h.writeUInt16BE(n,2); } else { h=Buffer.alloc(10); h[0]=0x82; h[1]=127; h.writeBigUInt64BE(BigInt(n),2); }
  return Buffer.concat([h,payload]); }
srv.on('upgrade',(req,sock)=>{
  const key=req.headers['sec-websocket-key']; if(!key){ sock.destroy(); return; }
  const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  const wantsMqtt=String(req.headers['sec-websocket-protocol']||'').split(',').map(s=>s.trim()).includes('mqtt');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n'+(wantsMqtt?'Sec-WebSocket-Protocol: mqtt\r\n':'')+'\r\n');
  sock.setNoDelay(true);
  const c={sock,topics:new Set(),wbuf:Buffer.alloc(0),mbuf:Buffer.alloc(0),open:true,alive:true};
  clients.add(c);
  c.send=data=>{ if(c.open) try{ sock.write(frame(data)); }catch(e){} };
  sock.on('data',d=>{ c.wbuf=Buffer.concat([c.wbuf,d]); wsParse(c); });
  const gone=()=>{ if(!c.open) return; c.open=false; clients.delete(c); for(const t of c.topics){ const s=subs.get(t); if(s){ s.delete(c); if(!s.size) subs.delete(t); } } try{ sock.destroy(); }catch(e){} };
  sock.on('close',gone); sock.on('error',gone); sock.on('end',gone);
});
function wsParse(c){
  for(;;){
    const b=c.wbuf; if(b.length<2) return;
    const op=b[0]&15, masked=(b[1]&128)!==0; let len=b[1]&127, o=2;
    if(len===126){ if(b.length<4) return; len=b.readUInt16BE(2); o=4; } else if(len===127){ if(b.length<10) return; len=Number(b.readBigUInt64BE(2)); o=10; }
    if(len>(1<<20)){ c.sock.destroy(); return; }
    const need=o+(masked?4:0)+len; if(b.length<need) return;
    let p=b.subarray(o+(masked?4:0),need);
    if(masked){ const m=b.subarray(o,o+4), q=Buffer.allocUnsafe(len); for(let i=0;i<len;i++) q[i]=p[i]^m[i&3]; p=q; }
    c.wbuf=b.subarray(need);
    if(op===8){ try{ c.sock.end(Buffer.from([0x88,0])); }catch(e){} return; }
    if(op===9){ try{ c.sock.write(Buffer.concat([Buffer.from([0x8A,p.length]),p])); }catch(e){} continue; }   // ping -> pong
    if(op===0||op===1||op===2){ c.alive=true; c.mbuf=Buffer.concat([c.mbuf,p]); mqttParse(c); }
  }
}

/* ---- MQTT 3.1.1 (QoS 0, tam eşleşen konular) ---- */
const pkt=(type,body)=>{ const rl=[]; let n=body.length; do{ let x=n%128; n=Math.floor(n/128); if(n>0) x|=128; rl.push(x); }while(n>0); return Buffer.concat([Buffer.from([type,...rl]),body]); };
function mqttParse(c){
  for(;;){
    const b=c.mbuf; if(b.length<2) return;
    let i=1, rl=0, mul=1, ok=false;
    while(i<b.length&&i<5){ const d=b[i++]; rl+=(d&127)*mul; mul*=128; if(!(d&128)){ ok=true; break; } }
    if(!ok||b.length<i+rl) return;
    const type=b[0]>>4, flags=b[0]&15, body=b.subarray(i,i+rl); c.mbuf=b.subarray(i+rl);
    if(type===1) c.send(pkt(0x20,Buffer.from([0,0])));                                           // CONNECT -> CONNACK
    else if(type===8){                                                                           // SUBSCRIBE -> SUBACK
      const pid=body.subarray(0,2); let o=2; const codes=[];
      while(o+2<=body.length){ const tl=(body[o]<<8)|body[o+1]; o+=2; const t=body.subarray(o,o+tl).toString(); o+=tl+1;
        if(!subs.has(t)) subs.set(t,new Set()); subs.get(t).add(c); c.topics.add(t); codes.push(0); }
      c.send(pkt(0x90,Buffer.concat([pid,Buffer.from(codes)])));
    }
    else if(type===3){                                                                           // PUBLISH -> abonelere
      const tl=(body[0]<<8)|body[1], t=body.subarray(2,2+tl).toString(), set=subs.get(t); if(!set) continue;
      const pay=body.subarray(2+tl+(((flags>>1)&3)>0?2:0)), out=pkt(0x30,Buffer.concat([body.subarray(0,2+tl),pay]));
      for(const x of set) if(x!==c) x.send(out);
    }
    else if(type===12) c.send(Buffer.from([0xD0,0]));                                            // PINGREQ -> PINGRESP
    else if(type===14){ try{ c.sock.end(); }catch(e){} }
  }
}
setInterval(()=>{ for(const c of clients){ if(!c.alive){ try{ c.sock.destroy(); }catch(e){} continue; } c.alive=false; } },90000);   // 90 sn sessizse düşür (istemci 20 sn'de bir ping atar)
srv.listen(PORT,()=>console.log('Ülke Fethi sunucusu :'+PORT));
