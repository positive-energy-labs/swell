/**
 * The HMI: a read-only gauge with two verbs, decide and retry. The token field holds an operator's token, and the
 * operator is whoever that token names. The test checks the template, not a DOM: the script has never run under proof.
 */
export const hmi = (plants: ReadonlyArray<string>) => `<!doctype html>
<meta charset="utf-8"><title>swell</title>
<style>body{font:14px system-ui;margin:2rem;max-width:72rem}table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #ddd;padding:.3rem .5rem;text-align:left;vertical-align:top}code{font-size:12px}button{margin-right:.3rem}</style>
<h1>swell</h1>
<label>plant <select id="plant">${plants.map((p) => `<option>${p}</option>`).join("")}</select></label>
<label>operator token <input id="token" type="password" size="16"></label>
<h2>signatures</h2><table id="signatures"><thead><tr><th>signature</th><th>sources</th><th>hits</th><th>runs</th><th>rate</th></tr></thead><tbody></tbody></table>
<h2>proposals</h2><table id="proposals"><thead><tr><th>loop</th><th>subject</th><th>text</th><th>cites</th><th>verdict</th></tr></thead><tbody></tbody></table>
<h2>given up</h2><table id="dead"><thead><tr><th>rule</th><th>subject</th><th>failures</th><th>error</th><th></th></tr></thead><tbody></tbody></table>
<script>
const $=s=>document.querySelector(s);
const esc=s=>String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
$("#token").value=localStorage.getItem("swell.token")||"";$("#token").onchange=()=>localStorage.setItem("swell.token",$("#token").value);
async function load(){
  const v=await (await fetch("/view?plant="+encodeURIComponent($("#plant").value))).json();
  $("#signatures tbody").innerHTML=v.signatures.map(i=>\`<tr><td><code>\${esc(i.signature)}</code></td><td>\${esc(i.sources.join(", "))}</td><td>\${i.hits}</td><td>\${i.runs}</td><td>\${i.rate.toFixed(2)}</td></tr>\`).join("");
  $("#proposals tbody").innerHTML=v.proposals.map(p=>\`<tr><td>\${esc(p.loop)}</td><td><code>\${esc(p.subject)}</code></td><td>\${esc(p.text)}</td><td>\${p.cites.map(c=>/^https?:/.test(c)?\`<a href="\${esc(c)}">\${esc(c)}</a>\`:esc(c)).join("<br>")}</td><td>\${p.verdict?(p.verdict.accept?"yes":"no")+" "+esc(p.verdict.text):\`<button data-a="1" data-l="\${esc(p.loop)}" data-s="\${esc(p.subject)}">yes</button><button data-a="0" data-l="\${esc(p.loop)}" data-s="\${esc(p.subject)}">no</button>\`}</td></tr>\`).join("");
  $("#dead tbody").innerHTML=v.health.flatMap(h=>h.dead.map(d=>\`<tr><td>\${esc(h.rule)}</td><td><code>\${esc(d.urn)}</code></td><td>\${d.failures}</td><td><code>\${esc(d.error.slice(0,300))}</code></td><td><button data-r="\${esc(h.rule)}" data-u="\${esc(d.urn)}">retry</button></td></tr>\`)).join("");
}
const post=async(path,body)=>{const r=await fetch(path,{method:"POST",headers:{"content-type":"application/json",authorization:"Bearer "+$("#token").value},body:JSON.stringify(body)});if(!r.ok)alert(r.status+" "+await r.text());load();};
document.addEventListener("click",async e=>{
  const b=e.target.closest("button[data-a]");
  if(b){const accept=b.dataset.a==="1";const text=accept?"":prompt("why not?")||"";if(!accept&&!text)return;
    return post("/decide",{plant:$("#plant").value,loop:b.dataset.l,subject:b.dataset.s,accept,text});}
  const r=e.target.closest("button[data-r]");
  if(r)return post("/retry",{rule:r.dataset.r,subject:r.dataset.u});
});
$("#plant").onchange=load;load();setInterval(load,5000);
</script>`;
