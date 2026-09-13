// The studio follows durable job evidence. Animation never supplies progress.
const phases = [
  {id:'research', label:'Research & write', active:'Finding the story', detail:'Researching the topic and writing your episode. This stage can take a few minutes.'},
  {id:'audio', label:'Make audio', active:'Giving the story a voice', detail:'Narrating the script and assembling the episode audio.'},
  {id:'upload', label:'Upload', active:'Sending it to Spotify', detail:'The audio is ready. Tower is handling the upload.'},
  {id:'verify', label:'Check publication', active:'Waiting for the green light', detail:'Checking the public feed. Spotify may need time to make the episode available.'},
];
const escape = s => String(s ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const activeStates=['queued','running','retry','verifying'];
let selected=localStorage.getItem('gk-followed-job'), snapshot, signature='', receivedAt=0, disconnected=false;
const $=s=>document.querySelector(s);
function duration(seconds){seconds=Math.max(0,Math.floor(seconds));if(seconds<60)return 'Less than a minute';const m=Math.floor(seconds/60);return m<60?`${m} min`:`${Math.floor(m/60)} hr ${m%60} min`;}
function safeLink(value){try{const url=new URL(value);return url.protocol==='https:'?url.href:null;}catch{return null;}}
function choose(){const jobs=(snapshot?.jobs||[]).filter(j=>j.kind==='special'||j.kind==='script');return jobs.find(j=>j.id===selected)||jobs.find(j=>activeStates.includes(j.status))||jobs[0];}
function clock(){const j=choose();if(!j)return;const p=j.progress||{};const now=(snapshot.server_time||Date.now()/1000)+(Date.now()-receivedAt)/1000;const ended=['done','cancelled'].includes(j.status);const elapsed=$('#studio-elapsed');if(elapsed)elapsed.textContent=duration((ended?j.updated:now)-(p.started_at||j.created));const activity=$('#studio-activity');if(activity)activity.textContent=disconnected?'Connection interrupted · showing the last saved update':`Last activity ${duration(now-(p.activity_at||j.updated)).toLowerCase()} ago`;const countdown=$('#studio-retry');if(countdown&&p.retry_at)countdown.textContent=p.retry_at>now?`Next attempt in ${duration(p.retry_at-now).toLowerCase()}.`:'Ready for the next worker check.';}
export function followJob(id){selected=id;localStorage.setItem('gk-followed-job',id);signature='';if(snapshot)draw();}
export function studioConnection(ok){disconnected=!ok;$('#studio')?.classList.toggle('offline',!ok);clock();}
export function renderStudio(data){snapshot=data;receivedAt=Date.now();disconnected=false;draw();}
function draw(){
 const container=$('#studio');if(!container)return;
 const j=choose(),jobs=(snapshot.jobs||[]).filter(j=>j.kind==='special'||j.kind==='script');
 const events=j?(snapshot.events||[]).filter(e=>e.job_id===j.id&&e.kind==='stage').slice(0,3):[];
 const key=JSON.stringify([j,events,jobs.map(x=>[x.id,x.title])]);if(signature===key){clock();return;}signature=key;
 if(!j){container.innerHTML=`<div class="studio-idle"><div class="idle-record" aria-hidden="true">✳</div><div><p class="studio-kicker">IN THE STUDIO</p><h2>Your idea. Your next episode.</h2><p>Request a topic above and watch it become a story, a voice, and an episode you can play.</p><div class="idle-path">Research & write <span>→</span> Make audio <span>→</span> Upload <span>→</span> Check publication</div></div></div>`;return;}
 const p=j.progress||{},live=j.status==='done'&&p.verified,legacy=j.status==='done'&&!p.verified;
 const phaseIndex=phases.findIndex(x=>x.id===p.phase),index=live?4:phaseIndex;
 const queued=j.status==='queued',retry=j.status==='retry',failed=j.status==='needs_attention',cancelled=j.status==='cancelled';
 const waiting=queued||retry||failed||cancelled||legacy;
 const current=phases[phaseIndex];
 let headline=live?'Ready for your ears.':queued?'Your idea is on the list.':retry?'A short pause. Progress is saved.':failed?'This episode needs a hand.':cancelled?'Request cancelled.':legacy?'An episode from your archive.':current?.active||'Getting the studio ready';
 let description=live?'Your episode is verified live. Press play whenever you’re ready.':queued?'Your request is saved. It will start when the production worker is available.':retry?'Tower will try again automatically using the work already saved.':failed?(j.error||'Open the details to see what needs attention. Your saved work is retained.'):cancelled?'This queued request will not be produced.':legacy?'This older request is marked complete; live publication evidence was not saved with it.':current?.detail||'Loading the saved request and preparing production.';
 const audio=p.audio;
 if(!waiting&&p.phase==='audio'&&audio)description=`${audio.completed} of ${audio.total} audio sections complete. ${audio.completed===audio.total?'Assembling the finished episode.':'Narration is progressing through the script.'}`;
 const status=live?'LIVE ON SPOTIFY':queued?'QUEUED':retry?'AUTOMATIC RETRY':failed?'NEEDS ATTENTION':cancelled?'CANCELLED':legacy?'ARCHIVE':'EPISODE IN PROGRESS';
 const listen=safeLink(p.listen_url),show='https://open.spotify.com/show/0344TpzH4nfACvR7amNX7V';
 const options=jobs.map(x=>`<option value="${escape(x.id)}" ${x.id===j.id?'selected':''}>${escape(x.title)}</option>`).join('');
 container.className=`studio ${live?'is-live':waiting?'is-paused':'is-working'} ${failed?'is-error':''}`;
 container.innerHTML=`<div class="studio-top"><p class="studio-kicker"><span class="studio-led"></span> IN THE STUDIO</p><label class="studio-select-label">Following <select id="studio-select" aria-label="Episode to follow">${options}</select></label></div>
 <div class="studio-main"><div class="studio-record" aria-hidden="true"><div class="record-groove"><div class="record-label"><span class="record-number">${live?'✓':waiting?'GK':String(Math.max(0,index)+1).padStart(2,'0')}</span><span class="record-caption">${live?'ON AIR':waiting?'DAILY':'OF 04 STAGES'}</span></div></div><div class="studio-wave">${Array.from({length:17},(_,i)=>`<i style="--bar:${[20,35,55,30,70,45,85,50,95,45,75,30,60,40,55,25,15][i]}%;--delay:${i*-.13}s"></i>`).join('')}</div></div>
 <div class="studio-story"><span class="studio-status">${status}</span><h2>${escape(j.title)}</h2><h3>${headline}</h3><p class="studio-description">${escape(description)}</p>${retry?'<p id="studio-retry"></p>':''}
 ${!waiting&&p.phase==='audio'&&audio?`<div class="audio-meter"><progress max="${audio.total}" value="${audio.completed}" aria-label="Completed audio sections"></progress><span>${audio.completed} / ${audio.total} sections</span></div>`:''}
 <div class="studio-actions">${live?`<a class="listen-button" href="${escape(listen||show)}" target="_blank" rel="noopener">▶ ${listen?'Listen to your episode':'Open Spotify'} <span>↗</span></a>`:''}${failed?`<button data-studio-action="${j.episode?'reconcile':'retry'}" data-id="${escape(j.id)}">${j.episode?'Check publication again':'Retry saved work'}</button>`:''}<button class="studio-details" data-studio-detail="${escape(j.id)}">View activity ↗</button></div></div></div>
 <ol class="studio-stages" aria-label="Episode production stages">${phases.map((phase,i)=>{const completed=live||(!legacy&&index>i),active=!waiting&&index===i;return `<li class="${completed?'finished':active?'current':'upcoming'}" ${active?'aria-current="step"':''}><span class="stage-node">${completed?'✓':i+1}</span><span>${phase.label}<small>${completed?'Complete':active?'In progress':waiting&&index===i?'Paused':'Up next'}</small></span></li>`;}).join('')}</ol>
 <div class="studio-bottom"><div><small>${p.started_at?'Time since start':'Time since request'}</small><strong id="studio-elapsed"></strong></div>${p.words?`<div><small>Script</small><strong>${Number(p.words).toLocaleString()} words</strong></div>`:''}<p id="studio-activity"></p><span class="studio-reassurance">You can close Tower. Telegram will let you know.</span></div>
 ${events.length?`<details class="studio-events"><summary>Recent activity</summary>${events.map(e=>`<p><time>${new Date(e.ts*1000).toLocaleTimeString([],{hour:'numeric',minute:'2-digit'})}</time>${escape(e.detail)}</p>`).join('')}</details>`:''}`;
 $('#studio-select').onchange=e=>followJob(e.target.value);clock();
 const announcer=$('#studio-announcement');if(announcer)announcer.textContent=`${j.title}: ${headline}${audio&&p.phase==='audio'?` ${audio.completed} of ${audio.total} sections complete.`:''}`;
}
setInterval(clock,10000);
