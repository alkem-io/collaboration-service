#!/usr/bin/env bash
# Run the executable image against real Yjs clients and an isolated broker.
set -euo pipefail
: "${EVIDENCE:?set an absolute private evidence directory}"
: "${RABBITMQ_IMAGE:?set the immutable local broker fixture image}"
IMAGE="${1:?pass the immutable executable image ID}"
export IMAGE
python3 - <<'PY'
import json,os,pathlib,select,socket,subprocess,time,urllib.request,uuid
root=pathlib.Path.cwd(); evidence=pathlib.Path(os.environ['EVIDENCE']);evidence.mkdir(parents=True,exist_ok=True)
image=os.environ['IMAGE'];broker=os.environ['RABBITMQ_IMAGE']
assert image.startswith('sha256:') and '@sha256:' in broker
run='forge091-collab-smoke-'+uuid.uuid4().hex[:10]
resources={'run':run,'resources':[],'synthetic':True,'external_providers':False}
manifest=evidence/'smoke-resources.json';processes=[]
def save(): manifest.write_text(json.dumps(resources,indent=2))
def command(args): return subprocess.check_output(args,text=True).strip()
def port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def create(kind,name,args):
 assert subprocess.run(['docker',kind,'inspect',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode!=0
 row={'kind':kind,'name':name,'planned_args':args,'id':None}
 if kind=='container':row['planned_anonymous_volumes']=json.loads(command(['docker','image','inspect',args[-1]]))[0]['Config'].get('Volumes') or {}
 resources['resources'].append(row);save()
 row['id']=command(args);save();return row['id']
def line(proc,expected):
 if not select.select([proc.stdout],[],[],30)[0]: raise RuntimeError('fixture output timed out: '+expected)
 got=proc.stdout.readline().strip();assert got==expected,(got,expected)
def harness(base,doc):
 args=['node','test/e2e/jsinterop/harness.mjs','--url',base+'/collab/'+doc+'?type=memo','--timeout','15000']
 obs=subprocess.Popen(args+['--mode','observe','--expect','IMAGE-EDIT'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True);processes.append(obs)
 time.sleep(.7)
 edit=subprocess.run(args+['--mode','edit','--marker','IMAGE-EDIT'],capture_output=True,text=True,timeout=25)
 out=obs.communicate(timeout=25)[0]
 for name,code,text in [('editor',edit.returncode,edit.stdout),('observer',obs.returncode,out)]:
  (evidence/(doc+'-'+name+'.log')).write_text(text)
  result=json.loads(next(x[7:] for x in text.splitlines() if x.startswith('RESULT ')))
  assert code==0 and result['ok'] and result['synced'] and result['peerAwarenessSeen'] and not result['decodeErrors'],result
  if name=='observer':assert 'IMAGE-EDIT' in result['text']
def start(name,mode,extra=[]):
 p=port();args=['docker','run','-d','--name',run+'-'+name,'--network',run+'-net','-p',f'127.0.0.1:{p}:4006']
 for value in ['HUB_MODE=inmemory','METADATA_STORE='+mode,'CHECKPOINT_STORE=inline','AUTH_MODE=open','AUTHZ_MODE=open',*extra]:args+=['-e',value]
 cid=create('container',run+'-'+name,args+[image]);base=f'http://127.0.0.1:{p}'
 for _ in range(100):
  try:
   with urllib.request.urlopen(base+'/healthz',timeout=.5) as response:assert response.status==200
   return base.replace('http:','ws:')
  except OSError:time.sleep(.1)
 raise RuntimeError('image did not become healthy')
save()
try:
 create('network',run+'-net',['docker','network','create',run+'-net'])
 base=start('inline','inmemory')
 request=urllib.request.Request(base.replace('ws:','http:')+'/collab/image-inline',data=b'{"contentType":"memo"}',headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(request,timeout=5) as response:assert response.status==201
 harness(base,'image-inline')
 missing=subprocess.run(['node','--input-type=module','-e',"import W from 'ws';const w=new W(process.argv[1]);setTimeout(()=>process.exit(2),5000);w.on('error',()=>process.exit(3));w.on('close',c=>process.exit(c===1008?0:1))",base+'/collab/unknown-image-document?type=memo'],cwd=root/'test/e2e/jsinterop',capture_output=True,text=True,timeout=10)
 assert missing.returncode==0,missing.stderr
 bp=port();brokerid=create('container',run+'-broker',['docker','run','-d','--name',run+'-broker','--network',run+'-net','--network-alias','broker','-p',f'127.0.0.1:{bp}:5672','-e','RABBITMQ_DEFAULT_USER=forge','-e','RABBITMQ_DEFAULT_PASS=synthetic-only',broker])
 resources['resources'][-1]['mounts']=json.loads(command(['docker','inspect',brokerid]))[0]['Mounts'];save()
 for _ in range(60):
  if subprocess.run(['docker','exec',brokerid,'rabbitmq-diagnostics','-q','ping'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:break
  time.sleep(1)
 else:raise RuntimeError('broker not ready')
 queue=run+'-metadata';lifecycle=run+'-lifecycle'
 subprocess.run(['go','build','-o',str(evidence/'owner-fixture'),'.scripts/dependency-image-fixture.go'],check=True)
 owner=subprocess.Popen([str(evidence/'owner-fixture'),f'amqp://forge:synthetic-only@127.0.0.1:{bp}/',queue,lifecycle],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(evidence/'smoke-owner.log').open('w'),text=True);processes.append(owner);line(owner,'READY')
 base=start('lifecycle','rabbitmq',['RABBITMQ_URL=amqp://forge:synthetic-only@broker:5672/','RABBITMQ_QUEUE='+queue,'LIFECYCLE_QUEUE='+lifecycle])
 harness(base,'image-lifecycle')
 client=subprocess.Popen(['node','test/e2e/jsinterop/image-lifecycle.mjs',base+'/collab/image-lifecycle?type=memo'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(evidence/'smoke-lifecycle-stderr.log').open('w'),text=True);processes.append(client);line(client,'READY')
 owner.stdin.write('delete\n');owner.stdin.flush();line(owner,'DELETED')
 out=client.communicate(timeout=25)[0];(evidence/'smoke-lifecycle.log').write_text(out)
 assert client.returncode==0,out
 assert json.loads(next(x[7:] for x in out.splitlines() if x.startswith('RESULT ')))['reconnectDenied']
 (evidence/'smoke-result.json').write_text(json.dumps({'pass':True,'image':image,'inline_create':True,'two_client_yjs_sync':True,'missing_document_denial':True,'broker_lifecycle_delete_close_reconnect_denial':True,'durable_production_topology':False},indent=2))
finally:
 for proc in processes:
  if proc.poll() is None:proc.terminate()
  try:proc.wait(timeout=10)
  except subprocess.TimeoutExpired:proc.kill();proc.wait()
 for row in reversed(resources['resources']):
  if not row['id']:continue
  if row['kind']=='container':
   logs=subprocess.run(['docker','logs',row['id']],capture_output=True,text=True);(evidence/(row['name']+'.log')).write_text(logs.stdout+logs.stderr)
  args=['docker','rm','-fv',row['id']] if row['kind']=='container' else ['docker','network','rm',row['id']]
  row['cleanup_exit']=subprocess.run(args,stdout=subprocess.DEVNULL).returncode;save()
 assert all(r.get('cleanup_exit')==0 for r in resources['resources'] if r['id'])
PY
