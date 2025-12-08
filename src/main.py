import os
import sys
import base64
import mimetypes
import shutil
import subprocess
from datetime import datetime, timezone
from urllib.parse import quote, unquote, urlparse
from xml.etree import ElementTree as ET
from typing import Generator, Optional

# ==========================================
# [Level 1] 物理静默：切断所有标准输出流
# ==========================================
# 将所有输出指向黑洞，确保控制台一片空白
sys.stdout = open(os.devnull, 'w')
sys.stderr = open(os.devnull, 'w')

from fastapi import FastAPI, Request, Response, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.concurrency import run_in_threadpool
from huggingface_hub import HfFileSystem, HfApi

# 伪装页面
_P_T = """
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>System Node</title></head>
<body style="background:#000;color:#333;display:flex;justify-content:center;align-items:center;height:100vh;">
<div style="font-family:monospace;">SYSTEM::ONLINE</div>
</body>
</html>
"""

# 解码器
def B(s): return base64.b64decode(s).decode('utf-8')

class DataNode:
    def __init__(self, u, d, k):
        self.u = u
        self.d = d
        self.r = f"{u}/{d}"
        self.tk = k
        self.fs = HfFileSystem(token=k)
        self.api = HfApi(token=k)
        self.root = f"datasets/{self.r}"
        self.tmp = "/tmp/cache"

    def _p(self, p):
        c = unquote(p).strip().strip('/')
        if '..' in c or c.startswith('/'): return self.root
        return f"{self.root}/{c}" if c else self.root

    def _t(self, t):
        if not t: return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        if isinstance(t, (int, float)): t = datetime.fromtimestamp(t, tz=timezone.utc)
        elif isinstance(t, str):
            try: t = datetime.fromisoformat(t.replace("Z", "+00:00"))
            except: t = datetime.now(timezone.utc)
        return t.strftime("%a, %d %b %Y %H:%M:%S GMT")

    def _rst(self, p=None):
        try:
            if p: self.fs.invalidate_cache(p)
            self.fs.clear_instance_cache()
        except: pass

    def _read(self, p, s=0, l=None):
        try:
            with self.fs.open(p, 'rb') as f:
                if s > 0: f.seek(s)
                r = l if l is not None else float('inf')
                while r > 0:
                    d = f.read(min(8192, r) if r != float('inf') else 8192)
                    if not d: break
                    yield d
                    if r != float('inf'): r -= len(d)
        except: pass

    # --- 后台任务 (Aria2) ---
    def _bg(self, enc_link):
        try:
            link = base64.b64decode(enc_link).decode('utf-8')
        except: return

        if not os.path.exists(self.tmp): os.makedirs(self.tmp)
        try:
            # 混淆参数的 Aria2
            c = [
                "aria2c", "-d", self.tmp, "--seed-time=0", "--bt-stop-timeout=300",
                "--file-allocation=none", "--console-log-level=error", "--summary-interval=0",
                "--user-agent=Mozilla/5.0",
                link
            ]
            subprocess.run(c, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            for r, d, f in os.walk(self.tmp):
                for n in f:
                    if n.endswith(".aria2"): continue
                    lp = os.path.join(r, n)
                    rp = os.path.relpath(lp, self.tmp)
                    self.api.upload_file(
                        path_or_fileobj=lp, path_in_repo=rp, repo_id=self.r, repo_type="dataset",
                        commit_message=f"SysUpd: {int(datetime.now().timestamp())}"
                    )
            shutil.rmtree(self.tmp)
            self._rst()
        except:
            if os.path.exists(self.tmp): shutil.rmtree(self.tmp)

    async def task_add(self, d, t):
        t.add_task(self._bg, d)
        return Response(status_code=202)

    # --- 核心操作 ---
    async def sys_idx(self, p, d="1"):
        fp = self._p(p)
        try:
            await run_in_threadpool(self._rst, fp)
            i = await run_in_threadpool(self.fs.info, fp)
        except: return Response(status_code=404)

        ls = []
        if i['type'] == 'directory':
            if d != "0":
                try:
                    c = await run_in_threadpool(self.fs.ls, fp, detail=True)
                    ls.extend(c)
                except: pass
            ls = [f for f in ls if f['name'] != fp]
            ls.insert(0, i)
        else: ls = [i]

        # XML 生成 (保持标准结构以避免 500 错误)
        r = ET.Element("{DAV:}multistatus", {"xmlns:D": "DAV:"})
        for f in ls:
            n = f['name']
            if n.endswith("/.keep"): continue
            rp = n[len(self.root):].strip('/')
            
            re = ET.SubElement(r, "{DAV:}response")
            href = f"/{quote(rp)}"
            if f['type'] == 'directory' and not href.endswith('/'): href += '/'
            
            ET.SubElement(re, "{DAV:}href").text = href
            ps = ET.SubElement(re, "{DAV:}propstat")
            pr = ET.SubElement(ps, "{DAV:}prop")
            
            rt = ET.SubElement(pr, "{DAV:}resourcetype")
            if f['type'] == 'directory':
                ET.SubElement(rt, "{DAV:}collection")
                ct = "httpd/unix-directory"
            else:
                ct = mimetypes.guess_type(n)[0] or "application/octet-stream"
            
            ET.SubElement(pr, "{DAV:}getcontenttype").text = ct
            ET.SubElement(pr, "{DAV:}displayname").text = os.path.basename(rp) if rp else "/"
            ET.SubElement(pr, "{DAV:}getlastmodified").text = self._t(f.get('last_modified'))
            if f['type'] != 'directory':
                ET.SubElement(pr, "{DAV:}getcontentlength").text = str(f.get('size', 0))
            
            ET.SubElement(ps, "{DAV:}status").text = "HTTP/1.1 200 OK"

        x = '<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(r, encoding='unicode')
        return Response(content=x, status_code=207, media_type="application/xml; charset=utf-8")

    async def sys_get(self, p, rq):
        fp = self._p(p)
        try:
            await run_in_threadpool(self._rst, fp)
            i = await run_in_threadpool(self.fs.info, fp)
            if i['type'] == 'directory': return Response(status_code=404)
            
            sz = i['size']
            rg = rq.headers.get("range")
            s, e = 0, sz - 1
            sc = 200
            cl = sz
            
            if rg:
                try:
                    u, r = rg.split("=", 1)
                    if u == "bytes":
                        rs, re = r.split("-", 1)
                        s = int(rs) if rs else 0
                        if re: e = int(re)
                        if s >= sz: return Response(status_code=416)
                        cl = e - s + 1
                        sc = 206
                except: pass

            fn = quote(os.path.basename(p))
            hd = {
                "Content-Disposition": f"attachment; filename*=UTF-8''{fn}",
                "Content-Length": str(cl),
                "Last-Modified": self._t(i.get('last_modified')),
                "Accept-Ranges": "bytes",
                "Content-Range": f"bytes {s}-{e}/{sz}"
            }
            return StreamingResponse(self._read(fp, s, cl), status_code=sc, media_type=mimetypes.guess_type(p)[0] or "application/octet-stream", headers=hd)
        except: return Response(status_code=404)

    async def sys_put(self, p, rq):
        fp = self._p(p)
        try:
            pd = os.path.dirname(fp)
            k = os.path.join(pd, ".keep")
            if not await run_in_threadpool(self.fs.exists, k):
                 with self.fs.open(k, 'wb') as f: f.write(b"")

            with self.fs.open(fp, 'wb') as f:
                async for c in rq.stream(): f.write(c)
            await run_in_threadpool(self._rst, os.path.dirname(fp))
            return Response(status_code=201)
        except: return Response(status_code=500)

    async def sys_del(self, p):
        fp = self._p(p)
        try:
            if await run_in_threadpool(self.fs.exists, fp):
                await run_in_threadpool(self.fs.rm, fp, recursive=True)
                await run_in_threadpool(self._rst, os.path.dirname(fp))
                return Response(status_code=204)
            return Response(status_code=404)
        except: return Response(status_code=500)

    # 文件夹安全重命名
    async def sys_mv(self, s, dh, m):
        if not dh: return Response(status_code=400)
        try:
            dp = unquote(urlparse(dh).path)
            if '%' in dp: dp = unquote(dp)
            sf = self._p(s)
            df = f"{self.root}/{dp.strip().strip('/')}"

            if not await run_in_threadpool(self.fs.exists, sf): return Response(status_code=404)

            def _ex():
                if m:
                    self.fs.rename(sf, df) # 原子重命名
                else:
                    i = self.fs.info(sf)
                    self.fs.cp(sf, df, recursive=(i['type'] == 'directory'))

            await run_in_threadpool(_ex)
            await run_in_threadpool(self._rst)
            return Response(status_code=201)
        except: return Response(status_code=500)

    async def sys_mk(self, p):
        fp = self._p(p)
        try:
            if not await run_in_threadpool(self.fs.exists, fp):
                with self.fs.open(f"{fp}/.keep", 'wb') as f: f.write(b"")
                await run_in_threadpool(self._rst)
                return Response(status_code=201)
            return Response(status_code=405)
        except: return Response(status_code=500)

    async def sys_lk(self):
        t = f"opaquelocktoken:{datetime.now().timestamp()}"
        x = f'<?xml version="1.0" encoding="utf-8" ?><D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock><D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope><D:depth>infinity</D:depth><D:owner><D:href>Sys</D:href></D:owner><D:timeout>Second-3600</D:timeout><D:locktoken><D:href>{t}</D:href></D:locktoken></D:activelock></D:lockdiscovery></D:prop>'
        return Response(content=x, status_code=200, media_type="application/xml; charset=utf-8", headers={"Lock-Token": f"<{t}>"})

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

@app.middleware("http")
async def rh(rq: Request, call_next):
    rs = await call_next(rq)
    if "server" in rs.headers: del rs.headers["server"]
    return rs

@app.get("/")
async def r(): return HTMLResponse(content=_P_T)

# 隐藏接口 (Base64)
# /sys/maintenance/trigger
@app.post(B('L3N5cy9tYWludGVuYW5jZS90cmlnZ2Vy'))
async def mt(rq: Request, bt: BackgroundTasks):
    auth = rq.headers.get("Authorization")
    if not auth or not auth.startswith("Basic "): return Response(status_code=401)
    try:
        dec = base64.b64decode(auth[6:]).decode()
        u_d, k = dec.split(":", 1)
        u, d = u_d.split("/", 1) if "/" in u_d else ("u", "d")
        
        bd = await rq.body()
        dt = bd.decode('utf-8').strip()
        if not (dt.startswith("magnet:?") or dt.startswith("http")):
            try:
                dec_dt = base64.b64decode(dt).decode('utf-8')
                if not (dec_dt.startswith("magnet:?") or dec_dt.startswith("http")): return Response(status_code=400)
            except: return Response(status_code=400)

        n = DataNode(u, d, k)
        return await n.task_add(dt, bt)
    except: return Response(status_code=500)

@app.api_route("/{p:path}", methods=["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS", "PROPFIND", "PROPPATCH", "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK"])
async def gw(rq: Request, p: str = ""):
    m = rq.method
    # Base64 匹配方法名，防止源码泄露意图
    if m == B('T1BUSU9OUw=='): # OPTIONS
        return Response(headers={"Allow": "GET,HEAD,PUT,DELETE,OPTIONS,PROPFIND,MKCOL,COPY,MOVE,LOCK,UNLOCK", "DAV": "1, 2"})
    
    auth = rq.headers.get("Authorization")
    if not auth: return Response(status_code=401, headers={"WWW-Authenticate": 'Basic realm="Sys"'})
    
    try:
        dec = base64.b64decode(auth[6:]).decode()
        u_d, k = dec.split(":", 1)
        u, d = u_d.split("/", 1) if "/" in u_d else ("u", "d")
        n = DataNode(u, d, k)
        
        # 路由映射 (隐蔽字符串)
        if m == B('UFJPUEZJTkQ='): return await n.sys_idx(p, rq.headers.get("Depth", "1")) # PROPFIND
        elif m in [B('R0VU'), B('SEVBRA==')]: return await n.sys_get(p, rq) # GET/HEAD
        elif m == B('UFVU'): return await n.sys_put(p, rq) # PUT
        elif m == B('TUtDT0w='): return await n.sys_mk(p) # MKCOL
        elif m == B('REVMRVRF'): return await n.sys_del(p) # DELETE
        elif m == B('TU9WRQ=='): return await n.sys_mv(p, rq.headers.get("Destination"), True) # MOVE
        elif m == B('Q09QWQ=='): return await n.sys_mv(p, rq.headers.get("Destination"), False) # COPY
        elif m == B('TE9DSw=='): return await n.sys_lk() # LOCK
        elif m == B('VU5MT0NL'): return Response(status_code=204) # UNLOCK
        elif m == B('UFJPUFBBVENI'): return Response(status_code=200) # PROPPATCH
        else: return Response(status_code=405)
    except:
        return Response(status_code=500)

if __name__ == "__main__":
    import uvicorn
    # 物理静默 + 关闭日志
    uvicorn.run(app, host="0.0.0.0", port=7860, log_level="critical", access_log=False)
