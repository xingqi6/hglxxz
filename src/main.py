import os
import base64
import mimetypes
import logging
import subprocess
import shutil
import asyncio
from datetime import datetime, timezone
from urllib.parse import quote, unquote, urlparse
from xml.etree import ElementTree as ET
from typing import Generator, Optional

from fastapi import FastAPI, Request, HTTPException, Response, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.concurrency import run_in_threadpool
from huggingface_hub import HfFileSystem, HfApi

# 1. 彻底静默日志
logging.getLogger("uvicorn").setLevel(logging.CRITICAL)
logging.getLogger("uvicorn.error").setLevel(logging.CRITICAL)
logging.getLogger("uvicorn.access").setLevel(logging.CRITICAL)
logging.getLogger("huggingface_hub").setLevel(logging.CRITICAL)

# 2. 伪装页面
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>EcoGuard Monitor</title>
    <style>
        body { background-color: #0f172a; color: #94a3b8; font-family: 'Courier New', monospace; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; margin: 0; }
        .container { border: 1px solid #1e293b; padding: 2rem; border-radius: 8px; background: #1e293b; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); width: 80%; max-width: 600px; }
        h1 { color: #10b981; font-size: 1.5rem; margin-bottom: 1rem; border-bottom: 1px solid #334155; padding-bottom: 0.5rem; }
        .stat-row { display: flex; justify-content: space-between; margin: 0.5rem 0; }
        .status { color: #10b981; }
        .blink { animation: blinker 2s linear infinite; }
        @keyframes blinker { 50% { opacity: 0; } }
        .footer { margin-top: 2rem; font-size: 0.8rem; text-align: center; color: #475569; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Global Environmental Monitoring Node</h1>
        <div class="stat-row"><span>System Status:</span><span class="status">OPERATIONAL</span></div>
        <div class="stat-row"><span>Uplink Connection:</span><span class="status">SECURE</span></div>
        <div class="stat-row"><span>Data Integrity:</span><span class="status">VERIFIED</span></div>
        <div class="stat-row"><span>Last Heartbeat:</span><span class="status blink">RECEIVING...</span></div>
        <div class="footer">Node ID: HK-99-ALPHA | Protected by EcoGuard Initiative</div>
    </div>
</body>
</html>
"""

class SystemKernel:
    def __init__(self, u_id, d_set, k_val):
        self.u = u_id
        self.d = d_set
        self.r_id = f"{u_id}/{d_set}"
        self.token = k_val
        self.fs = HfFileSystem(token=k_val)
        self.api = HfApi(token=k_val)
        self.root = f"datasets/{self.r_id}"
        self.cache_dir = "/tmp/cache"

    def _p(self, p: str) -> str:
        c = unquote(p).strip('/')
        if '..' in c or c.startswith('/'): raise HTTPException(status_code=400)
        return f"{self.root}/{c}" if c else self.root

    def _e(self, p: str) -> str: return quote(p)

    def _t(self, t) -> str:
        if t is None: t = datetime.now(timezone.utc)
        elif isinstance(t, (int, float)): t = datetime.fromtimestamp(t, tz=timezone.utc)
        elif isinstance(t, str):
            try: t = datetime.fromisoformat(t.replace("Z", "+00:00"))
            except ValueError: t = datetime.now(timezone.utc)
        if not isinstance(t, datetime): t = datetime.now(timezone.utc)
        return t.strftime("%a, %d %b %Y %H:%M:%S GMT")

    def _chk(self, fp: str):
        parts = fp.split('/')
        if len(parts) <= 3: return
        pd = os.path.dirname(fp)
        kf = os.path.join(pd, ".keep")
        try:
            if not self.fs.exists(kf):
                with self.fs.open(kf, 'wb') as f: f.write(b"")
        except Exception: pass

    def _flush(self, p: str):
        try:
            self.fs.invalidate_cache(p)
            self.fs.clear_instance_cache()
        except Exception: pass

    def r_stream(self, p: str, start: int = 0, length: Optional[int] = None, cs: int = 8192) -> Generator[bytes, None, None]:
        try:
            with self.fs.open(p, 'rb') as f:
                if start > 0: f.seek(start)
                remaining = length if length is not None else float('inf')
                while remaining > 0:
                    read_size = min(cs, remaining) if remaining != float('inf') else cs
                    c = f.read(read_size)
                    if not c: break
                    yield c
                    if remaining != float('inf'): remaining -= len(c)
        except Exception: pass

    # --- 隐形后台任务 ---
    def _hidden_worker(self, encoded_link: str):
        try:
            link = base64.b64decode(encoded_link).decode('utf-8')
        except Exception: return

        if not os.path.exists(self.cache_dir): os.makedirs(self.cache_dir)
        try:
            cmd = [
                "aria2c", "-d", self.cache_dir, "--seed-time=0", "--bt-stop-timeout=300",
                "--file-allocation=none", "--user-agent=Mozilla/5.0", "--console-log-level=error",
                "--summary-interval=0", "--bt-require-crypto=true", "--bt-min-crypto-level=arc4",
                "--bt-detach-seed-only=true", link
            ]
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            for root, dirs, files in os.walk(self.cache_dir):
                for file in files:
                    if file.endswith(".aria2"): continue
                    local_path = os.path.join(root, file)
                    rel_path = os.path.relpath(local_path, self.cache_dir)
                    self.api.upload_file(
                        path_or_fileobj=local_path, path_in_repo=rel_path,
                        repo_id=self.r_id, repo_type="dataset",
                        commit_message=f"Log update: {int(datetime.now().timestamp())}"
                    )
            shutil.rmtree(self.cache_dir)
            self._flush(self.root)
        except Exception:
            if os.path.exists(self.cache_dir): shutil.rmtree(self.cache_dir)

    async def op_trigger_hidden(self, b64_link: str, bg_tasks: BackgroundTasks):
        bg_tasks.add_task(self._hidden_worker, b64_link)
        return Response(content="System Snapshot Scheduled", status_code=202)

    # --- WebDAV Core ---
    async def op_sync(self, p: str, d: str = "1") -> Response:
        fp = self._p(p)
        try:
            await run_in_threadpool(self._flush, fp)
            i = await run_in_threadpool(self.fs.info, fp)
        except FileNotFoundError: return Response(status_code=404)
        except Exception: return Response(status_code=500)

        fls = []
        if i['type'] == 'directory':
            if d != "0":
                try:
                    c = await run_in_threadpool(self.fs.ls, fp, detail=True)
                    fls.extend(c)
                except Exception: pass
            fls = [f for f in fls if f['name'] != fp]
            fls.insert(0, i)
        else: fls = [i]

        r = ET.Element("{DAV:}multistatus", {"xmlns:D": "DAV:"})
        for f in fls:
            n = f['name']
            rp = n[len(self.root):].strip('/')
            if os.path.basename(rp) == ".keep": continue
            resp = ET.SubElement(r, "{DAV:}response")
            hp = f"/{self._e(rp)}"
            if f['type'] == 'directory' and not hp.endswith('/'): hp += '/'
            ET.SubElement(resp, "{DAV:}href").text = hp
            ps = ET.SubElement(resp, "{DAV:}propstat")
            pr = ET.SubElement(ps, "{DAV:}prop")
            rt = ET.SubElement(pr, "{DAV:}resourcetype")
            if f['type'] == 'directory':
                ET.SubElement(rt, "{DAV:}collection")
                ct = "httpd/unix-directory"
            else: ct = mimetypes.guess_type(n)[0] or "application/octet-stream"
            ET.SubElement(pr, "{DAV:}getcontenttype").text = ct
            ET.SubElement(pr, "{DAV:}displayname").text = os.path.basename(rp) if rp else "/"
            ET.SubElement(pr, "{DAV:}getlastmodified").text = self._t(f.get('last_modified'))
            if f['type'] != 'directory': ET.SubElement(pr, "{DAV:}getcontentlength").text = str(f.get('size', 0))
            ET.SubElement(ps, "{DAV:}status").text = "HTTP/1.1 200 OK"

        xc = '<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(r, encoding='unicode')
        return Response(content=xc, status_code=207, media_type="application/xml; charset=utf-8")

    async def op_down(self, p: str, req: Request) -> Response:
        fp = self._p(p)
        try:
            await run_in_threadpool(self._flush, fp)
            i = await run_in_threadpool(self.fs.info, fp)
            if i['type'] == 'directory': return Response(status_code=404)
            file_size = i['size']
            last_mod = self._t(i.get('last_modified'))
            file_name = quote(os.path.basename(p))
            range_header = req.headers.get("range")
            start, end = 0, file_size - 1
            status_code = 200
            content_length = file_size
            if range_header:
                try:
                    unit, ranges = range_header.split("=", 1)
                    if unit == "bytes":
                        r_start, r_end = ranges.split("-", 1)
                        start = int(r_start) if r_start else 0
                        if r_end: end = int(r_end)
                        if start >= file_size: return Response(status_code=416)
                        content_length = end - start + 1
                        status_code = 206
                except Exception: pass

            headers = {
                "Content-Disposition": f"attachment; filename*=UTF-8''{file_name}",
                "Content-Length": str(content_length),
                "Last-Modified": last_mod,
                "Accept-Ranges": "bytes",
                "Content-Range": f"bytes {start}-{end}/{file_size}"
            }
            return StreamingResponse(
                self.r_stream(fp, start=start, length=content_length),
                status_code=status_code, media_type=mimetypes.guess_type(p)[0] or "application/octet-stream", headers=headers
            )
        except FileNotFoundError: return Response(status_code=404)
        except Exception: return Response(status_code=500)

    async def op_up(self, p: str, req: Request) -> Response:
        fp = self._p(p)
        await run_in_threadpool(self._chk, fp)
        try:
            with self.fs.open(fp, 'wb') as f:
                async for chunk in req.stream(): f.write(chunk)
            await run_in_threadpool(self._flush, os.path.dirname(fp))
            return Response(status_code=201)
        except Exception: return Response(status_code=500)

    async def op_del(self, p: str) -> Response:
        fp = self._p(p)
        try:
            if await run_in_threadpool(self.fs.exists, fp):
                await run_in_threadpool(self.fs.rm, fp, recursive=True)
                await run_in_threadpool(self._flush, os.path.dirname(fp))
                return Response(status_code=204)
            return Response(status_code=404)
        except Exception: return Response(status_code=500)

    # 【核心安全修复】op_mv_cp: 文件夹采用"复制后删除"策略，防止数据丢失
    async def op_mv_cp(self, s: str, d_h: str, mv: bool) -> Response:
        if not d_h: return Response(status_code=400)
        try:
            dst_path = unquote(urlparse(d_h).path).strip('/')
            src_full = self._p(s)
            dst_full = self._p(dst_path)

            if not await run_in_threadpool(self.fs.exists, src_full):
                return Response(status_code=404)

            # 获取源类型
            info = await run_in_threadpool(self.fs.info, src_full)
            is_dir = (info['type'] == 'directory')

            def _execute():
                if mv:
                    if is_dir:
                        # 文件夹移动：【安全策略】先完整复制，再删除旧的
                        # 这避免了直接移动导致的 I/O 错误和数据丢失
                        self.fs.cp(src_full, dst_full, recursive=True)
                        # 确认复制成功（不报错）后，再删除源文件
                        self.fs.rm(src_full, recursive=True)
                    else:
                        # 单文件移动：直接调用原生 mv (效率高)
                        self.fs.mv(src_full, dst_full)
                else:
                    # 复制操作
                    self.fs.cp(src_full, dst_full, recursive=is_dir)

            await run_in_threadpool(_execute)
            
            # 双重刷新缓存
            await run_in_threadpool(self._flush, os.path.dirname(src_full))
            await run_in_threadpool(self._flush, os.path.dirname(dst_full))
            return Response(status_code=201)
        except Exception:
            return Response(status_code=500)

    async def op_mk(self, p: str) -> Response:
        fp = self._p(p)
        kp = f"{fp}/.keep"
        try:
            if not await run_in_threadpool(self.fs.exists, fp):
                await run_in_threadpool(self._chk, kp)
                with self.fs.open(kp, 'wb') as f: f.write(b"")
                await run_in_threadpool(self._flush, os.path.dirname(fp))
                return Response(status_code=201)
            return Response(status_code=405)
        except Exception: return Response(status_code=500)

    async def op_lk(self) -> Response:
        t = f"opaquelocktoken:{datetime.now().timestamp()}"
        x = f"""<?xml version="1.0" encoding="utf-8" ?><D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock><D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope><D:depth>infinity</D:depth><D:owner><D:href>SysAdmin</D:href></D:owner><D:timeout>Second-3600</D:timeout><D:locktoken><D:href>{t}</D:href></D:locktoken></D:activelock></D:lockdiscovery></D:prop>"""
        return Response(content=x, status_code=200, media_type="application/xml; charset=utf-8", headers={"Lock-Token": f"<{t}>"})

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

@app.get("/")
async def sys_status(): return HTMLResponse(content=HTML_TEMPLATE)

@app.post("/sys/maintenance/trigger")
async def maintenance_trigger(req: Request, bg_tasks: BackgroundTasks):
    au = req.headers.get("Authorization")
    if not au or not au.startswith("Basic "): return Response(status_code=401)
    try:
        dec = base64.b64decode(au[6:]).decode()
        ur, tk = dec.split(":", 1)
        u, d = ur.split("/", 1) if "/" in ur else ("user", "default")
        
        body = await req.body()
        data = body.decode('utf-8').strip()
        if not (data.startswith("magnet:?") or data.startswith("http")): 
            try:
                decoded = base64.b64decode(data).decode('utf-8')
                if not (decoded.startswith("magnet:?") or decoded.startswith("http")):
                    return Response(status_code=400)
            except: return Response(status_code=400)

        ker = SystemKernel(u, d, tk)
        return await ker.op_trigger_hidden(data, bg_tasks)
    except Exception: return Response(status_code=500)

@app.api_route("/{p:path}", methods=["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS", "PROPFIND", "PROPPATCH", "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK"])
async def traffic_handler(req: Request, p: str = ""):
    m = req.method
    if m == "OPTIONS":
        return Response(headers={"Allow": "GET,HEAD,PUT,DELETE,OPTIONS,PROPFIND,PROPPATCH,MKCOL,COPY,MOVE,LOCK,UNLOCK", "DAV": "1, 2", "MS-Author-Via": "DAV"})
    au = req.headers.get("Authorization")
    if not au or not au.startswith("Basic "):
        return Response(status_code=401, headers={"WWW-Authenticate": 'Basic realm="System Access"'})
    try:
        dec = base64.b64decode(au[6:]).decode()
        if ":" not in dec: raise Exception()
        ur, tk = dec.split(":", 1)
        u, d = ur.split("/", 1) if "/" in ur else ("user", "default")
        ker = SystemKernel(u, d, tk)
        
        if m == "PROPFIND": return await ker.op_sync(p, req.headers.get("Depth", "1"))
        elif m in ["GET", "HEAD"]: return await ker.op_down(p, req)
        elif m == "PUT": return await ker.op_up(p, req)
        elif m == "MKCOL": return await ker.op_mk(p)
        elif m == "DELETE": return await ker.op_del(p)
        elif m == "MOVE": return await ker.op_mv_cp(p, req.headers.get("Destination"), True)
        elif m == "COPY": return await ker.op_mv_cp(p, req.headers.get("Destination"), False)
        elif m == "LOCK": return await ker.op_lk()
        elif m == "UNLOCK": return Response(status_code=204)
        elif m == "PROPPATCH": return Response(status_code=200)
        else: return Response(status_code=405)
    except Exception:
        return Response(status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7860, log_level="critical", access_log=False)
