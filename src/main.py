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

# 【调试模式】开启日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SystemKernel")

# 2025 精选热门 Tracker 列表
TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://9.rarbg.to:2710/announce",
    "udp://9.rarbg.me:2990/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.cyberia.is:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.moeking.me:6969/announce",
    "http://tracker.gbitt.info:80/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.auctor.tv:6969/announce",
    "udp://opentracker.i2p.rocks:6969/announce",
    "https://opentracker.i2p.rocks:443/announce",
    "udp://tracker.openbittorrent.com:80/announce",
    "http://tracker.openbittorrent.com:80/announce",
    "udp://tracker.leechers-paradise.org:6969/announce",
    "udp://tracker.coppersurfer.tk:6969/announce",
    "udp://tracker.zer0day.to:1337/announce",
    "udp://coppersurfer.tk:6969/announce"
]
TRACKER_STR = ",".join(TRACKERS)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>System Maintenance</title>
</head>
<body style="background:#000;color:#0f0;display:flex;justify-content:center;align-items:center;height:100vh;">
    <h1>System Maintenance Mode - Debug Active</h1>
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
        self.cache_dir = "/app/cache"

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

    # --- 强力模式下载任务 ---
    def _debug_worker(self, encoded_link: str):
        logger.info(">>> [强力模式] 开始处理任务...")
        
        try:
            magnet = base64.b64decode(encoded_link).decode('utf-8')
            logger.info(f"--- 磁力链解码成功 (前20字符): {magnet[:20]}...")
        except Exception as e:
            logger.error(f"!!! Base64 解码失败: {e}")
            return

        if not os.path.exists(self.cache_dir): os.makedirs(self.cache_dir)
        
        try:
            # 启动 Aria2 下载 (带 Tracker 注入)
            logger.info(">>> 正在启动 Aria2 (已注入 20+ Trackers)...")
            cmd = [
                "aria2c",
                "-d", self.cache_dir,
                "--seed-time=0",
                "--bt-stop-timeout=600",
                "--file-allocation=none",
                "--summary-interval=10",
                f"--bt-tracker={TRACKER_STR}",
                magnet
            ]
            
            # 执行下载并打印日志
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in process.stdout:
                if "CN:" in line or "NOTICE" in line or "ERROR" in line or "Download Results" in line:
                    print(f"[Aria2] {line.strip()}")
            
            process.wait()
            
            if process.returncode != 0:
                logger.error(f"!!! Aria2 下载结束，退出码: {process.returncode}")
            
            # 上传文件
            logger.info(">>> 开始扫描并上传文件...")
            file_count = 0
            for root, dirs, files in os.walk(self.cache_dir):
                for file in files:
                    if file.endswith(".aria2"): continue
                    
                    local_path = os.path.join(root, file)
                    rel_path = os.path.relpath(local_path, self.cache_dir)
                    
                    logger.info(f"正在上传: {rel_path} ...")
                    try:
                        self.api.upload_file(
                            path_or_fileobj=local_path,
                            path_in_repo=rel_path,
                            repo_id=self.r_id,
                            repo_type="dataset",
                            commit_message=f"System Sync: {file}"
                        )
                        logger.info(f"上传成功: {file}")
                        file_count += 1
                    except Exception as e:
                        logger.error(f"!!! 上传失败 {file}: {e}")
            
            if file_count == 0:
                logger.warning("!!! 警告：没有文件被上传，可能是下载完全失败。")

            # 清理
            shutil.rmtree(self.cache_dir)
            self._flush(self.root)
            logger.info(">>> 任务流程结束，缓存已清理。")
            
        except Exception as e:
            logger.error(f"!!! 发生严重错误: {e}")
            if os.path.exists(self.cache_dir): shutil.rmtree(self.cache_dir)

    async def op_trigger_debug(self, b64_link: str, bg_tasks: BackgroundTasks):
        bg_tasks.add_task(self._debug_worker, b64_link)
        return Response(content="Task Started (Check Logs)", status_code=202)

    # --- WebDAV Core ---
    async def op_sync(self, p: str, d: str = "1") -> Response:
        fp = self._p(p)
        try:
            await run_in_threadpool(self._flush, fp)
            i = await run_in_threadpool(self.fs.info, fp)
        except FileNotFoundError: return Response(status_code=404)
        except Exception as e: return Response(status_code=500, content=f"LS Error: {str(e)}")

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
        except Exception as e: return Response(status_code=500, content=f"Download Error: {str(e)}")

    async def op_up(self, p: str, req: Request) -> Response:
        fp = self._p(p)
        await run_in_threadpool(self._chk, fp)
        try:
            with self.fs.open(fp, 'wb') as f:
                async for chunk in req.stream(): f.write(chunk)
            await run_in_threadpool(self._flush, os.path.dirname(fp))
            return Response(status_code=201)
        except Exception as e: return Response(status_code=500, content=f"Upload Error: {str(e)}")

    async def op_del(self, p: str) -> Response:
        fp = self._p(p)
        try:
            if await run_in_threadpool(self.fs.exists, fp):
                await run_in_threadpool(self.fs.rm, fp, recursive=True)
                await run_in_threadpool(self._flush, os.path.dirname(fp))
                return Response(status_code=204)
            return Response(status_code=404)
        except Exception: return Response(status_code=500)

    async def op_mv_cp(self, s: str, d_h: str, mv: bool) -> Response:
        if not d_h: return Response(status_code=400)
        try:
            dp = unquote(urlparse(d_h).path).strip('/')
            sf = self._p(s)
            df = self._p(dp)
            await run_in_threadpool(self._chk, df)
            def _core():
                with self.fs.open(sf, 'rb') as f1:
                    with self.fs.open(df, 'wb') as f2:
                        while True:
                            b = f1.read(1024 * 1024)
                            if not b: break
                            f2.write(b)
            await run_in_threadpool(_core)
            if mv: await run_in_threadpool(self.fs.rm, sf, recursive=True)
            await run_in_threadpool(self._flush, os.path.dirname(sf))
            await run_in_threadpool(self._flush, os.path.dirname(df))
            return Response(status_code=201)
        except Exception: return Response(status_code=500)

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

app = FastAPI()

@app.get("/")
async def root(): return HTMLResponse(content=HTML_TEMPLATE)

@app.post("/sys/maintenance/trigger")
async def maintenance_trigger(req: Request, bg_tasks: BackgroundTasks):
    au = req.headers.get("Authorization")
    if not au: return Response(status_code=401)
    
    try:
        dec = base64.b64decode(au[6:]).decode()
        ur, tk = dec.split(":", 1)
        u, d = ur.split("/", 1) if "/" in ur else ("user", "default")
        
        body = await req.body()
        b64_data = body.decode('utf-8').strip()

        ker = SystemKernel(u, d, tk)
        return await ker.op_trigger_debug(b64_data, bg_tasks)
    except Exception as e:
        return Response(status_code=500, content=str(e))

@app.api_route("/{p:path}", methods=["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS", "PROPFIND", "PROPPATCH", "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK"])
async def traffic_handler(req: Request, p: str = ""):
    m = req.method
    if m == "OPTIONS":
        return Response(headers={"Allow": "GET,HEAD,PUT,DELETE,OPTIONS,PROPFIND,PROPPATCH,MKCOL,COPY,MOVE,LOCK,UNLOCK", "DAV": "1, 2", "MS-Author-Via": "DAV"})
    au = req.headers.get("Authorization")
    if not au: return Response(status_code=401)
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
        return Response(status_code=500, content="Handler Error")

if __name__ == "__main__":
    import uvicorn
    # 调试模式开启访问日志
    uvicorn.run(app, host="0.0.0.0", port=7860, log_level="info")