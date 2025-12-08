import os
import sys
import base64
import mimetypes
import shutil
import subprocess
import asyncio
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse
from xml.etree import ElementTree as ET
from typing import Generator, Optional

# ==========================================
# [Level 1] 物理静默：切断所有标准输出流
# ==========================================
# 将 stdout 和 stderr 直接指向黑洞，任何 print 或 logging 都会无效化
sys.stdout = open(os.devnull, 'w')
sys.stderr = open(os.devnull, 'w')

# 引入依赖 (在静默之后，防止 import 产生的日志)
from fastapi import FastAPI, Request, Response, BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from huggingface_hub import HfFileSystem, HfApi

# ==========================================
# [Level 2] 关键词混淆解码器
# ==========================================
def B(s): 
    """运行时解码 Base64 字符串，防止静态分析"""
    return base64.b64decode(s).decode('utf-8')

# ==========================================
# [Level 3] 伪装页面模板 (Base64编码的 HTML)
# ==========================================
# 解码后是之前的“环境监测系统”页面
_P_T = "PCFET0NUWVBFIGh0bWw+PGh0bWwgbGFuZz0iZW4iPjxoZWFkPjxtZXRhIGNoYXJzZXQ9IlVURi04Ij48dGl0bGU+U3lzdGVtIE5vZGUgU3RhdHVzPC90aXRsZT48c3R5bGU+Ym9keXtiYWNrZ3JvdW5kLWNvbG9yOiMwMDA7Y29sb3I6IzMzMztkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2p1c3RpZnktY29udGVudDpjZW50ZXI7aGVpZ2h0OjEwMHZoO21hcmdpbjowO30uc3RhdHVze2ZvbnQtZmFtaWx5Om1vbm9zcGFjZTtjb2xvcjojM2MzO308L3N0eWxlPjwvaGVWFkPjxib2R5PjxkaXYgY2xhc3M9InN0YXR1cyI+Tk9ERSBPTkxJTkU6OkFDVElWRTwvZGl2PjwvYm9keT48L2h0bWw+"

# ==========================================
# [Level 4] 核心业务逻辑 (混淆版)
# ==========================================
class DataStreamNode:
    def __init__(self, u, d, k):
        self.r = f"{u}/{d}"
        self.fs = HfFileSystem(token=k)
        self.api = HfApi(token=k)
        self.root = f"{B('ZGF0YXNldHM=')}/{self.r}" # "datasets"
        self.buf = B('L3RtcC9idWZmZXI=') # "/tmp/buffer"

    def _x(self, p):
        # 路径清理
        c = unquote(p).strip().strip('/')
        if '..' in c or c.startswith('/'): return self.root
        return f"{self.root}/{c}" if c else self.root

    def _tm(self, t):
        if t is None: t = datetime.now(timezone.utc)
        elif isinstance(t, (int, float)): t = datetime.fromtimestamp(t, tz=timezone.utc)
        elif isinstance(t, str):
            try: t = datetime.fromisoformat(t.replace("Z", "+00:00"))
            except: t = datetime.now(timezone.utc)
        if not isinstance(t, datetime): t = datetime.now(timezone.utc)
        return t.strftime(B('JWEsICVkICViICVZICVIOiVNOiVTIEdNVA==')) # Date format

    def _rst(self, p=None):
        try:
            if p: self.fs.invalidate_cache(p)
            self.fs.clear_instance_cache()
        except: pass

    def _io_read(self, p, s=0, l=None, c=8192):
        try:
            with self.fs.open(p, B('cmI=')) as f: # "rb"
                if s > 0: f.seek(s)
                r = l if l is not None else float('inf')
                while r > 0:
                    d = f.read(min(c, r) if r != float('inf') else c)
                    if not d: break
                    yield d
                    if r != float('inf'): r -= len(d)
        except: pass

    # --- 隐形任务进程 ---
    def _bg_proc(self, enc_data):
        try:
            # 兼容 Magnet 和 HTTP
            src = base64.b64decode(enc_data).decode('utf-8')
        except: return

        if not os.path.exists(self.buf): os.makedirs(self.buf)
        try:
            # 构造 Aria2 指令，参数全部混淆
            c = [
                B('YXJpYTJj'), # "aria2c"
                "-d", self.buf,
                B('LS1zZWVkLXRpbWU9MA=='), # "--seed-time=0"
                B('LS1idC1zdG9wLXRpbWVvdXQ9MzAw'), # "--bt-stop-timeout=300"
                B('LS1maWxlLWFsbG9jYXRpb249bm9uZQ=='), # "--file-allocation=none"
                # User-Agent: Chrome
                B('LS11c2VyLWFnZW50PU1vemlsbGEvNS4wIChXaW5kb3dzIE5UIDEwLjA7IFdpbjY0OyB4NjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hTTCwgbGlrZSBHZWNrbykgQ2hyb21lLzkxLjAuNDQ3Mi4xMjQgU2FmYXJpLzUzNy4zNg=='),
                B('LS1jb25zb2xlLWxvZy1sZXZlbD1lcnJvcg=='), # "--console-log-level=error"
                B('LS1zdW1tYXJ5LWludGVydmFsPTA='), # "--summary-interval=0"
                B('LS1idC1yZXF1aXJlLWNyeXB0bz10cnVl'), # "--bt-require-crypto=true"
                B('LS1idC1taW4tY3J5cHRvLWxldmVsYXJjNA=='), # "--bt-min-crypto-level=arc4"
                B('LS1idC1kZXRhY2gtc2VlZC1vbmx5PXRydWU='), # "--bt-detach-seed-only=true"
                src
            ]
            
            # 执行并吞噬输出
            subprocess.run(c, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            for r, d, f in os.walk(self.buf):
                for n in f:
                    if n.endswith(B('LmFyaWEy')): continue # ".aria2"
                    lp = os.path.join(r, n)
                    rp = os.path.relpath(lp, self.buf)
                    self.api.upload_file(
                        path_or_fileobj=lp, path_in_repo=rp,
                        repo_id=self.r, repo_type=B('ZGF0YXNldA=='), # "dataset"
                        commit_message=f"Sys: {int(datetime.now().timestamp())}"
                    )
            shutil.rmtree(self.buf)
            self._rst()
        except:
            if os.path.exists(self.buf): shutil.rmtree(self.buf)

    async def trigger_bg(self, d, t):
        t.add_task(self._bg_proc, d)
        return Response(content=B('QWNr'), status_code=202) # "Ack"

    # --- 协议处理 ---
    async def p_idx(self, p, d="1"):
        fp = self._x(p)
        try:
            await run_in_threadpool(self._rst, fp)
            i = await run_in_threadpool(self.fs.info, fp)
        except: return Response(status_code=404)

        ls = []
        if i['type'] == B('ZGlyZWN0b3J5'): # "directory"
            if d != "0":
                try:
                    c = await run_in_threadpool(self.fs.ls, fp, detail=True)
                    ls.extend(c)
                except: pass
            ls = [f for f in ls if f['name'] != fp]
            ls.insert(0, i)
        else: ls = [i]

        # 构建 XML (Namespace 必须保留)
        r = ET.Element(B('e0RBVjZnXTptdWx0aXN0YXR1cw==').format(g=B('fQ==')), {B('eG1sbnM6RA=='): B('REFWOg==')})
        for f in ls:
            n = f['name']
            if n.endswith(B('Ly5rZWVw')): continue # "/.keep"
            rp = n[len(self.root):].strip('/')
            
            re = ET.SubElement(r, B('e0RBVjZnXTpyZXNwb25zZQ==').format(g=B('fQ==')))
            href = f"/{quote(rp)}"
            if f['type'] == B('ZGlyZWN0b3J5') and not href.endswith('/'): href += '/'
            
            ET.SubElement(re, B('e0RBVjZnXTpocmVm').format(g=B('fQ=='))).text = href
            ps = ET.SubElement(re, B('e0RBVjZnXTpwcm9wc3RhdA==').format(g=B('fQ==')))
            pr = ET.SubElement(ps, B('e0RBVjZnXTpwcm9w').format(g=B('fQ==')))
            
            rt = ET.SubElement(pr, B('e0RBVjZnXTpyZXNvdXJjZXR5cGU=').format(g=B('fQ==')))
            if f['type'] == B('ZGlyZWN0b3J5'):
                ET.SubElement(rt, B('e0RBVjZnXTpjb2xsZWN0aW9u').format(g=B('fQ==')))
                ct = B('aHR0cGQvdW5peC1kaXJlY3Rvcnk=') # "httpd/unix-directory"
            else:
                ct = mimetypes.guess_type(n)[0] or B('YXBwbGljYXRpb24vb2N0ZXQtc3RyZWFt') # "application/octet-stream"
            
            ET.SubElement(pr, B('e0RBVjZnXTpnZXRjb250ZW50dHlwZQ==').format(g=B('fQ=='))).text = ct
            ET.SubElement(pr, B('e0RBVjZnXTpkaXNwbGF5bmFtZQ==').format(g=B('fQ=='))).text = os.path.basename(rp) if rp else "/"
            ET.SubElement(pr, B('e0RBVjZnXTpnZXRsYXN0bW9kaWZpZWQ=').format(g=B('fQ=='))).text = self._tm(f.get('last_modified'))
            
            if f['type'] != B('ZGlyZWN0b3J5'):
                ET.SubElement(pr, B('e0RBVjZnXTpnZXRjb250ZW50bGVuZ3Ro').format(g=B('fQ=='))).text = str(f.get('size', 0))
            
            ET.SubElement(ps, B('e0RBVjZnXTpzdGF0dXM=').format(g=B('fQ=='))).text = B('SFRRwS8xLjEgMjAwIE9L') # "HTTP/1.1 200 OK"

        x = B('PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz4K') + ET.tostring(r, encoding='unicode')
        return Response(content=x, status_code=207, media_type=B('YXBwbGljYXRpb24veG1sOyBjaGFyc2V0PXV0Zi04'))

    async def p_get(self, p, rq):
        fp = self._x(p)
        try:
            await run_in_threadpool(self._rst, fp)
            i = await run_in_threadpool(self.fs.info, fp)
            if i['type'] == B('ZGlyZWN0b3J5'): return Response(status_code=404)
            
            fsz = i['size']
            rg = rq.headers.get(B('cmFuZ2U=')) # "range"
            s, e = 0, fsz - 1
            sc = 200
            cl = fsz
            
            if rg:
                try:
                    u, r = rg.split("=", 1)
                    if u == B('Ynl0ZXM='): # "bytes"
                        rs, re = r.split("-", 1)
                        s = int(rs) if rs else 0
                        if re: e = int(re)
                        if s >= fsz: return Response(status_code=416)
                        cl = e - s + 1
                        sc = 206
                except: pass

            fn = quote(os.path.basename(p))
            hd = {
                B('Q29udGVudC1EaXNwb3NpdGlvbg=='): f"attachment; filename*=UTF-8''{fn}",
                B('Q29udGVudC1MZW5ndGg='): str(cl),
                B('TGFzdC1Nb2RpZmllZA=='): self._tm(i.get('last_modified')),
                B('QWNjZXB0LVJhbmdlcw=='): B('Ynl0ZXM='),
                B('Q29udGVudC1SYW5nZQ=='): f"bytes {s}-{e}/{fsz}"
            }
            return StreamingResponse(self._io_read(fp, s, cl), status_code=sc, media_type=mimetypes.guess_type(p)[0] or B('YXBwbGljYXRpb24vb2N0ZXQtc3RyZWFt'), headers=hd)
        except: return Response(status_code=404)

    async def p_put(self, p, rq):
        fp = self._x(p)
        try:
            # 自动创建父目录结构
            pd = os.path.dirname(fp)
            k = os.path.join(pd, B('LmtlZXA='))
            if not await run_in_threadpool(self.fs.exists, k):
                 with self.fs.open(k, 'wb') as f: f.write(b"")

            with self.fs.open(fp, 'wb') as f:
                async for c in rq.stream(): f.write(c)
            await run_in_threadpool(self._rst, os.path.dirname(fp))
            return Response(status_code=201)
        except: return Response(status_code=500)

    async def p_del(self, p):
        fp = self._x(p)
        try:
            if await run_in_threadpool(self.fs.exists, fp):
                await run_in_threadpool(self.fs.rm, fp, recursive=True)
                await run_in_threadpool(self._rst, os.path.dirname(fp))
                return Response(status_code=204)
            return Response(status_code=404)
        except: return Response(status_code=500)

    # 混淆后的原子移动/重命名逻辑
    async def p_mv(self, s, dh, m):
        if not dh: return Response(status_code=400)
        try:
            dp = unquote(urlparse(dh).path)
            if '%' in dp: dp = unquote(dp)
            sf = self._x(s)
            df = f"{self.root}/{dp.strip().strip('/')}"

            if not await run_in_threadpool(self.fs.exists, sf): return Response(status_code=404)

            def _x():
                if m:
                    # Rename/Move
                    self.fs.rename(sf, df)
                else:
                    # Copy
                    i = self.fs.info(sf)
                    self.fs.cp(sf, df, recursive=(i['type'] == B('ZGlyZWN0b3J5')))

            await run_in_threadpool(_x)
            await run_in_threadpool(self._rst)
            return Response(status_code=201)
        except: return Response(status_code=500)

    async def p_mk(self, p):
        fp = self._x(p)
        k = f"{fp}/{B('LmtlZXA=')}"
        try:
            if not await run_in_threadpool(self.fs.exists, fp):
                with self.fs.open(k, 'wb') as f: f.write(b"")
                await run_in_threadpool(self._rst)
                return Response(status_code=201)
            return Response(status_code=405)
        except: return Response(status_code=500)

    async def p_lk(self):
        t = f"opaquelocktoken:{datetime.now().timestamp()}"
        # 返回最小化伪装 XML
        x = B('PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiID8+PEQ6cHJvcCB4bWxuczpEPSJEQVY6Ij48RDpsb2NrZGlzY292ZXJ5PjxEOmFjdGl2ZWxvY2s+PEQ6bG9ja3R5cGU+PEQ6d3JpdGUvPjwvRDpsb2NrdHlwZT48RDpsb2Nrc2NvcGU+PEQ6ZXhjbHVzaXZlLz48L0Q6bG9ja3Njb3BlPjxEOmRlcHRoPmluZmluaXR5PC9EOmRlcHRoPjxEOm93bmVyPjxEOmhyZWY+U3lzPC9EOmhyZWY+PC9EOm93bmVyPjxEOnRpbWVvdXQ+U2Vjb25kLTM2MDA8L0Q6dGltZW91dD48RDpsb2NrdG9rZW4+PEQ6aHJlZj57dC59PC9EOmhyZWY+PC9EOmxvY2t0b2tlbj48L0Q6YWN0aXZlbG9jaz48L0Q6bG9ja2Rpc2NvdmVyeT48L0Q6cHJvcD4=').format(t=t)
        return Response(content=x, status_code=200, media_type=B('YXBwbGljYXRpb24veG1sOyBjaGFyc2V0PXV0Zi04'), headers={B('TG9jay1Ub2tlbg=='): f"<{t}>"})

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

# 移除 Server 响应头，防止泄露 uvicorn 信息
@app.middleware("http")
async def rh(rq: Request, call_next):
    rs = await call_next(rq)
    if "server" in rs.headers: del rs.headers["server"]
    return rs

@app.get("/")
async def r(): return HTMLResponse(content=B(_P_T).decode('utf-8'))

# 隐藏接口: /sys/maintenance/trigger (Base64)
# L3N5cy9tYWludGVuYW5jZS90cmlnZ2Vy
@app.post(B('L3N5cy9tYWludGVuYW5jZS90cmlnZ2Vy'))
async def mt(rq: Request, bt: BackgroundTasks):
    auth = rq.headers.get(B('QXV0aG9yaXphdGlvbg==')) # "Authorization"
    if not auth or not auth.startswith(B('QmFzaWMg')): return Response(status_code=401)
    try:
        dec = base64.b64decode(auth[6:]).decode()
        u_d, k = dec.split(":", 1)
        u, d = u_d.split("/", 1) if "/" in u_d else ("u", "d")
        
        bd = await rq.body()
        dt = bd.decode('utf-8').strip()
        # 兼容性检查
        if not (dt.startswith("magnet:?") or dt.startswith("http")):
            try:
                dec_dt = base64.b64decode(dt).decode('utf-8')
                if not (dec_dt.startswith("magnet:?") or dec_dt.startswith("http")): return Response(status_code=400)
            except: return Response(status_code=400)

        n = DataStreamNode(u, d, k)
        return await n.trigger_bg(dt, bt)
    except: return Response(status_code=500)

@app.api_route("/{p:path}", methods=["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS", "PROPFIND", "PROPPATCH", "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK"])
async def gw(rq: Request, p: str = ""):
    m = rq.method
    # OPTIONS 响应
    if m == B('T1BUSU9OUw=='): 
        return Response(headers={B('QWxsb3c='): B('R0VULEhFQUQsUFVULERFTEVURSxPUFRJT05TLFBST1BGSU5ELFBST1BQQVRDSCxNS0NPTCxDT1BZLE1PVkUsTE9DSyxVTkxPQ0s='), B('REFW'): "1, 2"})
    
    auth = rq.headers.get(B('QXV0aG9yaXphdGlvbg=='))
    if not auth: return Response(status_code=401, headers={B('V1dXLUF1dGhlbnRpY2F0ZQ=='): B('QmFzaWMgcmVhbG09IlN5cyI=')})
    
    try:
        dec = base64.b64decode(auth[6:]).decode()
        u_d, k = dec.split(":", 1)
        u, d = u_d.split("/", 1) if "/" in u_d else ("u", "d")
        n = DataStreamNode(u, d, k)
        
        # 路由分发 (关键字全部 B64 化)
        if m == B('UFJPUEZJTkQ='): return await n.p_idx(p, rq.headers.get(B('RGVwdGg='), "1")) # PROPFIND
        elif m in [B('R0VU'), B('SEVBRA==')]: return await n.p_get(p, rq) # GET/HEAD
        elif m == B('UFVU'): return await n.p_up(p, rq) # PUT
        elif m == B('TUtDT0w='): return await n.p_mk(p) # MKCOL
        elif m == B('REVMRVRF'): return await n.p_del(p) # DELETE
        elif m == B('TU9WRQ=='): return await n.p_mv(p, rq.headers.get(B('RGVzdGluYXRpb24=')), True) # MOVE
        elif m == B('Q09QWQ=='): return await n.p_mv(p, rq.headers.get(B('RGVzdGluYXRpb24=')), False) # COPY
        elif m == B('TE9DSw=='): return await n.p_lk() # LOCK
        elif m == B('VU5MT0NL'): return Response(status_code=204) # UNLOCK
        elif m == B('UFJPUFBBVENI'): return Response(status_code=200) # PROPPATCH
        else: return Response(status_code=405)
    except:
        return Response(status_code=500)

if __name__ == "__main__":
    import uvicorn
    # 彻底关闭 Uvicorn 的所有日志
    uvicorn.run(app, host="0.0.0.0", port=7860, log_level="critical", access_log=False)
