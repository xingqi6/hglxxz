FROM python:3.10-slim

# 禁止生成 pyc 文件，禁止输出缓冲
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 安装必要的系统工具 (aria2)
RUN apt-get update && apt-get install -y aria2 && rm -rf /var/lib/apt/lists/*

COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 将代码复制为 kernel.py，名字更像系统内核
COPY src/main.py kernel.py

# 创建普通用户，模拟系统服务
RUN useradd -m -u 1000 system-runner
# 确保临时目录权限
RUN mkdir -p /tmp/buffer && chown -R system-runner:system-runner /tmp/buffer
USER system-runner
ENV PATH="/home/system-runner/.local/bin:$PATH"

# 启动命令
CMD ["python", "kernel.py"]
