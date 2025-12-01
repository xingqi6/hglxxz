FROM python:3.10-slim

WORKDIR /app

# [新增] 安装 aria2 用于下载磁力链
RUN apt-get update && apt-get install -y aria2 && rm -rf /var/lib/apt/lists/*

COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/main.py .

# 创建一个普通用户运行，增加安全性
RUN useradd -m -u 1000 user
# 确保临时下载目录有权限
RUN mkdir -p /app/cache && chown -R user:user /app/cache
USER user
ENV PATH="/home/user/.local/bin:$PATH"

CMD ["python", "main.py"]
