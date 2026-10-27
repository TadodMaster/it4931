FROM python:3.11.13-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/python/common:/app/python/generator:/app/python/oracle:/app/python/rules-python:/app/python/bench:/app/python/query-api \
    LK_CONFIG=/app/shared/config/demo.yaml DATA_ROOT=/data
WORKDIR /app
COPY python/requirements.txt python/requirements.txt
RUN pip install --no-cache-dir -r python/requirements.txt
# docker CLI only (talks to the host daemon through the mounted socket): bench uses it for chaos tests, stats, job submit
RUN python -c "import platform,tarfile,io,urllib.request as u; m={'x86_64':'x86_64','amd64':'x86_64','aarch64':'aarch64','arm64':'aarch64'}[platform.machine().lower()]; d=u.urlopen('https://download.docker.com/linux/static/stable/'+m+'/docker-27.3.1.tgz').read(); t=tarfile.open(fileobj=io.BytesIO(d)); f=t.extractfile('docker/docker'); open('/usr/local/bin/docker','wb').write(f.read())" && chmod +x /usr/local/bin/docker
COPY shared ./shared
COPY python ./python
WORKDIR /app/python
