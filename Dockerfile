# =============================================================================
# Doc-Pipeline — Production Dockerfile
# =============================================================================
# Two-stage build:
#   Stage 1 (builder): pip-install dependencies into venv
#   Stage 2 (runtime): minimal image with project code + deps
# =============================================================================

# -- Stage 1: Builder --------------------------------------------------------
FROM python:3.12-slim AS builder

# Prevent Python from writing .pyc / buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# requirements.txt 里有 `artesian @ git+https://…` 这种 VCS 直接引用，
# pip 为满足它必须调用 git——而 python:3.12-slim **不带 git**
# （CI 实测报 `Cannot find command 'git'`，2026-10-07）。
# 所以这里确实要装构建工具，只装在 builder 阶段：运行阶段是 COPY venv，
# apt 包不会跟着进最终镜像。
RUN apt-get update \
    && apt-get install --no-install-recommends -y git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt


# -- Stage 2: Runtime --------------------------------------------------------
FROM python:3.12-slim AS runtime

# System setup: non-root user + runtime dirs
RUN addgroup --system --gid 1001 app && \
    adduser --system --uid 1001 --gid 1001 --disabled-password --gecos "" app && \
    mkdir -p /app/checkpoints /app/logs /app/versions /app/backups /data && \
    chown -R app:app /app /data

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Make venv binaries available in PATH
ENV PATH=/opt/venv/bin:$PATH

# Copy project code
COPY --chown=app:app . .

# Runtime data directories (persistent volume mounts)
VOLUME ["/app/checkpoints", "/app/logs", "/app/versions", "/app/backups"]

# Admin API
EXPOSE 8910

# Health check: 纯 Python 探测 /health（slim 镜像无 procps/pgrep，勿依赖）。
# /health 永远免鉴权，容器内直接探 127.0.0.1 即可
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8910/health', timeout=3)"

# Drop privileges
USER app:app

# Default: 常驻 Admin API 服务模式（生产配置绑定 0.0.0.0:8910）。
# 生产配置的非回环绑定强制要求 ADMIN_API_KEY —— 运行时请传入：
#   docker run -d -p 8910:8910 -e ADMIN_API_KEY=change-me nexusloom
# 需要一次性生成文档时覆盖默认 CMD：
#   docker run --rm -v $(pwd)/output:/app/output nexusloom test_input.md -o output/doc.md
ENTRYPOINT ["python", "run.py"]
CMD ["-c", "config.production.json", "--admin", "--daemon"]
