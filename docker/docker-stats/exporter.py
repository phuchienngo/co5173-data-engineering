"""CPU and memory per container for Prometheus (BR5 monitoring, plan B1).

cAdvisor can't map containers on this Docker host (containerd image store),
so this reads the Docker API's stats instead. Standard library only. Metric
names follow cAdvisor's, so the dashboards would work with either. Only the
containers of this exporter's own Compose project are exported.

Prometheus scrapes http://docker-stats:9417/metrics.
"""

import http.client
import json
import os
import socket
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SOCKET = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")
PORT = int(os.environ.get("PORT", "9417"))


class DockerConnection(http.client.HTTPConnection):
    def __init__(self):
        super().__init__("docker", timeout=10)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(SOCKET)


def docker_get(path):
    conn = DockerConnection()
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"{path}: HTTP {response.status}")
        return json.loads(body)
    finally:
        conn.close()


def own_project():
    # The container's hostname is its short id unless the service sets one.
    me = docker_get(f"/containers/{socket.gethostname()}/json")
    return me["Config"]["Labels"].get("com.docker.compose.project")


PROJECT = None


def container_figures(container):
    name = container["Names"][0].lstrip("/")
    stats = docker_get(f"/containers/{container['Id']}/stats?stream=false&one-shot=true")
    info = docker_get(f"/containers/{container['Id']}/json")
    memory = stats.get("memory_stats", {})
    usage = memory.get("usage", 0)
    # Working set as cAdvisor and `docker stats` define it: usage minus the
    # page cache the kernel can drop (cgroup v2: inactive_file).
    inactive = memory.get("stats", {}).get("inactive_file", 0)
    nano_cpus = info["HostConfig"].get("NanoCpus") or 0
    return name, {
        "container_cpu_usage_seconds_total": stats["cpu_stats"]["cpu_usage"]["total_usage"] / 1e9,
        "container_memory_usage_bytes": usage,
        "container_memory_working_set_bytes": max(usage - inactive, 0),
        "container_spec_memory_limit_bytes": memory.get("limit", 0),
        "container_spec_cpus": nano_cpus / 1e9,
    }


HELP = {
    "container_cpu_usage_seconds_total": ("counter", "CPU time used by the container, in seconds."),
    "container_memory_usage_bytes": ("gauge", "Memory used, including page cache."),
    "container_memory_working_set_bytes": ("gauge", "Memory used minus reclaimable page cache."),
    "container_spec_memory_limit_bytes": ("gauge", "Memory limit of the container."),
    "container_spec_cpus": ("gauge", "CPU limit of the container (0 = none)."),
}


def render():
    containers = [c for c in docker_get("/containers/json")
                  if c["Labels"].get("com.docker.compose.project") == PROJECT]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(safe_figures, containers))
    lines = []
    for metric, (kind, text) in HELP.items():
        lines.append(f"# HELP {metric} {text}")
        lines.append(f"# TYPE {metric} {kind}")
        for result in results:
            if result:
                name, figures = result
                lines.append(f'{metric}{{name="{name}"}} {figures[metric]}')
    lines.append("# TYPE docker_stats_containers gauge")
    lines.append(f"docker_stats_containers {sum(1 for r in results if r)}")
    return "\n".join(lines) + "\n"


def safe_figures(container):
    try:
        return container_figures(container)
    except Exception as exc:  # a container stopping mid-scrape must not fail the scrape
        print(f"skipped {container['Names']}: {exc}", flush=True)
        return None


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_error(404)
            return
        try:
            body = render().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
        except Exception as exc:
            body = f"error: {exc}\n".encode()
            self.send_response(500)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # no access log
        pass


if __name__ == "__main__":
    PROJECT = own_project()
    print(f"exporting containers of project {PROJECT} on :{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
