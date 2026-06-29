"""
Tiny Docker Socket Client
=========================
Direct REST API calls to Docker daemon via Unix socket.
Replaces subprocess.run(['docker', ...]) which requires docker CLI.

API docs: https://docs.docker.com/engine/api/v1.43/
"""
from __future__ import annotations

import json
import socket
from typing import Any, Dict, List, Optional
from urllib.parse import quote

DOCKER_SOCKET = "/var/run/docker.sock"


def _request(method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
    """Make HTTP request to Docker daemon via Unix socket."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect(DOCKER_SOCKET)
        body_str = ""
        headers = ""
        if body is not None:
            body_str = json.dumps(body)
            headers = (
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(body_str)}\r\n"
            )
        req = (
            f"{method} {path} HTTP/1.1\r\n"
            f"Host: docker\r\n"
            f"{headers}"
            f"Connection: close\r\n"
            f"\r\n"
            f"{body_str}"
        )
        sock.sendall(req.encode())

        # Read response
        chunks = []
        while True:
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                break
            if not chunk:
                break
            chunks.append(chunk)
        response = b"".join(chunks).decode("utf-8", errors="ignore")
    finally:
        sock.close()

    if not response:
        return None

    # Split header and body
    parts = response.split("\r\n\r\n", 1)
    if len(parts) != 2:
        return None
    head, body_str = parts
    # Strip chunked encoding if present
    if "chunked" in head.lower():
        # Decode chunked transfer encoding
        decoded = []
        body_bytes = body_str.encode("utf-8")
        idx = 0
        while idx < len(body_bytes):
            line_end = body_bytes.find(b"\r\n", idx)
            if line_end == -1:
                break
            size_str = body_bytes[idx:line_end].decode("utf-8").strip()
            try:
                size = int(size_str, 16)
            except ValueError:
                break
            if size == 0:
                break
            decoded.append(body_bytes[line_end + 2:line_end + 2 + size])
            idx = line_end + 2 + size + 2  # skip \r\n after chunk
        body_str = b"".join(decoded).decode("utf-8", errors="ignore")
    else:
        # Remove any leading non-JSON chars (e.g., junk headers in body)
        import re
        m = re.search(r'(\{.*\}|\[.*\])', body_str, re.DOTALL)
        if m:
            body_str = m.group(1)

    try:
        return json.loads(body_str)
    except json.JSONDecodeError:
        return body_str[:500]


def list_containers(all: bool = True) -> List[Dict[str, Any]]:
    """List all containers (running + stopped)."""
    path = "/containers/json?all=true" if all else "/containers/json"
    result = _request("GET", path)
    if not isinstance(result, list):
        return []
    return result


def container_ps(project: str = "smart-scenic-bigdata") -> List[Dict[str, Any]]:
    """List containers using docker compose project's label."""
    all_containers = list_containers(all=True)
    return [
        c for c in all_containers
        if c.get("Labels", {}).get("com.docker.compose.project") == project
    ]


def exec_in_container(container: str, cmd: List[str], timeout: int = 30) -> Dict[str, Any]:
    """Execute command in running container. Returns {stdout, stderr, exit_code}."""
    # Create exec instance
    exec_id = _request("POST", f"/containers/{container}/exec", {
        "Cmd": cmd,
        "AttachStdout": True,
        "AttachStderr": True,
    })
    if not exec_id or "Id" not in exec_id:
        return {"stdout": "", "stderr": "exec create failed", "exit_code": -1}

    # Start exec
    _request("POST", f"/exec/{exec_id['Id']}/start")

    # Get result
    result = _request("GET", f"/exec/{exec_id['Id']}/json")
    return {
        "stdout": "",  # streamed, hard to collect; use inspect below
        "stderr": "",
        "exit_code": result.get("ExitCode", -1) if isinstance(result, dict) else -1,
    }


def exec_capture(container: str, cmd: List[str], timeout: int = 30) -> Dict[str, Any]:
    """Execute command and capture stdout/stderr (using exec inspect output)."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(DOCKER_SOCKET)
        # POST /containers/{name}/exec
        body = json.dumps({"Cmd": cmd, "AttachStdout": True, "AttachStderr": True})
        req = (
            f"POST /containers/{container}/exec HTTP/1.1\r\n"
            f"Host: docker\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n\r\n{body}"
        )
        sock.sendall(req.encode())
        resp = b""
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            resp += chunk
        resp_text = resp.decode("utf-8", errors="ignore")
        # Parse exec id from response
        import re
        m = re.search(r'"Id"\s*:\s*"([^"]+)"', resp_text)
        if not m:
            return {"stdout": "", "stderr": resp_text[:200], "exit_code": -1}
        exec_id = m.group(1)
    finally:
        sock.close()

    # Start the exec
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(DOCKER_SOCKET)
        body = json.dumps({"Detach": False, "Tty": False})
        req = (
            f"POST /exec/{exec_id}/start HTTP/1.1\r\n"
            f"Host: docker\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n\r\n{body}"
        )
        sock.sendall(req.encode())
        resp = b""
        while True:
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                break
            if not chunk:
                break
            resp += chunk
        resp_text = resp.decode("utf-8", errors="ignore")
    finally:
        sock.close()

    # Parse stdout/stderr from docker stream format
    # Format: stream header (8 bytes: type[1] + size[3] + data[size])
    # But Python raw HTTP doesn't decode this. Just return the raw body stripped of HTTP headers.
    if "\r\n\r\n" in resp_text:
        body = resp_text.split("\r\n\r\n", 1)[1]
    else:
        body = resp_text

    # Get exit code
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect(DOCKER_SOCKET)
        req = f"GET /exec/{exec_id}/json HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n"
        sock.sendall(req.encode())
        resp = b""
        while True:
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                break
            if not chunk:
                break
            resp += chunk
        import re
        m = re.search(r'"ExitCode"\s*:\s*(-?\d+)', resp.decode("utf-8", errors="ignore"))
        exit_code = int(m.group(1)) if m else -1
    finally:
        sock.close()

    return {"stdout": body, "stderr": "", "exit_code": exit_code}