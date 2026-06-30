"""Docker exec via socket — CORRECT stream frame format (8-byte header)."""
import socket, struct, json, re

DOCKER_SOCKET = "/var/run/docker.sock"


def _request_raw(method, path, body=None, timeout=30):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect(DOCKER_SOCKET)
    body_str = json.dumps(body) if body else ""
    req = f"{method} {path} HTTP/1.1\r\nHost: docker\r\nContent-Type: application/json\r\nContent-Length: {len(body_str)}\r\nConnection: close\r\n\r\n{body_str}"
    sock.sendall(req.encode())
    chunks = []
    while True:
        try: c = sock.recv(65536)
        except socket.timeout: break
        if not c: break
        chunks.append(c)
    sock.close()
    return b"".join(chunks)


def _decode_chunked(body):
    out = b""
    i = 0
    while i < len(body):
        j = body.find(b"\r\n", i)
        if j < 0: break
        size_str = body[i:j].decode("ascii", "ignore").strip()
        if ";" in size_str: size_str = size_str.split(";", 1)[0]
        try: size = int(size_str, 16)
        except ValueError: break
        if size == 0: break
        i = j + 2
        out += body[i:i+size]
        i += size
        if body[i:i+2] == b"\r\n": i += 2
    return out


def _parse_stream(body):
    """Parse Docker stream frames.
    Frame format (8 bytes header):
      [0]   : stream type (1=stdout, 2=stderr)
      [1-3] : padding (zero)
      [4-7] : payload size (uint32 big-endian)
      [8-N] : payload data
    """
    out = b""
    err = b""
    i = 0
    while i + 8 <= len(body):
        t = body[i]
        # Read size from bytes 4-7 (skip 3 padding bytes at positions 1-3)
        size = struct.unpack(">I", body[i+4:i+8])[0]
        if size < 0 or size > 10**7:
            break
        if i + 8 + size > len(body):
            size = len(body) - i - 8
        payload = body[i+8:i+8+size]
        if t == 1:
            out += payload
        elif t == 2:
            err += payload
        i += 8 + size
    return out, err


def exec_capture(container, cmd, timeout=60):
    raw = _request_raw("POST", f"/containers/{container}/exec", {
        "Cmd": cmd, "AttachStdout": True, "AttachStderr": True, "Tty": False,
    }, timeout=10)
    m = re.search(rb'"Id"\s*:\s*"([^"]+)"', raw)
    if not m:
        return {"stdout": "", "stderr": raw.decode("utf-8", "ignore")[:200], "exit_code": -1}
    exec_id = m.group(1).decode("utf-8")

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect(DOCKER_SOCKET)
    body = json.dumps({"Detach": False, "Tty": False})
    req = f"POST /exec/{exec_id}/start HTTP/1.1\r\nHost: docker\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}"
    sock.sendall(req.encode())
    chunks = []
    while True:
        try: c = sock.recv(65536)
        except socket.timeout: break
        if not c: break
        chunks.append(c)
    sock.close()
    raw = b"".join(chunks)
    head, _, raw_body = raw.partition(b"\r\n\r\n")
    if b"Transfer-Encoding: chunked" in head:
        raw_body = _decode_chunked(raw_body)
    stdout_bytes, stderr_bytes = _parse_stream(raw_body)

    info_raw = _request_raw("GET", f"/exec/{exec_id}/json", timeout=5)
    m = re.search(rb'"ExitCode"\s*:\s*(-?\d+)', info_raw)
    exit_code = int(m.group(1)) if m else -1

    return {
        "stdout": stdout_bytes.decode("utf-8", "ignore"),
        "stderr": stderr_bytes.decode("utf-8", "ignore"),
        "exit_code": exit_code,
    }


def list_containers(all=True):
    raw = _request_raw("GET", f"/containers/json?all={'true' if all else 'false'}", timeout=10)
    if not raw: return []
    head, _, body = raw.partition(b"\r\n\r\n")
    if b"Transfer-Encoding: chunked" in head:
        body = _decode_chunked(body)
    try: return json.loads(body)
    except: return []


if __name__ == "__main__":
    import sys
    r = exec_capture(sys.argv[1], ["echo", "hello"], timeout=15)
    print("rc:", r["exit_code"], "stdout:", repr(r["stdout"]), "stderr:", repr(r["stderr"]))
