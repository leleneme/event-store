"""Send JSON requests and print response frames as they arrive (Python stdlib only)."""

import argparse
import json
import socket
import struct

MAX_REQUEST_BYTES = 1024 * 1024


def encode_json(value):
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def read_exact(connection, size):
    result = bytearray()
    while len(result) < size:
        chunk = connection.recv(size - len(result))
        if not chunk:
            raise EOFError("Server closed the connection before the response completed")
        result.extend(chunk)
    return result


def responses(connection, request):
    body = encode_json(request)
    if not 0 < len(body) <= MAX_REQUEST_BYTES:
        raise ValueError("Request must fit in a 1 MiB frame")
    connection.sendall(struct.pack(">I", len(body)) + body)
    while True:
        size = struct.unpack(">I", read_exact(connection, 4))[0]
        frame = json.loads(read_exact(connection, size))
        yield frame
        if frame["type"] in ("done", "error"):
            return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("requests", nargs="+", help="JSON requests, executed sequentially")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5252)
    args = parser.parse_args()
    with socket.create_connection((args.host, args.port), timeout=10) as connection:
        for request in args.requests:
            for frame in responses(connection, json.loads(request)):
                print(json.dumps(frame, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
