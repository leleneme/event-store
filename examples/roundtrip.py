"""Append 10,000 sample events, reconnect, and stream them back for verification."""

import argparse
import base64
import json
import socket
import time
import uuid

from tcp_client import MAX_REQUEST_BYTES, encode_json, responses


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def sample_event(stream, index):
    payload = json.dumps({"number": index, "message": "sample event"}).encode("utf-8")
    return {
        "eventId": f"{stream}-{index}",
        "eventType": "SampleEvent",
        "payloadBase64": base64.b64encode(payload).decode("ascii"),
    }


def append_requests(stream, count, batch_size):
    start = 0
    while start < count:
        request = {
            "op": "append",
            "streamId": stream,
            "expectedRevision": "noStream" if start == 0 else start - 1,
            "events": [],
        }
        size_bytes = len(encode_json(request))
        while len(request["events"]) < min(batch_size, count - start):
            event = sample_event(stream, start + len(request["events"]))
            # Default JSON array separators are two bytes: comma and space.
            extra_bytes = len(encode_json(event)) + (2 if request["events"] else 0)
            if size_bytes + extra_bytes > MAX_REQUEST_BYTES:
                if not request["events"]:
                    raise ValueError("One event plus request metadata exceeds the 1 MiB frame limit")
                break
            request["events"].append(event)
            size_bytes += extra_bytes
        yield start, request
        start += len(request["events"])


def verify(connection, request, stream, start, count):
    received = 0
    for frame in responses(connection, request):
        if frame["type"] == "error":
            raise RuntimeError(f"{frame['code']}: {frame['message']}")
        if frame["type"] == "done":
            break
        if frame["type"] != "event":
            raise RuntimeError(f"Unexpected response: {frame}")
        index = start + received
        event = frame["event"]
        expected = {**sample_event(stream, index), "streamId": stream, "revision": index}
        if received >= count or any(event.get(key) != value for key, value in expected.items()):
            raise RuntimeError(f"Event verification failed at revision {index}: {event}")
        received += 1
    if received != count:
        raise RuntimeError(f"Expected {count} events, received {received}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5252)
    parser.add_argument("--count", type=positive_int, default=10_000)
    parser.add_argument("--batch-size", type=positive_int, default=500,
                        help="maximum events per append; requests also split at 1 MiB")
    parser.add_argument("--stream", default=f"roundtrip-{uuid.uuid4().hex}")
    args = parser.parse_args()
    address = (args.host, args.port)
    print(f"Stream: {args.stream}", flush=True)

    started = time.perf_counter()
    with socket.create_connection(address, timeout=30) as connection:
        for start, request in append_requests(args.stream, args.count, args.batch_size):
            size = len(request["events"])
            verify(connection, request, args.stream, start, size)
            print(f"Appended {start + size:,}/{args.count:,}", flush=True)
    print(f"Append completed in {time.perf_counter() - started:.2f}s", flush=True)

    # A new connection reads the committed stream after all writes have finished.
    started = time.perf_counter()
    with socket.create_connection(address, timeout=30) as connection:
        verify(connection, {"op": "read", "streamId": args.stream}, args.stream, 0, args.count)
    print(f"Read and verified {args.count:,} events in {time.perf_counter() - started:.2f}s")


if __name__ == "__main__":
    try:
        main()
    except (OSError, EOFError, ValueError, RuntimeError) as error:
        raise SystemExit(f"Roundtrip failed: {error}") from None
