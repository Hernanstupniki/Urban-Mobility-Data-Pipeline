#!/usr/bin/env python3
import os
import select
import socket
import threading

LISTEN_HOST = os.getenv("OLTP_PROXY_LISTEN_HOST", "172.18.0.1")
LISTEN_PORT = int(os.getenv("OLTP_PROXY_LISTEN_PORT", "5432"))
TARGET_HOST = os.getenv("OLTP_PROXY_TARGET_HOST", "127.0.0.1")
TARGET_PORT = int(os.getenv("OLTP_PROXY_TARGET_PORT", "5432"))


def bridge(client: socket.socket, address: tuple[str, int]) -> None:
    try:
        upstream = socket.create_connection((TARGET_HOST, TARGET_PORT), timeout=10)
    except OSError as exc:
        print(f"[oltp-proxy] upstream connection failed for {address}: {exc}", flush=True)
        client.close()
        return

    with client, upstream:
        sockets = [client, upstream]
        while True:
            readable, _, _ = select.select(sockets, [], [])
            for current in readable:
                try:
                    data = current.recv(65536)
                except OSError:
                    return
                if not data:
                    return
                peer = upstream if current is client else client
                try:
                    peer.sendall(data)
                except OSError:
                    return


def main() -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((LISTEN_HOST, LISTEN_PORT))
    listener.listen(128)
    print(
        f"[oltp-proxy] listening on {LISTEN_HOST}:{LISTEN_PORT} "
        f"-> {TARGET_HOST}:{TARGET_PORT}",
        flush=True,
    )

    while True:
        client, address = listener.accept()
        threading.Thread(target=bridge, args=(client, address), daemon=True).start()


if __name__ == "__main__":
    main()
