#!/usr/bin/env bash
# Usage: ./tunnel.sh   (Ctrl+C to stop; prints a new https://*.trycloudflare.com URL each run)
# http2: QUIC/UDP is throttled on this network (~30 KB/s vs ~1 MB/s)
cd "$(dirname "$0")"
exec caffeinate -i .runtime/cloudflared tunnel --no-autoupdate --protocol http2 --url http://127.0.0.1:9000
