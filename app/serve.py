import os
import signal
import sys

import uvicorn


def _ignore(signum, frame):
    print(f"[serve] signal {signal.Signals(signum).name} after shutdown ignored", flush=True)


def main() -> int:
    signal.signal(signal.SIGTERM, _ignore)
    signal.signal(signal.SIGINT, _ignore)
    config = uvicorn.Config(
        "app.main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        access_log=False,
        timeout_graceful_shutdown=int(os.getenv("TIMEOUT_GRACEFUL_SHUTDOWN", "20")),
    )
    server = uvicorn.Server(config)
    server.run()
    if not server.started:
        return 3
    print("[serve] graceful shutdown complete, exit code 0", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
