"""CLI entry point for the Flowcast API server."""

import argparse
import uvicorn

from flowcast.config import API_HOST, API_PORT


def main() -> None:
    parser = argparse.ArgumentParser(description="Flowcast: Start the API server.")
    parser.add_argument("--host", default=API_HOST)
    parser.add_argument("--port", type=int, default=API_PORT)
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    args = parser.parse_args()

    uvicorn.run(
        "flowcast.api.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
