"""Run the local server with a stop signal available to long-lived responses."""
from fastapi import FastAPI
import uvicorn


def create_server(app: FastAPI, *, host: str, port: int, log_level: str = "info") -> uvicorn.Server:
    server = uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_level=log_level))
    # ASGI lifespan shutdown runs only after HTTP connections have drained.
    # Streams need the earlier signal to finish while normal requests drain.
    app.state.is_shutting_down = lambda: server.should_exit
    return server


def run_server(app: FastAPI, *, host: str, port: int, log_level: str = "info") -> None:
    server = create_server(app, host=host, port=port, log_level=log_level)
    try:
        server.run()
    except KeyboardInterrupt:
        # Uvicorn re-raises Ctrl+C after completing its graceful shutdown.
        pass
    if not server.started:
        raise SystemExit(3)
