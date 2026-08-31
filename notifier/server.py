"""
Alertmanager webhook sink.

One POST endpoint plus two GETs, so the standard library's
ThreadingHTTPServer is used rather than pulling in a web framework.
"""

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import metrics
from .config import NotifierConfig
from .payload import parse_alerts
from .probe import PostgresProbe
from .store import NotificationStore

logger = logging.getLogger(__name__)

MAX_BODY_BYTES = 1_048_576


class AlertHandler(BaseHTTPRequestHandler):

    server_version = "heartbeat-notifier/1.0"

    # Injected by build_server.
    store: NotificationStore

    def do_GET(self) -> None:
        if self.path.startswith("/metrics"):
            self._send(
                200,
                generate_latest(),
                CONTENT_TYPE_LATEST,
            )

            return

        if self.path.startswith("/health"):
            self._send_json(200, {"status": "ok"})

            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if not self.path.startswith("/alerts"):
            self._send_json(404, {"error": "not found"})

            return

        try:
            body = self._read_json()
        except (TypeError, ValueError) as exc:
            logger.warning("Rejected webhook: %s", exc)

            self._send_json(400, {"error": str(exc)})

            return

        notifications = parse_alerts(body)

        for notification in notifications:
            metrics.alerts_received.labels(
                status=notification.status.lower(),
            ).inc()

            self.store.save(notification)

        # Acknowledged even when nothing parsed, so Alertmanager does
        # not retry a payload we will never understand.
        self._send_json(
            200,
            {"received": len(notifications)},
        )

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)

        if length <= 0:
            raise ValueError("empty body")

        if length > MAX_BODY_BYTES:
            raise ValueError("body too large")

        raw = self.rfile.read(length)

        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("malformed JSON") from exc

        if not isinstance(body, dict):
            raise TypeError("expected a JSON object")

        return body

    def _send(
        self,
        status: int,
        payload: bytes,
        content_type: str,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, status: int, payload: dict) -> None:
        self._send(
            status,
            json.dumps(payload).encode("utf-8"),
            "application/json",
        )

    def log_message(self, format, *args) -> None:
        # Route through logging instead of stderr, and drop the
        # per-request noise Prometheus scraping would generate.
        logger.debug(format, *args)


def build_server(
    config: NotifierConfig,
    store: NotificationStore,
) -> ThreadingHTTPServer:
    handler = type(
        "BoundAlertHandler",
        (AlertHandler,),
        {"store": store},
    )

    return ThreadingHTTPServer(
        (config.host, config.port),
        handler,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(name)s - "
            "%(message)s"
        ),
    )

    config = NotifierConfig()

    store = NotificationStore(config)

    probe = PostgresProbe(config, store)

    probe.start()

    server = build_server(config, store)

    logger.info(
        "Notifier listening on %s:%d "
        "(POST /alerts, GET /health, GET /metrics)",
        config.host,
        config.port,
    )

    try:
        server.serve_forever()

    except KeyboardInterrupt:
        logger.info("Notifier interrupted.")

    finally:
        probe.stop()

        server.shutdown()

        server.server_close()


if __name__ == "__main__":
    main()
