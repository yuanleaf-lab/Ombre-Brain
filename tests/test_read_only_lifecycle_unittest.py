import asyncio
import logging
import unittest
from unittest.mock import AsyncMock, Mock

from server_app import RuntimeLifecycle


class RuntimeLifecycleReadOnlyTests(unittest.TestCase):
    def test_read_only_skips_every_background_callback(self):
        calls = []
        async def record(name):
            calls.append(name)
        decay = Mock(start=AsyncMock(side_effect=lambda: record("decay")), stop=AsyncMock(side_effect=lambda: record("decay-stop")))
        outbox = Mock(start=AsyncMock(side_effect=lambda: record("outbox")), stop=AsyncMock(side_effect=lambda: record("outbox-stop")))
        lifecycle = RuntimeLifecycle(
            logger=logging.getLogger(__name__), read_only=True,
            decay_engine=decay, embedding_outbox=outbox,
            ensure_ollama_child=lambda: record("ollama"),
            load_tunnel_config=lambda: calls.append("tunnel"),
            restart_github_auto_task=lambda _: calls.append("sync"),
            github_auto_interval=1, keepalive_url="http://127.0.0.1:1/health",
        )
        async def run():
            await lifecycle.start()
            await lifecycle.stop()
        asyncio.run(run())
        self.assertEqual(calls, [])
        decay.start.assert_not_called()
        outbox.start.assert_not_called()

    def test_default_mode_still_starts_services(self):
        calls = []
        async def decay_start():
            calls.append("decay")
        async def decay_stop():
            calls.append("stop")
        lifecycle = RuntimeLifecycle(
            logger=logging.getLogger(__name__),
            decay_engine=Mock(start=decay_start, stop=decay_stop),
        )
        async def run():
            await lifecycle.start()
            await lifecycle.stop()
        asyncio.run(run())
        self.assertEqual(calls, ["decay", "stop"])


if __name__ == "__main__":
    unittest.main()
