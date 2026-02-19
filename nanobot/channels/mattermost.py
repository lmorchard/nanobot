"""Mattermost channel implementation using async WebSocket + httpx.

Connects to a Mattermost server using:
  - REST API v4 (httpx) for sending posts and resolving user info
  - WebSocket API (websockets) for real-time event streaming

No external Mattermost SDK is required; only websockets and httpx (both
already project dependencies) are used.

Ported from picoclaw's Go implementation.
"""

from __future__ import annotations

import asyncio
import json
from urllib.parse import urlparse, urlunparse

import httpx
import websockets
from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import MattermostConfig


class MattermostChannel(BaseChannel):
    """Mattermost channel using async WebSocket + REST API v4."""

    name = "mattermost"

    def __init__(self, config: MattermostConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: MattermostConfig = config
        self._http: httpx.AsyncClient | None = None
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._bot_user_id: str | None = None
        self._bot_username: str | None = None
        self._reconnect_task: asyncio.Task | None = None

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the Mattermost channel."""
        if not self.config.url or not self.config.token:
            logger.error("Mattermost url and token must be configured")
            return

        self._http = httpx.AsyncClient(
            base_url=self._api_base(),
            headers={
                "Authorization": f"Bearer {self.config.token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

        # Verify credentials and get bot user info
        try:
            resp = await self._http.get("/users/me")
            resp.raise_for_status()
            me = resp.json()
            self._bot_user_id = me["id"]
            self._bot_username = me.get("username", "")
            logger.info(f"Mattermost bot @{self._bot_username} authenticated (id={self._bot_user_id})")
        except Exception as e:
            logger.error(f"Mattermost auth failed: {e}")
            return

        # Connect WebSocket
        try:
            await self._connect_ws()
        except Exception as e:
            logger.error(f"Mattermost WebSocket connect failed: {e}")
            return

        self._running = True

        # Spawn reconnect watcher
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

        # Listen for events (blocks until stopped)
        await self._listen_ws()

    async def stop(self) -> None:
        """Stop the Mattermost channel."""
        self._running = False

        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._http:
            await self._http.aclose()
            self._http = None

        logger.info("Mattermost channel stopped")

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through Mattermost."""
        if not self._http:
            logger.warning("Mattermost client not running")
            return

        channel_id, root_id = _parse_chat_id(msg.chat_id)
        if not channel_id:
            logger.error(f"Invalid mattermost chat_id: {msg.chat_id!r}")
            return

        for chunk in _split_message(msg.content, 4000):
            try:
                body: dict[str, str] = {
                    "channel_id": channel_id,
                    "message": chunk,
                }
                if root_id:
                    body["root_id"] = root_id

                resp = await self._http.post("/posts", json=body)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Error sending Mattermost message: {e}")
                return

    # ── WebSocket connection ─────────────────────────────────────────────

    async def _connect_ws(self) -> None:
        """Open a WebSocket connection and authenticate."""
        ws_url = self._ws_url()
        logger.info(f"Connecting Mattermost WebSocket to {ws_url}")

        self._ws = await websockets.connect(ws_url, ping_interval=30, ping_timeout=10)

        # Send authentication challenge
        auth_msg = {
            "seq": 1,
            "action": "authentication_challenge",
            "data": {"token": self.config.token},
        }
        await self._ws.send(json.dumps(auth_msg))
        logger.info("Mattermost WebSocket connected")

    async def _listen_ws(self) -> None:
        """Read WebSocket events in a loop."""
        while self._running:
            if not self._ws:
                await asyncio.sleep(1)
                continue

            try:
                raw = await self._ws.recv()
                await self._handle_ws_message(raw)
            except websockets.ConnectionClosed:
                if not self._running:
                    return
                logger.warning("Mattermost WebSocket connection closed")
                self._ws = None
            except Exception as e:
                if not self._running:
                    return
                logger.error(f"Mattermost WebSocket read error: {e}")
                self._ws = None

    async def _reconnect_loop(self) -> None:
        """Reconnect WebSocket with exponential backoff."""
        delay = 5.0
        max_delay = 60.0

        while self._running:
            await asyncio.sleep(delay)

            if self._ws is not None:
                delay = 5.0
                continue

            logger.info("Attempting Mattermost WebSocket reconnect...")
            try:
                await self._connect_ws()
                delay = 5.0
                logger.info("Mattermost WebSocket reconnected")
            except Exception as e:
                logger.error(f"Mattermost WebSocket reconnect failed: {e}")
                delay = min(delay * 2, max_delay)

    # ── Event handling ──────────────────────────────────────────────────

    async def _handle_ws_message(self, raw: str | bytes) -> None:
        """Parse and dispatch a WebSocket event."""
        try:
            evt = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Mattermost: failed to parse WS frame")
            return

        event_type = evt.get("event", "")

        if event_type == "posted":
            await self._handle_posted(evt)
        elif event_type == "hello":
            logger.info("Mattermost WebSocket hello (server ready)")
        elif event_type == "":
            pass  # acknowledgement frame
        else:
            logger.debug(f"Mattermost: unhandled WS event: {event_type}")

    async def _handle_posted(self, evt: dict) -> None:
        """Handle a 'posted' event (new message)."""
        data = evt.get("data", {})

        # The post field is double-encoded JSON
        post_str = data.get("post", "{}")
        try:
            post = json.loads(post_str)
        except json.JSONDecodeError:
            logger.warning("Mattermost: failed to parse post JSON")
            return

        user_id = post.get("user_id", "")
        channel_id = post.get("channel_id", "")
        message = post.get("message", "").strip()
        post_id = post.get("id", "")
        root_id = post.get("root_id", "")
        channel_type = data.get("channel_type", "")
        sender_name = data.get("sender_name", "")

        # Ignore our own messages
        if user_id == self._bot_user_id:
            return

        # Check allowlist
        if not self.is_allowed(user_id):
            logger.debug(f"Mattermost: message rejected by allowlist (user={user_id})")
            return

        # Strip bot @mention
        message = self._strip_bot_mention(message)
        if not message:
            return

        # Build chat_id with threading context:
        #   DMs (channel_type "D"): just channelID (no threading)
        #   Channel, existing thread: channelID/rootID (always stay in thread)
        #   Channel, new message + reply_in_thread: channelID/postID (start new thread)
        #   Channel, new message + no threading: just channelID (flat reply)
        chat_id = channel_id
        if root_id:
            chat_id = f"{channel_id}/{root_id}"
        elif channel_type != "D" and self.config.reply_in_thread:
            chat_id = f"{channel_id}/{post_id}"

        logger.debug(
            f"Mattermost message from {sender_name} ({user_id}): {message[:50]}"
        )

        await self._handle_message(
            sender_id=user_id,
            chat_id=chat_id,
            content=message,
            metadata={
                "mattermost": {
                    "post_id": post_id,
                    "channel_id": channel_id,
                    "root_id": root_id,
                    "channel_type": channel_type,
                    "sender_name": sender_name,
                    "team_id": data.get("team_id", ""),
                }
            },
        )

    # ── Helpers ──────────────────────────────────────────────────────────

    def _api_base(self) -> str:
        """Build the REST API v4 base URL."""
        return self.config.url.rstrip("/") + "/api/v4"

    def _ws_url(self) -> str:
        """Build the WebSocket URL from the server URL."""
        parsed = urlparse(self.config.url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        ws_parsed = parsed._replace(scheme=scheme, path="/api/v4/websocket")
        return urlunparse(ws_parsed)

    def _strip_bot_mention(self, text: str) -> str:
        """Strip @botusername mention from message text."""
        username = self.config.username or self._bot_username
        if username:
            text = text.replace(f"@{username}", "")
        return text.strip()


# ── Module-level utilities ───────────────────────────────────────────────────


def _parse_chat_id(chat_id: str) -> tuple[str, str]:
    """Split 'channelID' or 'channelID/rootID' into components."""
    parts = chat_id.split("/", 1)
    channel_id = parts[0]
    root_id = parts[1] if len(parts) > 1 else ""
    return channel_id, root_id


def _split_message(content: str, limit: int = 4000) -> list[str]:
    """Split long content into chunks, preferring newline then word boundaries."""
    if len(content) <= limit:
        return [content]

    chunks: list[str] = []
    while content:
        if len(content) <= limit:
            chunks.append(content)
            break

        # Try to split at a newline within the last 200 chars of the limit
        segment = content[:limit]
        split = segment.rfind("\n", limit - 200)
        if split <= 0:
            # Fall back to last space within the last 100 chars
            split = segment.rfind(" ", limit - 100)
        if split <= 0:
            split = limit

        chunks.append(content[:split])
        content = content[split:].strip()

    return chunks
