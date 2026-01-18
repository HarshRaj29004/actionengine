import asyncio
import uuid

import actionengine
import pytest


# TODO: Use a local signalling server for tests
SIGNALLING_URL = "wss://actionengine.dev:19001"


ECHO_SCHEMA = actionengine.ActionSchema(
    name="echo",
    description="Echoes back the received message.",
    inputs=[("input", "text/plain")],
    outputs=[("output", "text/plain")],
)


async def run_echo(action: actionengine.Action):
    async for chunk in action["input"]:
        await action["output"].put(chunk)
    await action["output"].finalize()


@pytest.mark.asyncio
async def test_webrtc_stream_works():
    settings = actionengine.get_global_settings()
    current_readers_deserialise_automatically = (
        settings.readers_deserialise_automatically
    )

    webrtc_server = None

    try:
        server_identity = str(uuid.uuid4())
        client_identity = str(uuid.uuid4())

        action_registry = actionengine.ActionRegistry()
        action_registry.register(ECHO_SCHEMA.name, ECHO_SCHEMA, run_echo)
        service = actionengine.Service(action_registry)
        rtc_config = actionengine.webrtc.RtcConfig()
        webrtc_server = actionengine.webrtc.WebRtcServer.create(
            service,
            "0.0.0.0",
            server_identity,
            SIGNALLING_URL,
            rtc_config,
        )
        webrtc_server.run()

        stream = actionengine.webrtc.make_webrtc_stream(
            client_identity,
            server_identity,
            SIGNALLING_URL,
        )
        node_map = actionengine.NodeMap()
        session = actionengine.Session(node_map, action_registry)
        session.dispatch_from(stream)

        echo = action_registry.make_action(
            ECHO_SCHEMA.name, node_map=node_map, stream=stream, session=session
        )
        await asyncio.gather(
            echo.call(),
            echo["input"].put_and_finalize("Hello, WebRTC!"),
        )

        received = await echo["output"].consume(allow_none=True)
        assert (
            received is not None
        ), "Did not receive any message from echo action"
        assert received == "Hello, WebRTC!", "Received incorrect message"

        await asyncio.to_thread(stream.half_close)

    finally:
        settings.readers_deserialise_automatically = (
            current_readers_deserialise_automatically
        )
        if webrtc_server:
            webrtc_server.cancel()
            await asyncio.to_thread(webrtc_server.join)
