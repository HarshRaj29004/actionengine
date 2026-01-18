import asyncio

import actionengine
import pytest


async def handle_connection(
    stream: actionengine._C.websockets.WebsocketWireStream,
    _: actionengine.Session,
):
    received = await asyncio.to_thread(stream.receive)
    received.node_fragments[0].chunk.data = "Hello from custom handler"

    await asyncio.to_thread(stream.send, received)
    await asyncio.to_thread(stream.half_close)


@pytest.mark.asyncio
async def test_custom_connection_handler():
    action_registry = actionengine.ActionRegistry()
    service = actionengine.Service(action_registry, handle_connection)
    server = actionengine.websockets.WebsocketServer(service, port=20002)
    server.run()
    await asyncio.sleep(0.2)  # Give the server time to start

    client_stream = actionengine.websockets.make_websocket_stream(
        "127.0.0.2", "/", 20002
    )
    await asyncio.to_thread(client_stream.start)

    node_fragment = actionengine.NodeFragment()
    node_fragment.continued = False
    node_fragment.id = "test_message"
    node_fragment.seq = 0
    node_fragment.chunk = actionengine.Chunk()
    wire_message = actionengine.WireMessage()
    wire_message.node_fragments.append(node_fragment)

    await asyncio.to_thread(client_stream.send, wire_message)
    await asyncio.to_thread(client_stream.half_close)

    message = await asyncio.to_thread(client_stream.receive)
    assert message.node_fragments
    try:
        _ = message.node_fragments[0].chunk
    except:
        assert False, "Chunk is missing in the received message"
    assert message.node_fragments[0].chunk.data == b"Hello from custom handler"

    message = await asyncio.to_thread(client_stream.receive)
    assert message is None
