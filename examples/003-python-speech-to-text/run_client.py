import argparse
import asyncio
import uuid

import actionengine
from RealtimeSTT import AudioToTextRecorder

from stt.actions import has_stop_command
from stt.actions import make_action_registry
from stt.serialisation import register_stt_serialisers


SIGNALLING_URL = "wss://actionengine.dev:19001"


def setup_action_engine():
    register_stt_serialisers()

    settings = actionengine.get_global_settings()
    settings.readers_deserialise_automatically = True
    settings.readers_read_in_order = True
    settings.readers_remove_read_chunks = True


async def main(args: argparse.Namespace):
    setup_action_engine()

    action_registry = make_action_registry()
    node_map = actionengine.NodeMap()

    client_identity = str(uuid.uuid4())
    stream = await asyncio.to_thread(
        actionengine.webrtc.make_webrtc_stream,
        client_identity,
        args.server_webrtc_identity,
        SIGNALLING_URL,
    )

    print("Connected, starting session.")
    session = actionengine.Session(node_map, action_registry)
    session.dispatch_from(stream)

    action = action_registry.make_action(
        "speech_to_text", node_map=node_map, stream=stream, session=session
    )

    await action.call()

    print("Action called, waiting for ready signal.")
    ready = await action.get_output("ready").next()
    if ready is not True:
        print("Action not ready, exiting.")
        return

    recorder = AudioToTextRecorder(
        spinner=True,
        on_recorded_chunk=lambda audio: action.get_input("speech").put(audio),
    )
    print("You can start speaking now.")

    try:
        async for text in action["text"]:
            print(text)
            if has_stop_command(text):
                print("Stop command received, stopping recording.")
                break
    finally:
        shutdown = asyncio.to_thread(recorder.shutdown)
        print("Stopped recording.")

        await shutdown
        await action["speech"].finalize()
        try:
            await action.wait_until_complete()
        except Exception:
            raise
        finally:
            await asyncio.to_thread(stream.half_close)

        print("Finalised the speech stream.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the STT client.")

    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="The host to connect to.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=20000,
        help="The port to connect to.",
    )
    parser.add_argument(
        "--server_webrtc_identity",
        type=str,
        required=True,
        help="The WebRTC signalling identity to connect to.",
    )

    asyncio.run(main(parser.parse_args()))
