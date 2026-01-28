import asyncio
import logging
import os

import actionengine

from .gemini_helper import prepare_generate_content_action

SYSTEM_INSTRUCTIONS = [
    "You are a helpful research assistant that helps to investigate a given brief "
    "in the context of a broader research topic.",
    "You will investigate the brief, making sure to use Google Search, and making "
    "necessary citations.",
    "Present your findings in a clear and concise manner, using bullet points.",
    "Make sure to include the sources you used.",
    "In the beginning of your report, concisely mention the brief you were given.",
    "Respond in the same language as the input.",
]

logger = logging.getLogger(__name__)


async def run(action: actionengine.Action):
    try:
        api_key, topic, brief = await asyncio.gather(
            action["api_key"].consume(),
            action["topic"].consume(),
            action["brief"].consume(),
        )

        if api_key in ("alpha-demos",):
            api_key = os.environ.get("GEMINI_API_KEY", "")

        prompt = (
            f'The research topic is "{topic}".\n'
            f"The brief for your investigation is as follows: {brief}\n\n"
        )

        await action["user_log"].put(
            f"[investigate-{action.get_id()}] Investigating brief: {brief}."
        )
        logger.info(
            f"{action.get_id()} Investigating brief: {brief}.",
        )
        response_parts = []
        generate_content = await prepare_generate_content_action(
            action.get_registry(),
            chat_input=prompt,
            api_key=api_key,
            system_instructions=SYSTEM_INSTRUCTIONS,
        )
        generate_content.run()

        async def forward_thoughts():
            async for thought in generate_content["thoughts"]:
                await action["thoughts"].put(thought)
            await action["thoughts"].finalize()

        forward_thoughts_coro = forward_thoughts()

        async for chunk in generate_content["output"]:
            response_parts.append(chunk)
        await action["report"].put_and_finalize("".join(response_parts))
        await forward_thoughts_coro

    finally:
        await action["user_log"].put_and_finalize(
            f"[investigate-{action.get_id()}] Investigation complete."
        )
        logger.info(
            f"{action.get_id()} Investigation complete.",
        )


SCHEMA = actionengine.ActionSchema(
    name="investigate",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
        ("brief", "text/plain"),
    ],
    outputs=[
        ("report", "text/plain"),
        ("thoughts", "text/plain"),
        ("user_log", "text/plain"),
    ],
)
