import asyncio
import logging
import os

import actionengine

from .gemini_helper import prepare_generate_content_action


SYSTEM_INSTRUCTIONS = [
    "You are a helpful research assistant that helps to synthesise the findings "
    "of a research on a given topic, based on multiple intermediate reports. "
    "You will synthesise the findings into a final report, based on the "
    "intermediate reports you have been given. Make sure to address the brief "
    "you have been given. ",
]

logger = logging.getLogger(__name__)


async def run(action: actionengine.Action):
    try:
        topic = await action["topic"].consume()
        brief = await action["brief"].consume()
        report_ids = [report_id async for report_id in action["report_ids"]]

        await action["user_log"].put(
            f"[synthesise_findings] Synthesising findings for topic: {topic}. "
            f"Awaiting {len(report_ids)} reports."
        )
        logger.info(
            f"{action.get_id()} Synthesising findings for "
            f"topic: {topic}. Awaiting {len(report_ids)} reports.",
        )

        node_map = action.get_node_map()
        reports: list[str] = await asyncio.gather(
            *(node_map.get(report_id).consume() for report_id in report_ids)
        )
        await action["user_log"].put(
            f"[synthesise_findings] Synthesising {len(reports)} reports."
        )
        logger.info(
            f"{action.get_id()} Synthesising {len(reports)} " f"reports.",
        )

        prompt = (
            f"The topic is: {topic}. Report in the same language. You have the "
            f"following {len(reports)} intermediate "
            f"reports: {'\n\n'.join(reports)}\n\n. {brief}"
        )

        api_key = await action["api_key"].consume()
        if api_key in ("alpha-demos",):
            api_key = os.environ.get("GEMINI_API_KEY", "")

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
            await action["report"].put(chunk)
        await action["report"].finalize()
        await forward_thoughts_coro

    finally:
        await action["user_log"].put_and_finalize(
            f"[synthesise_findings] Synthesis complete."
        )
        logger.info(
            f"{action.get_id()} Synthesis complete.",
        )


SCHEMA = actionengine.ActionSchema(
    name="synthesise_findings",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
        ("brief", "text/plain"),
        ("report_ids", "text/plain"),
    ],
    outputs=[
        ("report", "text/plain"),
        ("thoughts", "text/plain"),
        ("user_log", "text/plain"),
    ],
)
