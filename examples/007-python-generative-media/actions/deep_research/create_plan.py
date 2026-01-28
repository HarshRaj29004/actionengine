import asyncio
import logging
import os
import traceback

import actionengine

from .gemini_helper import prepare_generate_content_action

SYSTEM_INSTRUCTIONS = [
    "You are a helpful research assistant that helps to create a research plan.",
    "You will create a step-by-step plan for researching a given topic.",
    "You may use Google Search to inform your plan.",
    "You will present the plan as an ordered list of steps.",
    "Present your plan clearly step by step, numbering each step, so that they "
    "form an ordered list.",
    "Formulate each step as if it was an instruction to "
    "yourself. Do not explain anything yet. Just present your plan. ",
    "Start directly with the list, no introduction. The final item of your "
    "plan should start with 'FINALLY: ' and be a detailed instruction "
    "that describes how you will synthesise and present the final result. "
    "Use no more than 4 steps. Make sure that the steps can be "
    "performed independently, because they will be performed by "
    "different agents. Reply in the same language as the input. ",
]

logger = logging.getLogger(__name__)


async def run(action: actionengine.Action):
    try:
        api_key, topic = await asyncio.gather(
            action["api_key"].consume(),
            action["topic"].consume(),
        )

        if api_key in ("alpha-demos",):
            api_key = os.environ.get("GEMINI_API_KEY", "")

        response_parts = []

        prompt = f"Here is the research topic: {topic}"

        await action["user_log"].put(
            f"[create_plan] Creating plan for topic: {topic}."
        )
        logger.info(f"{action.get_id()} Creating plan for topic: {topic}.")
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
        await forward_thoughts_coro

        await action["user_log"].put("[create_plan] Processing plan items.")
        logger.info(
            f"{action.get_id()} Processing plan items.",
        )

        full_response = "".join(response_parts)
        lines = full_response.split("\n")
        lines_no_numbers = [
            " ".join(line.split()[1:]) for line in lines if line.strip()
        ]

        plan_items = action["plan_items"]
        try:
            for line in lines_no_numbers:
                await action["user_log"].put(f"[create_plan] Plan item: {line}")
                logger.info(
                    f"{action.get_id()} Plan item: {line}.",
                )
                await plan_items.put(line)
        finally:
            await plan_items.finalize()
    except Exception:
        await action["user_log"].put("[create_plan] Failed to create plan.")
        logger.info(
            f"{action.get_id()} Failed to create plan.",
        )
        await action["user_log"].put(traceback.format_exc())
        traceback.print_exc()
        raise
    else:
        await action["user_log"].put("[create_plan] Finished creating plan.")
        logger.info(
            f"{action.get_id()} Finished creating plan.",
        )
    finally:
        await action["user_log"].finalize()


SCHEMA = actionengine.ActionSchema(
    name="create_plan",
    inputs=[
        ("api_key", "text/plain"),
        ("topic", "text/plain"),
    ],
    outputs=[
        ("plan_items", "text/plain"),
        ("thoughts", "text/plain"),
        ("user_log", "text/plain"),
    ],
)
