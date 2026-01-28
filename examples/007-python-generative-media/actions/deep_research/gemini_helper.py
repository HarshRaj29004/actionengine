import actionengine


async def prepare_generate_content_action(
    action_registry: actionengine.ActionRegistry,
    chat_input: str,
    api_key: str,
    session_token: str = "",
    system_instructions: list[str] | None = None,
):
    node_map = actionengine.NodeMap()
    action = action_registry.make_action("generate_content", node_map=node_map)
    await action["chat_input"].put_and_finalize(chat_input)
    await action["api_key"].put_and_finalize(api_key)
    await action["session_token"].put_and_finalize(session_token)

    if system_instructions:
        for instruction in system_instructions:
            await action["system_instructions"].put(instruction)
    await action["system_instructions"].finalize()

    action.clear_outputs_after_run(False)

    return action
