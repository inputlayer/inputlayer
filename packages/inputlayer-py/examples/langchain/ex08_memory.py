"""Conversational memory as facts."""

import asyncio

from examples.langchain._common import *

from inputlayer.integrations.langchain.params import iql_literal


async def run(kg):
    """Conversational memory backed by KG facts and IQL rules.

    Each message turn is stored as a fact. The LLM extracts topics and
    entities per turn. IQL rules derive active context, conversation
    threads, and relevant history - the LLM uses these derived facts
    to produce context-aware responses.

    This positions InputLayer as a structured memory backend, not just
    a vector store for chat history.
    """
    header("Conversational memory as facts", 8)

    # ── Schema for conversation memory ───────────────────────────────

    await kg.execute("+chat_message(id: int, role: string, content: string)")
    await kg.execute("+topic_mention(msg_id: int, topic: string)")
    await kg.execute("+entity_mention(msg_id: int, entity: string, kind: string)")

    # ── Rules that derive context from conversation history ──────────

    # Active topics: topics mentioned anywhere in the conversation
    await kg.execute("+active_topic(Topic) <- topic_mention(MsgId, Topic)")

    # Relevant messages: messages that mention an active topic
    await kg.execute(
        "+relevant_history(Id, Role, Content, Topic) <- "
        "chat_message(Id, Role, Content), topic_mention(Id, Topic)"
    )

    # Entity registry: all mentioned entities by type
    await kg.execute("+known_entity(Entity, Kind) <- entity_mention(MsgId, Entity, Kind)")

    # Cross-references: messages that share topics
    await kg.execute(
        "+related_turns(IdA, IdB, Topic) <- "
        "topic_mention(IdA, Topic), topic_mention(IdB, Topic), IdA != IdB"
    )

    subheader("Rules defined")
    print(f"{DIM}  active_topic(T) <- topic_mention(_, T){RESET}")
    print(f"{DIM}  relevant_history(Id, Role, Content, Topic)")
    print(f"    <- chat_message(Id, Role, Content), topic_mention(Id, Topic){RESET}")
    print(f"{DIM}  known_entity(E, Kind) <- entity_mention(_, E, Kind){RESET}")
    print(f"{DIM}  related_turns(A, B, Topic)")
    print(f"    <- topic_mention(A, Topic), topic_mention(B, Topic), A != B{RESET}")

    # ── Simulate a multi-turn conversation ───────────────────────────

    conversation = [
        (1, "user", "I need help optimizing our ML training pipeline."),
        (
            2,
            "assistant",
            "I can help with that. Are you seeing issues with "
            "data loading, model training, or GPU utilization?",
        ),
        (
            3,
            "user",
            "Mainly GPU utilization. We're using 4 A100s but only seeing 60% usage.",
        ),
        (
            4,
            "assistant",
            "Low GPU utilization often comes from data loading "
            "bottlenecks or small batch sizes. What framework?",
        ),
        (
            5,
            "user",
            "PyTorch with a custom DataLoader. We also noticed the loss plateauing after epoch 50.",
        ),
        (
            6,
            "assistant",
            "The loss plateau suggests a learning rate issue. "
            "Try cosine annealing. For GPU, check DataLoader workers.",
        ),
        (
            7,
            "user",
            "Good point. Can we also use mixed precision training to speed things up?",
        ),
    ]

    # Topics and entities extracted per turn (in production, the LLM does this)
    turn_topics = {
        1: ["ml-pipeline", "optimization"],
        3: ["gpu-utilization", "a100", "hardware"],
        4: ["data-loading", "batch-size", "gpu-utilization"],
        5: ["pytorch", "dataloader", "loss-plateau", "training"],
        6: ["learning-rate", "cosine-annealing", "dataloader", "gpu-utilization"],
        7: ["mixed-precision", "performance"],
    }

    turn_entities = {
        1: [("ml-pipeline", "system")],
        3: [("a100", "hardware"), ("gpu-cluster", "infrastructure")],
        5: [("pytorch", "framework"), ("dataloader", "component")],
        6: [("cosine-annealing", "technique"), ("dataloader", "component")],
        7: [("mixed-precision", "technique")],
    }

    # ── Step 1: Insert conversation turns as facts ───────────────────

    subheader("Step 1: Insert conversation as facts")

    for msg_id, role, content in conversation:
        await kg.execute(f"+chat_message({msg_id}, {iql_literal(role)}, {iql_literal(content)})")

    for msg_id, topics in turn_topics.items():
        for topic in topics:
            await kg.execute(f"+topic_mention({msg_id}, {iql_literal(topic)})")

    for msg_id, entities in turn_entities.items():
        for entity, kind in entities:
            await kg.execute(f"+entity_mention({msg_id}, {iql_literal(entity)}, {iql_literal(kind)})")

    print(f"\n  {GREEN}Inserted {len(conversation)} messages,")
    topic_count = sum(len(t) for t in turn_topics.values())
    entity_count = sum(len(e) for e in turn_entities.values())
    print(f"  {topic_count} topic mentions, {entity_count} entity mentions{RESET}")

    # ── Step 2: Query derived context ────────────────────────────────

    subheader("Step 2: Derived context (computed by IQL rules)")

    r = await kg.execute("?active_topic(T)")
    topics = sorted(row[0] for row in r.rows)
    print(f"\n  {WHITE}Active topics:{RESET}")
    for t in topics:
        print(f"    {YELLOW}{t}{RESET}")

    r = await kg.execute("?known_entity(E, Kind)")
    print(f"\n  {WHITE}Known entities:{RESET}")
    for row in r.rows:
        print(f"    {GREEN}{row[0]}{RESET} {DIM}({row[1]}){RESET}")

    r = await kg.execute("?related_turns(A, B, Topic)")
    print(f"\n  {WHITE}Cross-referenced turns:{RESET}")
    shown = set()
    for row in r.rows:
        pair = (min(row[0], row[1]), max(row[0], row[1]), row[2])
        if pair not in shown:
            shown.add(pair)
            a, b, topic = pair
            print(f"    Turn {CYAN}{a}{RESET} <-> Turn {CYAN}{b}{RESET} via {YELLOW}{topic}{RESET}")

    # ── Step 3: Context-aware response using derived facts ───────────

    if not check_llm():
        print(f"\n{DIM}  No LLM server detected - skipping LLM step.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    llm = get_llm()

    # Build context from derived facts (NOT raw message history)
    r = await kg.execute("?relevant_history(Id, Role, Content, Topic)")
    history_by_topic: dict[str, list[str]] = {}
    for row in r.rows:
        topic = row[3]
        content = f"[Turn {row[0]}, {row[1]}] {row[2]}"
        history_by_topic.setdefault(topic, []).append(content)

    # The user's latest question is about mixed precision
    # Rules automatically link it to related topics
    r = await kg.execute("?related_turns(7, OtherId, Topic)")
    related_topics = sorted({row[2] for row in r.rows})

    context_parts = [f"Current question (turn 7): {conversation[-1][2]}"]
    context_parts.append(f"\nTopics related to this question: {', '.join(related_topics)}")
    context_parts.append("\nRelevant prior discussion:")
    for topic in related_topics:
        if topic in history_by_topic:
            for line in history_by_topic[topic]:
                context_parts.append(f"  {line}")

    context_parts.append(f"\nKnown entities: {', '.join(topics)}")
    context = "\n".join(context_parts)

    subheader("Step 3: LLM responds using rule-derived context")
    print(f"{DIM}  Context assembled from IQL-derived facts, not raw history{RESET}")

    prompt = ChatPromptTemplate.from_template(
        "You are a helpful ML engineering assistant. The following context was "
        "assembled from a knowledge graph that tracks conversation topics and "
        "entities across turns. Use it to give a relevant, contextual answer.\n\n"
        "{context}\n\n"
        "Provide a concise, helpful response to the user's latest question."
    )

    chain = prompt | llm | StrOutputParser()
    answer = await chain.ainvoke({"context": context})

    subheader("LLM Response (context from derived facts):")
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
