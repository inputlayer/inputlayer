"""LCEL chain with LLM."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Full LCEL chain: retriever | prompt | llm | parser.

    Auto-detects LM Studio at localhost:1234. Set LLM_BASE_URL and LLM_MODEL
    env vars to override. Skips gracefully if no LLM is available.
    """
    header("LCEL chain with LLM", 4)

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    # Check if LM Studio (or compatible server) is running
    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
        print(f"\n{DIM}  LLM server detected at {base_url}{RESET}")
        print(f"{DIM}  Model: {model}{RESET}")
    except Exception:
        print(f"\n{DIM}  No LLM server detected at {base_url} — skipping.{RESET}")
        print(f"{DIM}  Start LM Studio and load a model, or set LLM_BASE_URL/LLM_MODEL.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(
        base_url=base_url,
        api_key="lm-studio",
        model=model,
        temperature=0.7,
    )

    retriever = InputLayerRetriever(
        kg=kg,
        query='?article(Id, Title, Content, "{input}", Emb)',
        page_content_columns=["Content"],
        metadata_columns=["Title"],
    )

    prompt = ChatPromptTemplate.from_template(
        "You are a helpful assistant. Based on the following articles from a "
        "knowledge graph, answer the question concisely.\n\n"
        "Articles:\n{context}\n\n"
        "Question: What are the main topics covered in the {question} category?"
    )

    chain = {"context": retriever, "question": lambda x: x} | prompt | llm | StrOutputParser()

    question = "ml"
    print(f"\n{DIM}  Chain: retriever | prompt | llm | parser{RESET}")
    print(f'{DIM}  Question: "What are the main topics in the {question} category?"{RESET}')

    subheader("LLM Response:")
    answer = await chain.ainvoke(question)
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
