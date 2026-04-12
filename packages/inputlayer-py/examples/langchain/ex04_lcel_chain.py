"""LCEL chain with LLM."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Full LCEL chain: retriever | prompt | llm | parser.

    Auto-detects LM Studio at localhost:1234. Set LLM_BASE_URL and LLM_MODEL
    env vars to override. Skips gracefully if no LLM is available.
    """
    header("LCEL chain with LLM", 4)

    if not check_llm():
        print(f"\n{DIM}  No LLM server detected — skipping.{RESET}")
        print(f"{DIM}  Start LM Studio and load a model, or set LLM_BASE_URL/LLM_MODEL.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    llm = get_llm()

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
