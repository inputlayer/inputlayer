// AUTO-GENERATED - do not edit. Run "node scripts/bundle-content.mjs" to regenerate.

export interface TocEntry {
  level: number
  text: string
  id: string
}

export interface BlogPost {
  slug: string
  title: string
  date: string
  author: string
  category: string
  excerpt: string
  content: string
  toc: TocEntry[]
}

export interface UseCase {
  slug: string
  title: string
  icon: string
  subtitle: string
  content: string
  toc: TocEntry[]
}

export interface ComparisonPage {
  slug: string
  title: string
  competitors: string[]
  content: string
  toc: TocEntry[]
}

export interface CustomerStory {
  slug: string
  title: string
  industry: string
  keyMetric: string
  content: string
  toc: TocEntry[]
}

export const blogPosts: BlogPost[] = [
  {
    "slug": "why-vector-search-alone-fails",
    "title": "Why Vector Search Alone Fails Your AI Agent",
    "date": "2026-02-25",
    "author": "InputLayer Team",
    "category": "Architecture",
    "excerpt": "Vector similarity finds things that look like the answer. But when the answer requires connecting facts across different sources, similarity search hits a wall.",
    "content": "\n# Why Vector Search Alone Fails Your AI Agent\n\nImagine you're building a healthcare AI agent. A patient asks: *\"Can I eat shrimp tonight?\"*\n\nYour agent does what it's supposed to do - it embeds the question and runs a similarity search. Back come the results: shrimp recipes, nutritional info, some allergy FAQs. All genuinely relevant to the words \"eat shrimp.\"\n\nAll completely wrong for this patient.\n\n## What went wrong\n\nThe correct answer is \"No, and it could be dangerous.\" But that answer doesn't live in any single document. It lives across three separate pieces of information:\n\n```chain\nSarah takes Amiodarone\n-- interacts with\nIodine\n-- found in\nShrimp\n=> Shrimp is risky for Sarah\n```\n\nThe patient takes a specific medication. That medication interacts with iodine. Shrimp is high in iodine. Each fact sits in a different database. And the phrase \"shrimp dinner\" has zero similarity to \"medication contraindications\" - they share no words, no concepts, no embedding overlap.\n\nThis is the core problem: **the connection between these facts is logical, not semantic.** You can't find it by looking for similar text. You have to follow a chain of relationships from one fact to the next.\n\n## This shows up everywhere\n\nThe healthcare example is vivid, but this same pattern appears in every domain where answers require connecting multiple facts.\n\n```steps\nA compliance analyst asks: \"Is this transaction suspicious?\" :: The vector DB finds similar transactions. It misses that Entity A is owned by Entity B, which is on a sanctions list.\nAn employee searches for Q3 revenue reports :: The vector DB returns 40 matches. It can't check whether this employee has permission to see any of them through the org hierarchy.\nA supply chain manager asks about disruptions :: The vector DB finds news about port closures. It can't trace which of your suppliers use that port, and which products are affected.\n```\n\nIn every case, the answer requires **following a chain of connected facts** - not finding a similar document. The information exists, but it's spread across different sources, and the connections between them are structural, not textual.\n\n## Why more RAG tricks won't help\n\nWhen teams hit this problem, the first instinct is to optimize the retrieval pipeline. Better chunking. Better embeddings. Hybrid search. Re-ranking.\n\nNone of these help, because the problem isn't retrieval quality. The retrieval is working perfectly - it's finding the most similar content. The problem is that similarity is the wrong tool for the job.\n\n```flow\nUser question -> Embed -> Similarity search -> Similar documents\n```\n\nThis pipeline answers: *\"What text looks most like my question?\"* That's great when the answer exists in a single document. It fails when the answer must be **derived** by connecting facts from different places.\n\n## What teams end up building\n\nTo work around this, teams start adding systems. A graph database for relationships. A rules engine for business logic. An authorization service for access control. Application code to stitch it all together.\n\n```flow\nUser question -> Vector DB [primary] -> Graph DB -> Rules engine -> Auth service -> Reconcile in app code\n```\n\nThe reasoning logic ends up scattered across services. When a fact changes, you have to propagate the change across all of them. It works, but it's fragile, and each new capability makes it more fragile.\n\n## What it looks like with a reasoning layer\n\nInputLayer sits alongside your vector database and handles the part that similarity search can't: following chains of logic and deriving conclusions.\n\n```flow\nUser question -> Your vector DB -> Similar documents\n```\n\n```flow\nUser question -> InputLayer [primary] -> Derived conclusions\n```\n\nYou keep your vector database for what it's good at - finding similar content. You add InputLayer for questions that require reasoning: traversing relationships, evaluating rules, checking permissions through hierarchies.\n\nWhen the patient's medication list changes, InputLayer automatically updates every downstream risk assessment. When an employee changes departments, their permissions recalculate through the org hierarchy. When a corporate ownership structure shifts, the compliance analysis adjusts. All of this happens incrementally - only the affected conclusions recompute, not the entire knowledge base.\n\n## Getting started\n\nInputLayer is open-source and runs in a single Docker container:\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) walks you through building your first knowledge graph in about 10 minutes.",
    "toc": [
      {
        "level": 2,
        "text": "What went wrong",
        "id": "what-went-wrong"
      },
      {
        "level": 2,
        "text": "This shows up everywhere",
        "id": "this-shows-up-everywhere"
      },
      {
        "level": 2,
        "text": "Why more RAG tricks won't help",
        "id": "why-more-rag-tricks-wont-help"
      },
      {
        "level": 2,
        "text": "What teams end up building",
        "id": "what-teams-end-up-building"
      },
      {
        "level": 2,
        "text": "What it looks like with a reasoning layer",
        "id": "what-it-looks-like-with-a-reasoning-layer"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  },
  {
    "slug": "inputlayer-in-10-minutes",
    "title": "InputLayer in 10 Minutes: From Docker to Your First Knowledge Graph",
    "date": "2026-02-20",
    "author": "InputLayer Team",
    "category": "Tutorial",
    "excerpt": "A hands-on tutorial to get InputLayer running and build your first knowledge graph with rules, recursive queries, and vector search.",
    "content": "\n# InputLayer in 10 Minutes: From Docker to Your First Knowledge Graph\n\nBy the end of this tutorial, you'll have a running knowledge graph that can answer questions no regular database can handle. We'll build it step by step, and I'll explain what's happening at each point.\n\n## Step 1: Start InputLayer\n\nRun this in your terminal:\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nYou'll see InputLayer start up and print a message telling you it's ready. That's it - no config files, no setup.\n\n## Step 2: Open the REPL\n\nNow you need a way to talk to InputLayer. Open your browser and go to:\n\n```\nhttp://localhost:8080\n```\n\nThis opens InputLayer's interactive REPL - a command line where you can type queries and see results immediately. Think of it like a SQL console, but for knowledge graphs.\n\nYou can also use the [Python SDK](/docs/guides/python-sdk/) or the [REST API](/docs/guides/configuration/), but the REPL is the fastest way to explore.\n\n## Step 3: Store some facts\n\nLet's model a small organization. In the REPL, you'll store three facts about who manages whom:\n\n```\nAlice manages Bob\nBob manages Charlie\nBob manages Diana\n```\n\nHere's what that looks like as a structure:\n\n```tree\nAlice [primary]\n  Bob\n    Charlie\n    Diana\n```\n\nThat's the entire org chart. Three facts, four people. InputLayer stores these immediately - no schema to define, no tables to create.\n\nYou can already ask simple questions: \"Who does Bob manage?\" returns Charlie and Diana. \"Who manages Bob?\" returns Alice. These are direct lookups, nothing special yet.\n\n## Step 4: Define a rule\n\nThis is the step where InputLayer becomes fundamentally different from a regular database.\n\nWe want to answer: *\"Who does Alice have authority over?\"* Alice manages Bob directly. But does she have authority over Charlie? She doesn't manage Charlie - Bob does. But intuitively, yes - because she manages the person who manages Charlie.\n\nYou express this intuition as a rule:\n\n```note\ntype: tip\nIf person A manages person B, then A has authority over B.\nAnd if A has authority over B, and B has authority over C, then A has authority over C too.\n```\n\nThat second sentence is the important part - it's recursive. It says: authority flows down through the management chain, no matter how deep it goes.\n\nWhen you enter this rule, InputLayer immediately starts reasoning. It applies the rule over and over until there are no more conclusions to draw. Here's what it figures out:\n\n```steps\nAlice manages Bob, so Alice has authority over Bob :: Direct - from the fact you stored\nBob manages Charlie, so Bob has authority over Charlie :: Direct - from the fact you stored\nBob manages Diana, so Bob has authority over Diana :: Direct - from the fact you stored\nAlice has authority over Bob, and Bob has authority over Charlie... :: Following the chain one more step\nAlice has authority over Charlie :: Derived - the engine figured this out [success]\nAlice has authority over Diana :: Derived - same logic, through Bob [success]\n```\n\nFive authority relationships, derived automatically from three facts and one rule.\n\n## Step 5: Ask a question\n\nNow query: *\"Who does Alice have authority over?\"*\n\nThe answer: **Bob, Charlie, and Diana.**\n\nAlice doesn't manage Charlie or Diana directly. But the engine followed the chain and figured it out. This is something a regular database can't do - it required two hops of reasoning.\n\n## Step 6: Add vector search\n\nInputLayer supports vector embeddings alongside logical reasoning. This is where it gets powerful, because you can combine the two in a single query.\n\nSay each person has authored some documents, and each document has an embedding vector. You can now ask something that would normally require multiple systems:\n\n*\"Find documents similar to my query, but only from people that Alice has authority over.\"*\n\n```steps\nResolve Alice's authority chain :: The engine figures out: Bob, Charlie, Diana\nFind documents authored by those people :: Filters to their documents only\nRank by semantic similarity to the query :: Returns the most relevant ones [success]\n```\n\nReasoning and retrieval, combined in one pass. No separate authorization service, no glue code.\n\n## Step 7: See incremental updates\n\nAdd a new fact: Diana now manages a new employee, Frank.\n\n```tree\nAlice [primary]\n  Bob\n    Charlie\n    Diana\n      Frank [success]\n```\n\nQuery authority again. Frank shows up in Alice's results immediately - even though you never told the system about Alice's relationship to Frank. The engine derived it: Alice has authority over Diana, Diana manages Frank, therefore Alice has authority over Frank.\n\nThe important part: InputLayer didn't recompute everything from scratch. It identified that the new fact only affects a small part of the graph and updated just that. On a 2,000-node graph, this is over **1,600x faster** than recomputing everything.\n\n## Step 8: See correct retraction\n\nRemove the fact that Bob manages Diana.\n\n```tree\nAlice [primary]\n  Bob\n    Charlie\n```\n\nQuery authority again. Diana and Frank are gone from Alice's results. But Bob still has authority over Charlie - that relationship doesn't depend on Diana at all.\n\nHere's the subtle part: what if Diana had reported to Alice through *two* paths? Say both Bob and Eve managed Diana. Removing Bob's management of Diana shouldn't remove Alice's authority over Diana if the Eve path still exists. InputLayer tracks this automatically - a conclusion only disappears when *all* paths supporting it are gone.\n\n## What you just built\n\nIn about 10 minutes, you've used:\n\n| Capability | What happened |\n|---|---|\n| Knowledge graph | Stored facts about people and relationships |\n| Recursive reasoning | A rule derived authority chains automatically |\n| Vector search | Combined similarity with logical reasoning |\n| Incremental updates | New facts propagated in milliseconds |\n| Correct retraction | Removed facts cleaned up precisely |\n\nThese capabilities normally require stitching together multiple systems - a graph database, a vector database, a rules engine, application code. InputLayer handles them all in one place.\n\n## Next steps\n\nThe [data modeling guide](/docs/guides/core-concepts/) covers how to design your knowledge graph schema. The [vectors guide](/docs/guides/vectors/) dives deeper into similarity search and HNSW indexes. And the [Python SDK](/docs/guides/python-sdk/) is the fastest way to integrate InputLayer into your applications.",
    "toc": [
      {
        "level": 2,
        "text": "Step 1: Start InputLayer",
        "id": "step-1-start-inputlayer"
      },
      {
        "level": 2,
        "text": "Step 2: Open the REPL",
        "id": "step-2-open-the-repl"
      },
      {
        "level": 2,
        "text": "Step 3: Store some facts",
        "id": "step-3-store-some-facts"
      },
      {
        "level": 2,
        "text": "Step 4: Define a rule",
        "id": "step-4-define-a-rule"
      },
      {
        "level": 2,
        "text": "Step 5: Ask a question",
        "id": "step-5-ask-a-question"
      },
      {
        "level": 2,
        "text": "Step 6: Add vector search",
        "id": "step-6-add-vector-search"
      },
      {
        "level": 2,
        "text": "Step 7: See incremental updates",
        "id": "step-7-see-incremental-updates"
      },
      {
        "level": 2,
        "text": "Step 8: See correct retraction",
        "id": "step-8-see-correct-retraction"
      },
      {
        "level": 2,
        "text": "What you just built",
        "id": "what-you-just-built"
      },
      {
        "level": 2,
        "text": "Next steps",
        "id": "next-steps"
      }
    ]
  },
  {
    "slug": "benchmarks-1587x-faster-recursive-queries",
    "title": "Benchmarks: 1,587x Faster Recursive Queries with Differential Dataflow",
    "date": "2026-02-15",
    "author": "InputLayer Team",
    "category": "Engineering",
    "excerpt": "How InputLayer's incremental computation engine delivers sub-millisecond updates on recursive queries over large graphs. The architecture behind the numbers.",
    "content": "\n# Benchmarks: 1,587x Faster Recursive Queries with Differential Dataflow\n\nWhen a single fact changes in a knowledge graph with 400,000 derived relationships, how much work should the system do?\n\nThe naive answer: recompute all 400,000 relationships from scratch. That's what most systems do. It takes 11 seconds.\n\nThe smart answer: figure out which relationships are actually affected by the change and update only those. That takes 6.83 milliseconds.\n\nThat's a **1,652x** difference. And it's the difference between \"we can check permissions in real time\" and \"we run a batch job overnight and hope nothing changes before morning.\"\n\n## The benchmark setup\n\nWe wanted to test something that reflects real-world usage, not a synthetic micro-benchmark. So we picked a common pattern: computing transitive authority in an organizational graph. This is the same kind of computation you'd need for access control chains, supply chain risk propagation, or entity resolution across corporate structures.\n\n```flow\n2,000 nodes -> ~6,000 edges -> ~400,000 derived relationships\n```\n\n```note\ntype: info\nThe test: add one new edge, then measure how long it takes to update all derived relationships.\n```\n\nThe 400,000 derived relationships come from the transitive nature of authority. If A manages B and B manages C, then A has authority over C. Follow that logic through 2,000 nodes with an average depth of 8-10 levels, and the number of derived relationships grows fast.\n\n## The results\n\n| Approach | Time | What it does |\n|---|---|---|\n| Full recomputation | 11,280 ms | Throws away all 400,000 derived relationships, re-derives them all |\n| InputLayer (incremental) | 6.83 ms | Identifies affected relationships, updates only those |\n\nFull recomputation doesn't care that you only changed one edge. It treats the entire graph as dirty and rebuilds everything. InputLayer's engine, on the other hand, traces the impact of the change through the derivation graph and touches only what's affected.\n\nTo put 6.83ms in perspective: that's fast enough to run inline with an API request. You can check permissions, compute supply chain exposure, or resolve entity relationships at query time rather than pre-computing them in a batch process.\n\n## The scaling story\n\nHere's where it gets really interesting. The incremental advantage doesn't stay constant as your graph grows - it gets *dramatically* better.\n\n| Graph size | Derived relationships | Full recompute | Incremental | Speedup |\n|---|---|---|---|---|\n| 500 nodes | ~25,000 | 420 ms | 1.2 ms | **350x** |\n| 1,000 nodes | ~100,000 | 2,800 ms | 3.1 ms | **903x** |\n| 2,000 nodes | ~400,000 | 11,280 ms | 6.83 ms | **1,652x** |\n\nLook at how the two columns grow. Full recomputation grows roughly quadratically - double the nodes, quadruple the time. But incremental updates grow much slower, because most single-fact changes only ripple through a small portion of the graph.\n\n```steps\n500 nodes: 420ms full vs 1.2ms incremental :: 350x faster\n1,000 nodes: 2,800ms full vs 3.1ms incremental :: 903x faster\n2,000 nodes: 11,280ms full vs 6.83ms incremental :: 1,652x faster [primary]\n```\n\nThis scaling behavior is fundamental, not accidental. Full recomputation has to process the entire graph regardless of what changed. Incremental updates process only the \"blast radius\" of the change, which stays relatively small even as the total graph grows.\n\nAt 10,000 nodes, the full recompute would take over a minute. The incremental update would still be in the low tens of milliseconds. That's the difference between a feature that's practical in production and one that isn't.\n\n## Why the numbers work this way\n\nInputLayer is built on [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow), a Rust library for incremental computation created by Frank McSherry. The core idea is simple: instead of storing derived results as static data, the engine represents everything as *differences* that can be efficiently passed along.\n\nHere's how a fact change flows through the system:\n\n```chain\nYou add an edge: \"Diana manages Eve\"\n-- who has authority over Diana?\nEngine finds: Alice and Bob (from existing derivations) [primary]\n-- so they must also have authority over Eve\nEngine checks: does Eve manage anyone? Yes - Frank\n-- so Alice and Bob also get authority over Frank\nEngine checks: does Frank manage anyone? No. Done. [success]\n=> Total work: 4 new derived relationships in ~2ms\n```\n\nThe engine didn't scan the entire graph. It didn't recompute relationships for nodes that weren't affected. It started from the change, followed the ripple effects, and stopped as soon as the ripple died out.\n\nFor recursive reasoning - like transitive authority where conclusions feed back into the computation - the engine runs a loop until it reaches a stable point where no new differences are produced. When something changes later, it re-enters that loop at the point of change and computes only the new differences.\n\nInputLayer also uses a technique called Magic Sets that makes queries demand-driven. When you ask \"who does Alice have authority over?\", the engine doesn't compute authority for every person in the organization. It starts from Alice and follows only the relevant paths. Query time becomes proportional to Alice's portion of the graph, not the entire organization.\n\n## Correct retraction: the hard part\n\nAdding facts is relatively straightforward to handle incrementally. Removing them is where things get genuinely hard.\n\nSay Alice has authority over Charlie through two independent paths:\n\n```flow\nAlice -> Bob -> Charlie [primary]\n```\n\n```flow\nAlice -> Diana -> Charlie [primary]\n```\n\nIf you remove Bob's management of Charlie, Alice should still have authority over Charlie through Diana. But if you remove Diana's management of Charlie too, the authority should disappear entirely.\n\nThe engine tracks this through weighted differences. Each derived relationship has a weight based on the number of independent paths that support it. When a path is removed, the weight goes down. Only when it reaches zero does the conclusion go away.\n\n```steps\nBoth paths exist: authority(Alice, Charlie) weight is 2 :: via Bob and Diana\nRemove Bob to Charlie: weight drops to 1 :: still exists via Diana [success]\nRemove Diana to Charlie: weight drops to 0 :: retracted [highlight]\n```\n\nOn our benchmark graph, retracting a single edge and propagating all downstream changes takes under 10ms. Bulk retractions (removing 100 edges) complete in about a second. Fast enough for real-time applications where facts change frequently.\n\n## What this means in practice\n\nThe practical takeaway here is about which architectural patterns become possible.\n\n**Without incremental computation**, you're stuck with batch processing. Pre-compute permissions overnight. Rebuild recommendation indexes hourly. Re-run compliance checks on a schedule. And accept that between runs, your derived data is stale.\n\n**With incremental computation**, you can do these things live:\n\n| Use case | Batch approach | Incremental approach |\n|---|---|---|\n| Access control | Nightly permission rebuild | Live permission check at query time |\n| Supply chain risk | Hourly risk recalculation | Instant risk update when a supplier status changes |\n| Compliance screening | Daily sanctions check | Real-time flag when ownership structure changes |\n| Recommendations | Model retrain every few hours | Instant update when user behavior or inventory changes |\n\nThe 1,652x speedup isn't about making a slow thing faster. It's about making batch-only workloads work in real time. That's a qualitative difference in what you can build.\n\n## Try it yourself\n\nInputLayer is open-source:\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nStart with the [quickstart guide](/docs/guides/quickstart/) to build your first knowledge graph, or dive into the [recursion documentation](/docs/guides/recursion/) to see how recursive reasoning works under the hood.",
    "toc": [
      {
        "level": 2,
        "text": "The benchmark setup",
        "id": "the-benchmark-setup"
      },
      {
        "level": 2,
        "text": "The results",
        "id": "the-results"
      },
      {
        "level": 2,
        "text": "The scaling story",
        "id": "the-scaling-story"
      },
      {
        "level": 2,
        "text": "Why the numbers work this way",
        "id": "why-the-numbers-work-this-way"
      },
      {
        "level": 2,
        "text": "Correct retraction: the hard part",
        "id": "correct-retraction-the-hard-part"
      },
      {
        "level": 2,
        "text": "What this means in practice",
        "id": "what-this-means-in-practice"
      },
      {
        "level": 2,
        "text": "Try it yourself",
        "id": "try-it-yourself"
      }
    ]
  },
  {
    "slug": "policy-filtered-semantic-search",
    "title": "Policy-Filtered Semantic Search: Access Control Meets Vector Similarity",
    "date": "2026-02-10",
    "author": "InputLayer Team",
    "category": "Architecture",
    "excerpt": "Most systems handle access control and semantic search as separate concerns. Here's what happens when you combine them into a single query.",
    "content": "\n# Policy-Filtered Semantic Search: Access Control Meets Vector Similarity\n\nEvery enterprise RAG application eventually runs into the same problem. The semantic search works great - it finds relevant documents. But then you need to check whether the user is actually allowed to see those documents. And suddenly you're maintaining two systems, writing glue code between them, and dealing with consistency bugs that only show up in production.\n\nThis post is about what happens when you stop treating authorization and retrieval as separate concerns and combine them into a single operation.\n\n## How it usually works (and where it breaks)\n\nThe typical architecture looks like this:\n\n```chain\nUser sends a query\n-- calls auth service\nAuth service returns permissions (e.g. department: \"engineering\", level: \"L5+\")\n-- passes filters to vector database\nVector database runs query with metadata filters\n-- returns results\nFiltered results [success]\n```\n\nThe application calls an auth service to figure out the user's permissions, translates those permissions into metadata filters, and passes those filters to the vector database along with the query. It works. For simple permission models - \"engineering can see engineering docs\" - it works fine.\n\nBut here's where it falls apart. Most real organizations don't have flat permission models. They have hierarchies. And hierarchies are recursive.\n\n## The recursion problem\n\nConsider Sarah, a VP of Engineering at a 500-person company. Her reporting chain looks like this:\n\n```tree\nSarah (VP Engineering) [primary]\n  Marcus (Dir. Platform)\n    Team Alpha (8 people)\n    Team Beta (6 people)\n  Priya (Dir. AI/ML)\n    Team Gamma (10 people)\n    Team Delta (5 people)\n  James (Dir. DevOps)\n    Team Epsilon (7 people)\n```\n\nSarah should be able to see documents from everyone in her org - all 36 people across 5 teams. Marcus should see documents from Alpha and Beta (14 people). A team lead in Alpha should see only Alpha's documents (8 people).\n\nHow do you express \"Sarah can see documents from everyone beneath her in the org chart\" as a metadata filter? You can't hardcode the list of 36 people - that becomes stale the moment someone joins, leaves, or transfers. You can't say `department = \"engineering\"` because that doesn't respect the sub-hierarchies (Marcus shouldn't see Priya's team's confidential documents unless the policy says so).\n\nWhat you actually need is a recursive walk of the reporting structure, starting from Sarah and going down through every layer. And that walk needs to happen at query time, against the current state of the org chart, every single time.\n\nThat's not something a metadata filter can do.\n\n## Combining authorization and search in one pass\n\nIn InputLayer, you describe your org structure as facts (who reports to whom) and your access policy as a rule (managers can see documents from their entire reporting chain). The engine handles both the authorization logic and the semantic search in a single query.\n\nHere's what happens when Sarah searches for \"deployment best practices\":\n\n```steps\nResolve authorization (recursive): start from Sarah, walk her full reporting chain :: Sarah can see docs from 36 people [primary]\nFind documents: filter to documents authored by those 36 people :: 847 documents in scope\nRank by similarity: compare each document's embedding to the query :: Top 10 results, all authorized, ranked by relevance [success]\n```\n\n```note\ntype: tip\nAll three steps happen in one pass. No separate auth service call, no metadata filter translation, no consistency gap.\n```\n\nThe authorization is evaluated against the current state of the org chart, right now, as part of the query.\n\n## The consistency problem most teams don't notice\n\nHere's a subtle bug that exists in nearly every two-system auth+search setup.\n\nMonday: Bob reports to Sarah. Sarah can see Bob's documents. The auth service knows this, the metadata filters reflect it.\n\nTuesday morning: Bob transfers from Engineering to Product. The auth service updates immediately. But the metadata on Bob's documents in the vector database? That gets updated in a batch job that runs at midnight.\n\nTuesday afternoon: Sarah searches for something. The auth service says she can't see Bob's docs anymore (correct). But what about documents authored by someone who reported to Bob, whose metadata filter was set to `org: \"engineering\"` and hasn't been updated yet? That depends on how your metadata propagation works. And these edge cases multiply with every layer of hierarchy and every type of permission grant.\n\n```chain\nBob transfers at 9am\n-- 15-hour consistency gap begins\nAuth says NO, but vector database says YES [highlight]\n-- batch job runs at midnight\nGap finally closes the next day [success]\n```\n\nWith InputLayer, this gap doesn't exist. Authorization is computed from the current facts at query time. Update the reporting structure at 9am, and the 9:01am query reflects the change. No propagation delay, no batch job, no stale permissions.\n\n## It also works in reverse: retraction\n\nWhen Bob leaves the company entirely, you retract the fact that Bob reports to Sarah.\n\nInputLayer automatically retracts everything that was derived through that relationship. Sarah loses access to Bob's documents. She also loses access to documents from anyone who reported through Bob - if Bob managed a team, that entire branch of Sarah's permission tree disappears.\n\nBut here's the important part: if any of those people also report to Sarah through a different path (say, a dotted-line relationship), those permissions survive. The engine tracks how many independent paths support each access grant and only retracts when all paths are gone.\n\nIn an append-only system, Bob's documents just sit there in the index until someone manually cleans them up. In InputLayer, the cleanup is automatic and precise.\n\n## Performance: can you actually do this at query time?\n\nEvaluating a recursive org chart walk on every search request sounds expensive. In practice, it's not.\n\nInputLayer's incremental computation engine means the recursive authorization isn't recomputed from scratch on every query. The first time Sarah queries, the engine walks her reporting chain. After that, it maintains the result incrementally. When the org chart changes, only the affected portion recomputes.\n\n| Operation | Time |\n|---|---|\n| Initial authority computation (2,000-node org) | ~200ms |\n| Incremental update after one org change | <7ms |\n| Subsequent queries (cached derivations) | <1ms for auth + vector search time |\n\nInputLayer also evaluates demand-driven. When Sarah queries, it doesn't compute the authorization chain for every person in the organization. It starts from Sarah and follows only her reporting paths. Query time scales with the size of Sarah's org, not the total company size.\n\nFor a VP with 100 reports (direct and transitive), the authorization adds negligible overhead to the vector search. For a CEO of a 10,000-person company - the most extreme case - it's still in the low tens of milliseconds.\n\n## Getting started\n\nIf you're dealing with authorization that's more complex than flat metadata filters - and most enterprise applications are - this pattern is worth exploring.\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) gets you running in about 5 minutes. The [recursion documentation](/docs/guides/recursion/) explains how recursive rules work, which is the foundation for hierarchical authorization.",
    "toc": [
      {
        "level": 2,
        "text": "How it usually works (and where it breaks)",
        "id": "how-it-usually-works-and-where-it-breaks"
      },
      {
        "level": 2,
        "text": "The recursion problem",
        "id": "the-recursion-problem"
      },
      {
        "level": 2,
        "text": "Combining authorization and search in one pass",
        "id": "combining-authorization-and-search-in-one-pass"
      },
      {
        "level": 2,
        "text": "The consistency problem most teams don't notice",
        "id": "the-consistency-problem-most-teams-dont-notice"
      },
      {
        "level": 2,
        "text": "It also works in reverse: retraction",
        "id": "it-also-works-in-reverse-retraction"
      },
      {
        "level": 2,
        "text": "Performance: can you actually do this at query time?",
        "id": "performance-can-you-actually-do-this-at-query-time"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  },
  {
    "slug": "building-product-recommendation-engine",
    "title": "Building a Product Recommendation Engine with InputLayer",
    "date": "2026-02-05",
    "author": "InputLayer Team",
    "category": "Tutorial",
    "excerpt": "A step-by-step guide to building a recommendation engine that combines collaborative filtering, product relationships, and semantic similarity in a single knowledge graph.",
    "content": "\n# Building a Product Recommendation Engine with InputLayer\n\nA customer just bought a DSLR camera. Your recommendation engine suggests... more cameras. Three other DSLRs in slightly different price ranges.\n\nThe customer doesn't need another camera. They need a lens, a memory card, and a bag to carry it all in. But those items look nothing like a camera in embedding space. The connection between \"camera\" and \"camera bag\" is *logical* (one is an accessory for the other), not *semantic* (their product descriptions have little overlap).\n\nThis is the gap between similarity-based recommendations and reasoning-based recommendations. In this tutorial, we'll build an engine that handles both - and a few other things that traditional recommenders struggle with.\n\n## What we're building\n\nBy the end of this tutorial, you'll have a recommendation engine with four distinct signals:\n\n```tree\nRecommendation Engine [primary]\n  Collaborative Filtering\n    \"users who bought X also bought Y\"\n  Category Affinity (recursive)\n    \"related product categories, at any depth\"\n  Semantic Similarity\n    \"products with similar descriptions\"\n  Accessory Relationships\n    \"this product goes with that one\"\n```\n\n```note\ntype: info\nResults are combined, de-duplicated, and filtered: already purchased items are excluded, out-of-stock items are excluded, and discontinued items are removed automatically.\n```\n\nEach signal is expressed as a simple, readable rule. The engine combines them automatically. And because this runs on a knowledge graph with incremental computation, the recommendations stay fresh without model retraining or index rebuilding.\n\n## Step 1: Model your product catalog\n\nEverything starts with your product data. In InputLayer, this means storing structured facts about products, their categories, and how categories relate to each other.\n\nYou store each product with its name and direct category. Then you describe the category hierarchy - running shoes fall under athletic footwear, which falls under footwear, which falls under apparel. This hierarchy is the backbone for one of our recommendation signals.\n\nYou also store embedding vectors for each product, generated from product descriptions using a text embedding model. These power the semantic similarity signal.\n\n```tree\nSports [primary]\n  Athletic\n    Footwear\n      SKU_001 \"Running Shoes\"\n      SKU_002 \"Trail Shoes\"\n    Accessories\n      SKU_003 \"Running Socks\"\n      SKU_004 \"Hydration Pack\"\n    Electronics\n      SKU_005 \"GPS Watch\"\n```\n\n## Step 2: Feed in user behavior\n\nNext, purchase history and browsing data. Who bought what, and what have they been looking at recently. In production, you'd ingest this from your transaction database as events happen.\n\n```tree\nPurchase History\n  user_1: Running Shoes, Running Socks\n  user_2: Running Shoes, Hydration Pack\n  user_3: Trail Shoes, GPS Watch\nBrowsing Data\n  user_1 viewed: Trail Shoes, GPS Watch [muted]\n```\n\nThe important thing: these aren't just rows in a table. They're facts in a knowledge graph that the reasoning engine can combine with other facts through rules. That's the key difference from a traditional recommendation database.\n\n## Step 3: Define recommendation rules\n\nThis is where the approach diverges from traditional ML recommendations. Instead of training a model, we express recommendation logic as rules. Each rule captures a different signal, and each rule is readable in plain English.\n\n**Rule 1 - Collaborative filtering:** \"If two users bought the same product, the other products each user bought become recommendations for the other.\" This is the classic \"customers who bought X also bought Y\" pattern. But it's expressed as a rule, not a matrix factorization - which means you can read it, debug it, and explain exactly why a recommendation appeared.\n\nWhat this looks like in practice for user_1:\n\n```chain\nuser_1 bought Running Shoes\n-- who else bought Running Shoes?\nuser_2 also bought Running Shoes [primary]\n-- what else did user_2 buy?\nuser_2 also bought Hydration Pack\n=> Recommend Hydration Pack to user_1 [success]\n```\n\n**Rule 2 - Category affinity (recursive):** \"If a user bought something in one category, recommend products from related categories.\" This rule is recursive - it follows the category hierarchy to find related categories at any depth.\n\n```chain\nuser_1 bought Running Shoes (in footwear)\n-- walk up the category tree\nFootwear is under Athletic [primary]\n-- what else is under Athletic?\nAccessories and Electronics are also under Athletic\n=> Recommend from related categories: Hydration Pack, GPS Watch [success]\n```\n\nBuying running shoes surfaces recommendations not just from footwear, but from accessories and electronics too, because they share a parent category. And this works no matter how deep or wide your category tree goes.\n\n**Rule 3 - Semantic similarity:** Products with similar descriptions (as measured by their embedding vectors) become recommendations. This catches relationships that the category hierarchy misses - two products from completely different categories that people tend to use together.\n\n**Rule 4 - Accessory relationships:** \"When a customer buys a product, recommend its accessories - but only if they haven't already bought them and they're in stock.\" This is the explicit knowledge that a camera bag goes with a camera, expressed directly rather than inferred statistically.\n\n## Step 4: Combine and query\n\nNow you ask: \"What should we recommend to user_1?\"\n\nThe engine evaluates all four rules, combines their results, filters out products user_1 has already bought, checks stock availability, and returns the final list:\n\n```tree\nSignals for user_1 [primary]\n  Collaborative: Hydration Pack (via user_2)\n  Category: Hydration Pack, GPS Watch, Trail Shoes\n  Semantic: Trail Shoes (0.92 similarity to Running Shoes)\n  Accessory: (none defined in this example) [muted]\n```\n\n```steps\nTrail Shoes - matched by category + semantic similarity :: strongest combined signal [primary]\nHydration Pack - matched by collaborative + category :: two independent signals [primary]\nGPS Watch - matched by category :: single signal\n```\n\nEach recommendation carries its provenance. You can explain to the user *why* each item was recommended, and you can explain to your product team which signals are driving the most engagement. Try getting that kind of transparency from a neural collaborative filtering model.\n\n## Step 5: Watch it stay fresh\n\nHere's where the knowledge graph approach really shines compared to model-based recommenders.\n\n**A new purchase comes in.** User_1 buys a GPS Watch. You add that fact. All recommendations update instantly - GPS Watch drops out of user_1's recommendations (already purchased), and any collaborative filtering signals that involve GPS Watch recalculate. No model retraining needed.\n\n**A product goes out of stock.** You update the stock status for Trail Shoes. Every recommendation that included Trail Shoes disappears from results automatically. When it's back in stock, the recommendations come back. No index rebuild needed.\n\n**A product is discontinued.** You retract it from the catalog entirely. InputLayer's correct retraction mechanism removes it from every recommendation result, every collaborative filtering signal, every category association - automatically and immediately. No stale suggestions pointing customers to a product page that returns a 404.\n\n```flow\nTraditional ML recommender [highlight] -> Retrain model (hours) -> Rebuild index (minutes) -> Deploy (minutes)\n```\n\n```flow\nInputLayer [success] -> Retract fact -> Recommendations update (~ms) -> Done\n```\n\n## Where to take this next\n\nWhat we've built is the foundation. Here are the layers you'd add for production:\n\n**Inventory-aware filtering** - only recommend products that are actually in stock and available in the customer's region. This is one more condition on the recommendation rule.\n\n**Time decay** - weight recent purchases more heavily than old ones. A customer who bought running shoes yesterday is more likely to need accessories than a customer who bought them two years ago.\n\n**Price affinity** - recommend products in the customer's typical price range. If they buy premium products, don't recommend budget options.\n\n**Seasonal rules** - boost winter gear in November, swimwear in May. Express seasonality as a rule rather than baking it into a training set.\n\nEach of these is just another rule in the knowledge graph. The engine handles the interactions between all rules automatically - you don't need to worry about how time decay interacts with category affinity, or how inventory filtering affects collaborative signals. Define the rules, and the engine composes them.\n\nCheck out the [data modeling guide](/docs/guides/core-concepts/) for patterns that work well at scale, and the [Python SDK](/docs/guides/python-sdk/) for integrating this into your e-commerce platform.",
    "toc": [
      {
        "level": 2,
        "text": "What we're building",
        "id": "what-were-building"
      },
      {
        "level": 2,
        "text": "Step 1: Model your product catalog",
        "id": "step-1-model-your-product-catalog"
      },
      {
        "level": 2,
        "text": "Step 2: Feed in user behavior",
        "id": "step-2-feed-in-user-behavior"
      },
      {
        "level": 2,
        "text": "Step 3: Define recommendation rules",
        "id": "step-3-define-recommendation-rules"
      },
      {
        "level": 2,
        "text": "Step 4: Combine and query",
        "id": "step-4-combine-and-query"
      },
      {
        "level": 2,
        "text": "Step 5: Watch it stay fresh",
        "id": "step-5-watch-it-stay-fresh"
      },
      {
        "level": 2,
        "text": "Where to take this next",
        "id": "where-to-take-this-next"
      }
    ]
  },
  {
    "slug": "why-we-built-on-differential-dataflow",
    "title": "Why We Built InputLayer on Differential Dataflow",
    "date": "2026-01-30",
    "author": "InputLayer Team",
    "category": "Engineering",
    "excerpt": "The story behind our choice of Timely and Differential Dataflow as the computation engine, and what that means for the kinds of problems InputLayer can solve.",
    "content": "\n# Why We Built InputLayer on Differential Dataflow\n\nEvery engineering team has that one decision that shaped everything that came after. For us, it was choosing [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow) as the computation engine underneath InputLayer. It determined what we could build, what performance we could offer, and which problems we could solve that other systems can't.\n\nThis is the story of why we made that choice and what it means for the people building on InputLayer today.\n\n## The problem that started everything\n\nWe wanted to build a knowledge graph engine that could do something deceptively simple: keep derived conclusions up to date when facts change.\n\nThat sounds straightforward until you think about scale. Imagine a knowledge graph with 100,000 facts and 50 rules that derive new conclusions from those facts. Some of those rules are recursive - their output feeds back into their input. The initial computation produces millions of derived facts. Fine - that's a one-time cost.\n\nBut then a single fact changes. One employee transfers departments. One entity gets added to a sanctions list. One product goes out of stock.\n\n```chain\n100,000 source facts feed into 50 rules\n-- initial computation\n2,000,000 derived facts [primary]\n-- then one fact changes\nHow many of those 2M derived facts are affected? [highlight]\n=> Usually just a few hundred. But the naive approach recomputes all 2 million.\n```\n\nWith a naive approach, you throw away all 2 million derived facts and recompute them from scratch. For small graphs, that's fast enough. For production workloads, it doesn't work. At 11 seconds per recomputation on a 2,000-node graph, you're locked into batch processing. Real-time permission checks, live compliance screening, instant recommendation updates - none of that is practical.\n\nWe needed an engine that could update just the affected derivations, correctly, in milliseconds.\n\n## What we evaluated\n\nWe spent months evaluating different approaches. Each one taught us something about what we needed.\n\n**Batch-oriented engines** are the gold standard for one-time rule evaluation. They compile rules into extremely efficient programs that process an entire dataset in one pass. Some even generate low-level code that runs blazingly fast for batch workloads.\n\nThe limitation: there's no concept of \"update.\" If you add a fact, you rerun the entire program. For a knowledge graph that changes frequently - which is most production use cases - that means paying the full computation cost every time anything changes.\n\n**Graph databases** offer incremental capabilities for simple path queries. But their query languages weren't designed for recursive derivation. They can traverse stored edges, but they can't derive *new* edges based on rules and then recursively reason over those derived edges. And they don't maintain results incrementally when the graph changes.\n\n**Building from scratch** was tempting. We could design an incremental engine perfectly suited to our needs. But correct incremental maintenance through recursive fixpoints is one of the hardest problems in database research. Tracking which derived facts should retract when a source fact is removed - especially when derived facts might have multiple supporting paths - is notoriously subtle. Teams that have tried typically spend years before reaching production quality.\n\n```tree\nWhat we needed [primary]\n  Fast batch computation\n  Incremental updates (not full recompute)\n  Recursive derivation (rules that reference themselves)\n  Correct retraction (delete actually deletes)\n  Reasonable time to production\n```\n\n```note\ntype: warning\nNo single existing approach gave us everything we needed. Batch engines lacked incremental updates. Graph databases lacked recursive derivation. Building from scratch would take years.\n```\n\n## Finding Differential Dataflow\n\nThen we found Frank McSherry's work on [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow), built on top of [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow). Both are Rust libraries. The performance was a bonus. The computational model was the real discovery.\n\nThe core idea is simple enough to explain in a paragraph: instead of storing derived data as static results, the engine represents everything as *weighted differences*. Adding a fact is a +1 difference. Removing a fact is a -1 difference. Every computation in the system processes differences in and produces differences out. This means every operation is naturally incremental - it never looks at the whole dataset, only at what changed.\n\n```flow\nTraditional: Input facts -> Compute ALL -> Static results\n```\n\n```flow\nAfter change (traditional): Changed fact -> Compute ALL again [highlight] -> Rebuilt results\n```\n\n```flow\nDifferential: Changed fact -> Compute DIFFERENCE only [success] -> Only changed derivations\n```\n\n## How it handles the hard part: recursive retraction\n\nThe real test of an incremental system isn't additions - it's deletions. And specifically, deletions through recursive derivation chains.\n\nHere's the scenario that breaks naive incremental systems. Alice has authority over Charlie through two independent paths:\n\n```flow\nPath 1: Alice -> Bob -> Charlie [primary]\n```\n\n```flow\nPath 2: Alice -> Diana -> Charlie [primary]\n```\n\nRemove Bob's management of Charlie. Does Alice lose authority over Charlie? *No* - the path through Diana still supports it. Now remove Diana's management of Charlie too. Does Alice lose authority over Charlie? *Yes* - there are no remaining paths.\n\nDifferential Dataflow handles this through its weight-based model. Each derived fact carries a weight representing the number of independent derivation paths. Removing a path decreases the weight. The fact only retracts when the weight hits zero.\n\n```steps\nBoth paths exist: authority(Alice, Charlie) weight is 2 :: Alive\nRemove Bob's path: weight drops to 1 :: Still alive - Diana's path remains [success]\nRemove Diana's path: weight drops to 0 :: Retracted [highlight]\n```\n\nThis sounds simple in theory. In practice, getting it right through multiple levels of recursive derivation, where intermediate conclusions can also have multiple support paths, is extraordinarily difficult. Differential Dataflow solves it at the engine level, which means we didn't have to.\n\n## What this gives InputLayer users\n\nBuilding on Differential Dataflow gave us three properties that show up directly in what you can build with InputLayer.\n\n**Incremental maintenance:** When a fact changes, only the affected derivations recompute. On a 2,000-node graph with 400,000 derived relationships, updating a single edge takes 6.83ms instead of 11.3 seconds. That's a 1,652x speedup that turns batch-only workloads into real-time operations.\n\n**Correct retraction:** Delete a fact, and everything derived through it disappears - but only if there's no alternative derivation path. Phantom permissions, stale recommendations, lingering compliance flags - these bugs simply don't exist when the engine handles retraction correctly.\n\n**Demand-driven evaluation:** We combined Differential Dataflow with Magic Sets optimization, which rewrites recursive rules to only compute what's needed for a specific query. Ask \"who does Alice have authority over?\" and the engine starts from Alice and follows only her paths - it doesn't compute authority for the entire organization. Query time is proportional to the relevant portion of the graph.\n\n## The tradeoffs\n\nNo engineering decision is free. Here's what we trade.\n\n**Memory:** Differential Dataflow maintains operator state in memory. For very large datasets, memory usage grows with the size of the maintained derivations. We handle this with persistent storage - Parquet files plus a write-ahead log - that lets us recover state without keeping everything in memory indefinitely. But it's a real consideration for very large knowledge graphs.\n\n**Complexity floor:** The Timely/Differential Dataflow programming model is powerful but has a steep learning curve. We invested significant engineering time building the abstraction layer that compiles high-level rules into efficient dataflow graphs. Users never touch the dataflow layer directly - but we do, and it required deep expertise to get right.\n\n**Single-node:** Currently, InputLayer runs on a single node. Timely Dataflow supports distributed computation, and that's on our roadmap. But today, the engine is bounded by what a single machine can handle. For most knowledge graph workloads, that's millions of facts and derived relationships - but it's a real limit for truly massive datasets.\n\n## Where the choice matters most\n\nThe Differential Dataflow foundation matters most for use cases where data changes frequently and derived conclusions need to stay current. Access control hierarchies where people change roles regularly. Supply chain graphs where supplier status changes daily. Compliance systems where entity relationships and sanctions lists are updated constantly. Agent memory systems where new observations arrive continuously.\n\nFor batch-once-query-many workloads with no updates, a simpler engine would be fine. But the moment your facts change and you need derived conclusions to stay correct, the incremental approach pays for itself immediately.\n\nOur [benchmarks post](/blog/benchmarks-1587x-faster-recursive-queries/) has the specific numbers. And the [quickstart guide](/docs/guides/quickstart/) gets you running in about 5 minutes so you can see it in action.",
    "toc": [
      {
        "level": 2,
        "text": "The problem that started everything",
        "id": "the-problem-that-started-everything"
      },
      {
        "level": 2,
        "text": "What we evaluated",
        "id": "what-we-evaluated"
      },
      {
        "level": 2,
        "text": "Finding Differential Dataflow",
        "id": "finding-differential-dataflow"
      },
      {
        "level": 2,
        "text": "How it handles the hard part: recursive retraction",
        "id": "how-it-handles-the-hard-part-recursive-retraction"
      },
      {
        "level": 2,
        "text": "What this gives InputLayer users",
        "id": "what-this-gives-inputlayer-users"
      },
      {
        "level": 2,
        "text": "The tradeoffs",
        "id": "the-tradeoffs"
      },
      {
        "level": 2,
        "text": "Where the choice matters most",
        "id": "where-the-choice-matters-most"
      }
    ]
  },
  {
    "slug": "fraud-detection-entity-chain-reasoning",
    "title": "Fraud Detection Through Entity Chain Reasoning",
    "date": "2026-01-25",
    "author": "InputLayer Team",
    "category": "Use Case",
    "excerpt": "How knowledge graph reasoning uncovers fraud that pattern matching misses - by following chains of entity relationships across corporate structures and transaction flows.",
    "content": "\n# Fraud Detection Through Entity Chain Reasoning\n\nHere's a transaction your screening system flagged as clean: a wire transfer from your client to Alpha Corp for $50,000. Alpha Corp is a registered corporation with a clean record. Nothing suspicious.\n\nExcept Alpha Corp is a subsidiary of Beta LLC. Beta LLC is 60% owned by Gamma Holding. And Gamma Holding is 80% controlled by someone on a sanctions list.\n\n```chain\nYour client sends $50K to Alpha Corp\n-- subsidiary of\nBeta LLC\n-- 60% owned by\nGamma Holding\n-- 80% owned by\nSANCTIONED ENTITY [highlight]\n=> Four hops deep. Each record looks clean in isolation. The violation is only visible through the chain.\n```\n\nTraditional fraud detection systems check the direct counterparty against a list. That catches the obvious cases. It completely misses the layered structures that sophisticated actors actually use.\n\n## Why pattern matching can't solve this\n\nMost fraud detection runs on rules over individual transactions. Flag transactions over $10,000. Flag transactions to high-risk jurisdictions. Flag counterparties that appear on sanctions lists. These rules work on a per-transaction basis, looking at fields on a single record.\n\nThe structural fraud problem is fundamentally different. You're not looking for a suspicious field on a single transaction. You're looking for a suspicious *path* through a network of entity relationships - a path that might not exist in any single system.\n\n```tree\nWhat pattern matching sees [muted]\n  Transaction #TX-001\n    From: Our Client\n    To: Alpha Corp\n    Amount: $50,000\n    Sanctions match: NO\n    PEP match: NO\n    High-risk jurisdiction: NO\n```\n\n```tree\nWhat chain reasoning sees [highlight]\n  Alpha Corp\n    subsidiary of Beta LLC\n      60% owned by Gamma Holding\n        80% owned by SANCTIONED PERSON\n  Indirect sanctions exposure: YES\n```\n\nThe information exists across separate registries - corporate records, ownership filings, sanctions lists. No single database contains the complete picture. You have to follow the chain.\n\n## How InputLayer traces these chains\n\nIn InputLayer, you model entity relationships as facts: \"Person X owns 80% of Company Y.\" \"Company A is a subsidiary of Company B.\" These facts come from corporate registries, ownership databases, and KYC records - data you probably already collect.\n\nThen you define the compliance logic as a rule: \"An entity has sanctions exposure if it's directly sanctioned, or if it's owned (above a threshold) by an entity that has sanctions exposure.\"\n\nThat second clause is recursive. It says: trace the ownership chain as deep as it goes, and at every level, check whether the owner has sanctions exposure. If the owner does - whether directly or through its own ownership chain - the exposure flows down.\n\nHere's what the engine does when it evaluates this rule against our example:\n\n```steps\nIs Alpha Corp directly sanctioned? No. :: Check direct status\nWho owns Alpha Corp? Beta LLC (subsidiary). :: Walk up one level\nIs Beta LLC directly sanctioned? No. :: Check direct status\nWho owns Beta LLC? Gamma Holding (60%, above 25% threshold). :: Walk up one level\nIs Gamma Holding directly sanctioned? No. :: Check direct status\nWho owns Gamma Holding? Sanctioned Person (80%, above 25% threshold). :: Walk up one level\nIs Sanctioned Person directly sanctioned? YES. :: Match found [highlight]\n```\n\n```chain\nSanctioned Person is sanctioned\n-- exposure flows down through ownership\nGamma Holding has indirect sanctions exposure [highlight]\n-- exposure flows down\nBeta LLC has indirect sanctions exposure [highlight]\n-- exposure flows down\nAlpha Corp has indirect sanctions exposure [highlight]\n=> Transaction TX-001 is FLAGGED\n```\n\nThe engine didn't just check the direct counterparty. It walked the full ownership and control chain, evaluated the sanctions exposure rule at every level, and passed the result back down. All automatically, from a single rule definition.\n\nAnd this works for chains of any depth. Five layers of shell companies? No problem. Ten intermediaries? The engine follows the chain until there's nowhere left to go.\n\n## Beneficial ownership: the same pattern, different question\n\nRegulators worldwide are tightening beneficial ownership requirements. The core question is: who are the natural persons that ultimately own or control this entity?\n\nThe computation is surprisingly similar to sanctions screening, with one twist: you need to multiply ownership percentages through the layers.\n\n```flow\nPerson X (80%) -> Holding A (60%) -> Company B [primary]\n```\n\nEffective beneficial ownership of Person X in Company B: 80% x 60% = 48%. If your regulatory threshold is 25%, Person X is a beneficial owner of Company B even though they don't own it directly.\n\nAdd more layers, and the math compounds:\n\n```flow\nPerson X (80%) -> Holding A (60%) -> Sub B (70%) -> Company C [primary]\n```\n\nEffective ownership: 80% x 60% x 70% = 33.6%. Still above 25% - Person X is a beneficial owner of Company C.\n\nInputLayer handles the multiplication and propagation through any number of layers. Define a threshold, and the engine identifies every natural person who qualifies as a beneficial owner for every entity in your graph.\n\n## What happens when facts change\n\nThis is where the knowledge graph approach becomes especially valuable for compliance. Entity relationships change constantly. Companies are acquired. Ownership stakes are transferred. New sanctions designations are published. Old ones are lifted.\n\nWhen you add a new sanctions designation - say Gamma Holding's owner gets added to the list - InputLayer propagates the change immediately. It identifies every entity in that person's ownership chain, evaluates whether the ownership thresholds are met, and flags the affected transactions. On a graph with thousands of entities, this takes milliseconds.\n\n```flow\nBefore (batch): Sanctions list updated [highlight] -> Full recomputation (seconds to minutes) -> Alerts are stale until done\n```\n\n```flow\nWith InputLayer: Sanctions list updated [success] -> Incremental update (milliseconds) -> Alerts are current immediately\n```\n\nThe reverse is equally important. When someone is removed from a sanctions list, all the downstream flags that were derived through their ownership chain clear automatically. No manual cleanup, no stale alerts clogging up your compliance team's queue. And if an entity had sanctions exposure through *multiple* paths (e.g., owned by two sanctioned individuals), removing one designation correctly preserves the remaining exposure.\n\n## Structuring detection: connecting related entities\n\nBeyond direct sanctions, compliance teams need to detect structuring - splitting large transactions into smaller ones to avoid reporting thresholds. The standard approach checks individual transactions against the $10,000 threshold. Sophisticated actors split transactions across related entities to stay below it.\n\n```tree\nSanctioned Person [highlight]\n  Entity A\n  Entity B\n  Entity C\n```\n\n```chain\nEntity A sends $4,000 to Target Company\n-- related entity\nEntity B sends $3,500 to Target Company\n-- related entity\nEntity C sends $3,000 to Target Company\n=> Combined total: $10,500 - above threshold [highlight]\n```\n\nEach individual transaction is below $10,000. But the entities are related through common ownership, and their combined transactions to the same target exceed the threshold.\n\nInputLayer's recursive reasoning identifies these relationships automatically. It determines which entities are connected through any chain of ownership, aggregates their transactions within a time window, and fires an alert when the combined total exceeds the threshold. The \"related entity\" determination is itself a recursive walk - Entity A and Entity C might be connected through multiple intermediate layers.\n\n## Getting started\n\nIf you're working on compliance, sanctions screening, or transaction monitoring, this approach to entity chain reasoning is worth exploring.\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) gets you running in about 5 minutes. The [recursion documentation](/docs/guides/recursion/) covers the recursive reasoning that powers entity chain traversal.",
    "toc": [
      {
        "level": 2,
        "text": "Why pattern matching can't solve this",
        "id": "why-pattern-matching-cant-solve-this"
      },
      {
        "level": 2,
        "text": "How InputLayer traces these chains",
        "id": "how-inputlayer-traces-these-chains"
      },
      {
        "level": 2,
        "text": "Beneficial ownership: the same pattern, different question",
        "id": "beneficial-ownership-the-same-pattern-different-question"
      },
      {
        "level": 2,
        "text": "What happens when facts change",
        "id": "what-happens-when-facts-change"
      },
      {
        "level": 2,
        "text": "Structuring detection: connecting related entities",
        "id": "structuring-detection-connecting-related-entities"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  },
  {
    "slug": "when-similarity-is-not-enough",
    "title": "InputLayer + Your Vector Database: When Similarity Is Not Enough",
    "date": "2026-01-20",
    "author": "InputLayer Team",
    "category": "Architecture",
    "excerpt": "Your vector database handles similarity search beautifully. But some queries need reasoning, not just retrieval. Here's how to know when you need both.",
    "content": "\n# InputLayer + Your Vector Database: When Similarity Is Not Enough\n\nYour vector database is probably doing exactly what you need it to do. Embed documents, search by similarity, feed context to your LLM. For a lot of use cases, that pipeline works beautifully and you shouldn't change it.\n\nBut at some point - maybe you've already hit it - you'll encounter queries where the results are *relevant* but not *correct*. The returned documents are genuinely similar to the query. They just don't answer the actual question, because the answer requires connecting dots that no similarity metric can connect.\n\nThis post is about recognizing that moment and understanding what to do about it.\n\n## A tale of two questions\n\nHere's the clearest way to see the difference. Consider two questions a financial analyst might ask:\n\n**Question A:** \"Show me recent reports about risk management.\"\n\nThis is a similarity question. The answer is a set of documents whose content is semantically close to \"risk management.\" Your vector database handles this perfectly. Embed the query, find nearest neighbors, done.\n\n**Question B:** \"Does our client have exposure to any sanctioned entities?\"\n\nThis is a reasoning question. The answer requires tracing ownership chains through corporate structures - Entity A owns 60% of Entity B, which has a subsidiary C, which is on a sanctions list. No single document contains this answer. The information is spread across entity registrations, ownership records, and sanctions lists.\n\n```flow\nQuestion A: similarity search [success] -> Relevant documents found\n```\n\n```chain\nQuestion B: similarity search\n-- finds documents that mention sanctions\nBut that's not the same as HAVING exposure [highlight]\n-- needs reasoning instead\nTrace ownership chain through entity relationships\n=> Yes or No answer (from connected facts, not document similarity) [success]\n```\n\nYour vector database will find documents that *mention* sanctions for Question B. It might even find documents about your client. But it can't trace the ownership chain that connects them. That connection is structural, not semantic.\n\n## Three signs you've hit the wall\n\nOver time, we've noticed three patterns that signal teams need reasoning alongside their retrieval.\n\n### 1. You're writing multi-query orchestration code\n\nThe first sign is architectural. You find yourself writing application code that makes multiple database calls and stitches the results together. Query the vector database for relevant docs. Query a graph for relationships. Hit an auth service. Reconcile everything in application code.\n\n```\n// This code smell means you need a reasoning layer\nconst docs = await vectorDB.search(queryEmbedding, topK=50);\nconst userPerms = await authService.getPermissions(userId);\nconst filteredDocs = docs.filter(d =>\n  userPerms.departments.includes(d.metadata.department) ||\n  userPerms.teams.includes(d.metadata.team) ||\n  (d.metadata.author && await orgChart.isSubordinate(d.metadata.author, userId))\n);\n// ^ This recursive check is the red flag\n```\n\nThe recursive `isSubordinate` check at the end is the tell. You've hit a reasoning problem and you're trying to solve it with imperative code and API calls. It works, but it's fragile, slow, and hard to keep consistent.\n\n### 2. Your metadata filters can't express the access policy\n\nThis is the access control version. Your permission model started simple - department-based, maybe role-based. But now it involves hierarchies: managers can see their reports' documents, and their reports' reports, and so on down the chain.\n\n```chain\nSimple access control (works fine)\n-- metadata filter\nFilter: department = \"engineering\" [success]\n```\n\n```chain\nComplex access control (breaks down)\n-- metadata filter\nFilter: author in ??? [highlight]\n-- you don't know the list\nYou need to recursively traverse the org chart first\n=> Can't express \"everyone in Alice's reporting chain\" as a flat filter\n```\n\nYou can't express \"everyone in Alice's transitive reporting chain\" as a flat metadata filter because you don't know who's in that chain until you recursively traverse the org chart. And that chain changes every time someone joins, leaves, or transfers.\n\n### 3. Stale derived data is accumulating silently\n\nThe third sign is the most sneaky because it's invisible at first. Your system has derived some conclusions - cached recommendations, pre-computed access lists, materialized views - and the source data has changed, but the conclusions haven't updated.\n\nA partner relationship ended three months ago. The partnership flag was removed from the CRM. But the integration recommendations, the priority support routing, the shared document access - those derived conclusions are still sitting in various caches and indexes. Nobody cleaned them up because nobody knows all the places they spread to.\n\n```tree\nFact: Partner relationship ended (March) [highlight]\n  Still in vector index: \"Company X gets priority support\" (from April doc) [muted]\n  Still in recommendations: \"Try Company X's integration\" (stale since March) [muted]\n  Still in access list: Company X employees see partner docs (stale since March) [muted]\n```\n\n```note\ntype: warning\nIn InputLayer, retracting the partnership fact automatically retracts every conclusion derived from it. In a system without proper retraction, these stale conclusions accumulate month after month.\n```\n\n## How the two systems complement each other\n\nThe mental model is simple:\n\n```flow\nYour vector database [primary] -> \"What content looks most like this query?\"\n```\n\n```flow\nInputLayer [primary] -> \"What can be concluded from these facts and rules?\"\n```\n\nMost real applications need both. A customer support agent needs to find relevant help articles (vector search) and check the customer's subscription tier (reasoning). A research assistant needs to find related papers (vector search) and trace the citation graph to foundational work (graph reasoning). A financial advisor needs to find matching investment products (vector search) and verify regulatory compliance (rule evaluation).\n\nThe cleanest pattern is straightforward: use each system for what it's best at. Keep your vector database for similarity queries. Add InputLayer for the reasoning queries. For the cases where you need both at the same time - \"find documents similar to X that this user is authorized to see through their reporting chain\" - InputLayer handles the combined query in a single pass with its native vector search capabilities.\n\n## When to stick with just your vector database\n\nNot every application needs reasoning. If your queries are straightforward similarity lookups with simple metadata filters, your vector database is the right tool and adding InputLayer would be unnecessary complexity.\n\nThe honest assessment: if you don't have any of the three signs above - no multi-query orchestration, no hierarchical access control, no stale derived data - you probably don't need InputLayer yet. And that's fine. Build with what works today and add the reasoning layer when you actually need it.\n\nThe trigger is when you find yourself building a reasoning engine inside your application code. When that happens, you're better off using one that's purpose-built.\n\n## Getting started\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) takes about 5 minutes. If you're specifically interested in combining vector search with reasoning, the [vectors documentation](/docs/guides/vectors/) covers InputLayer's native vector capabilities.",
    "toc": [
      {
        "level": 2,
        "text": "A tale of two questions",
        "id": "a-tale-of-two-questions"
      },
      {
        "level": 2,
        "text": "Three signs you've hit the wall",
        "id": "three-signs-youve-hit-the-wall"
      },
      {
        "level": 3,
        "text": "1. You're writing multi-query orchestration code",
        "id": "1-youre-writing-multi-query-orchestration-code"
      },
      {
        "level": 3,
        "text": "2. Your metadata filters can't express the access policy",
        "id": "2-your-metadata-filters-cant-express-the-access-policy"
      },
      {
        "level": 3,
        "text": "3. Stale derived data is accumulating silently",
        "id": "3-stale-derived-data-is-accumulating-silently"
      },
      {
        "level": 2,
        "text": "How the two systems complement each other",
        "id": "how-the-two-systems-complement-each-other"
      },
      {
        "level": 2,
        "text": "When to stick with just your vector database",
        "id": "when-to-stick-with-just-your-vector-database"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  },
  {
    "slug": "correct-retraction-why-delete-should-actually-delete",
    "title": "Correct Retraction: Why Delete Should Actually Delete",
    "date": "2026-01-15",
    "author": "InputLayer Team",
    "category": "Engineering",
    "excerpt": "When you delete a fact from a knowledge graph, what happens to everything that was derived from it? Most systems get this wrong. Here's why it matters and how InputLayer handles it.",
    "content": "\n# Correct Retraction: Why Delete Should Actually Delete\n\nThree months after a security incident, the forensics team discovers something troubling. A former employee - let's call him Bob - had his access revoked on the day he left. His account was deactivated. His role was removed from the auth system.\n\nBut Bob had authority over a team of six people. Those six people had authored documents. The system had derived that Bob's manager, Alice, could access those documents through Bob. When Bob left, his direct access disappeared. But Alice's transitive access to those documents - the part that was *derived* through Bob's position - was never cleaned up.\n\nFor three months, Alice had access to documents she shouldn't have been able to see. Not because anyone made an error, but because the system didn't properly retract derived conclusions when a source fact was removed.\n\nThis is the correct retraction problem. And it's one of the most under-appreciated issues in data systems that derive conclusions from connected facts.\n\n## Simple on the surface, hard underneath\n\nAt first glance, retraction seems trivial. Delete a fact, delete everything that depended on it. Done.\n\nLet's walk through why it's not that simple.\n\nAlice manages Bob. Bob manages Charlie. The system derives transitive authority:\n\n```tree\nAlice [primary]\n  Bob (direct report)\n    Charlie (Bob's direct report)\n```\n\n```steps\nAlice has authority over Bob :: direct\nBob has authority over Charlie :: direct\nAlice has authority over Charlie :: transitive: Alice to Bob to Charlie\n```\n\nBob leaves the company. You remove \"Alice manages Bob.\" What should happen?\n\n```steps\nAlice has authority over Bob :: RETRACT - no longer manages him [highlight]\nAlice has authority over Charlie :: RETRACT - was derived through Bob [highlight]\nBob has authority over Charlie :: KEEP - this fact is independent of Alice [success]\n```\n\nAlice loses authority over both Bob and Charlie. But Bob keeps authority over Charlie because that relationship doesn't depend on Alice's management of Bob. The retraction needs to be precise - it can't just cascade blindly down the graph.\n\nOK, that's manageable. But now consider the harder case.\n\n## The diamond problem\n\nAlice manages both Bob and Diana. Both Bob and Diana manage Charlie.\n\n```tree\nAlice [primary]\n  Bob\n    Charlie\n  Diana\n    Charlie\n```\n\nAlice has authority over Charlie through *two independent paths*: through Bob and through Diana. The derived fact `authority(Alice, Charlie)` has two reasons to exist.\n\nNow Bob stops managing Charlie:\n\n```tree\nAlice [primary]\n  Bob [muted]\n  Diana\n    Charlie [success]\n```\n\nShould Alice lose authority over Charlie? **No.** The path through Diana still supports it.\n\nNow Diana also stops managing Charlie:\n\n```tree\nAlice [primary]\n  Bob [muted]\n  Diana [muted]\nCharlie (no paths remain) [highlight]\n```\n\n*Now* Alice should lose authority over Charlie. Both supporting paths are gone.\n\nThis is the multiple derivation path problem, and it's what makes correct retraction genuinely difficult. A derived conclusion should only disappear when *every* path that supports it has been removed. Not when the first path is removed. Not when most paths are removed. Only when the count reaches zero.\n\n## How most systems get this wrong\n\nThere are three common approaches, and each fails in a different way.\n\n**Approach 1: Don't retract derived data at all.** Many systems are append-only for derived conclusions. You can mark a source fact as deleted, but the derived facts remain in whatever cache, index, or materialized view they were written to. This is the \"phantom permissions\" problem - users retain access that should have been revoked. It's also the \"ghost recommendations\" problem - discontinued products keep showing up because the derived recommendation was never cleaned up.\n\n**Approach 2: Recompute everything from scratch.** Throw away all derived data and re-derive it all. This is correct but expensive. On a knowledge graph with millions of derived facts, recomputation takes seconds or minutes. You can run it as a batch job, but between batch runs, your data is potentially inconsistent.\n\n**Approach 3: Delete derived facts that \"look related.\"** Walk from the retracted fact and delete anything downstream. This is fast, but it's wrong whenever the diamond problem appears. You'll delete conclusions that should have survived because they had alternative derivation paths.\n\n```tree\nApproaches compared [primary]\n  Append-only (no retraction)\n    Simple retraction: No [highlight]\n    Diamond problem: No [highlight]\n    Performance: N/A [muted]\n  Full recomputation\n    Simple retraction: Yes [success]\n    Diamond problem: Yes [success]\n    Performance: Slow (seconds to minutes) [highlight]\n  Naive cascade deletion\n    Simple retraction: Yes [success]\n    Diamond problem: No (deletes too much) [highlight]\n    Performance: Fast but incorrect [highlight]\n  Weighted differences (InputLayer)\n    Simple retraction: Yes [success]\n    Diamond problem: Yes [success]\n    Performance: Fast and correct [success]\n```\n\n## How InputLayer solves it: weighted differences\n\nInputLayer is built on Differential Dataflow, which represents every derived fact as a weighted record. The weight counts the number of independent derivation paths that support the conclusion.\n\nHere's the diamond example, step by step:\n\n```steps\nInitial state: Alice manages Bob and Diana, both manage Charlie :: authority(Alice, Charlie) has weight 2\nRemove \"Bob manages Charlie\": -1 via Bob path :: Weight is now 1 - conclusion SURVIVES [success]\nRemove \"Diana manages Charlie\": -1 via Diana path :: Weight is now 0 - conclusion RETRACTED [highlight]\n```\n\nThe engine doesn't need to search for alternative paths or do any special-case reasoning. The weight arithmetic handles it automatically. And this works through any number of recursive levels - if the derivation chain is 10 hops deep with branching paths at every level, the weights still track correctly.\n\n## Retraction through recursive chains\n\nThe diamond problem is hard enough with a single level of derivation. With recursion, it gets harder - but the weighted approach still handles it.\n\nConsider a deeper hierarchy:\n\n```flow\nAlice -> Bob -> Charlie -> Diana -> Eve [primary]\n```\n\nThe derived fact `authority(Alice, Eve)` goes through 4 hops. If you remove \"Charlie manages Diana,\" the engine needs to retract not just `authority(Charlie, Diana)` but also `authority(Alice, Diana)`, `authority(Bob, Diana)`, `authority(Alice, Eve)`, `authority(Bob, Eve)`, and `authority(Charlie, Eve)` - every derived authority that passed through the Charlie-Diana link.\n\nBut if Diana also reports to someone else (say, Frank, who reports to Alice through a different branch), some of those authority relationships might survive through the alternative path.\n\nThe engine tracks all of this through differences. Each removal spreads as a -1 difference through the derivation graph. At each node, the difference combines with existing weights. Conclusions retract when and only when their weight reaches zero. No manual reasoning about paths needed.\n\n## Why this matters: three real scenarios\n\n**Access control:** When someone leaves the company, every permission derived through their position needs to disappear. But only the permissions that were *exclusively* derived through their position. If a document was accessible through two independent authorization paths and one is removed, access should continue through the remaining path. Getting this wrong means either phantom permissions (security risk) or over-retraction (broken access for people who should still have it).\n\n**Recommendations:** When a product is discontinued, every recommendation that included it should vanish. If a recommendation was \"users who bought X also bought Y,\" and Y is discontinued, the recommendation disappears. But if Y was also recommended through a different signal (semantic similarity, category affinity), that recommendation should survive through the remaining signal.\n\n**Compliance:** When an entity is removed from a sanctions list, every downstream flag derived from that designation should clear. But if an entity had sanctions exposure through two different ownership paths, removing one designation should correctly preserve the remaining exposure. Your compliance team should not be chasing alerts that are no longer valid. They should also not miss alerts that are still valid because the retraction was too aggressive.\n\n## Performance\n\nCorrect retraction is only useful if it's fast enough to happen in real time. If propagating a retraction takes seconds, you're back to batch processing.\n\n| Operation | Time (2,000-node graph) |\n|---|---|\n| Retract 1 edge, propagate all downstream changes | <10ms |\n| Retract 10 edges, propagate all downstream changes | ~100ms |\n| Retract 100 edges, propagate all downstream changes | ~1 second |\n\nThese numbers come from our benchmark graph with ~400,000 derived relationships. The incremental approach means each retraction only touches the affected portion of the derivation graph. The total graph size barely matters - what matters is the size of the ripple effect from the specific retraction.\n\n## Getting started\n\nIf you want to see correct retraction in action, the [quickstart guide](/docs/guides/quickstart/) walks through a hands-on example. The [recursion documentation](/docs/guides/recursion/) explains how recursive rules interact with retraction. And our [benchmarks post](/blog/benchmarks-1587x-faster-recursive-queries/) covers the performance characteristics in detail.\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```",
    "toc": [
      {
        "level": 2,
        "text": "Simple on the surface, hard underneath",
        "id": "simple-on-the-surface-hard-underneath"
      },
      {
        "level": 2,
        "text": "The diamond problem",
        "id": "the-diamond-problem"
      },
      {
        "level": 2,
        "text": "How most systems get this wrong",
        "id": "how-most-systems-get-this-wrong"
      },
      {
        "level": 2,
        "text": "How InputLayer solves it: weighted differences",
        "id": "how-inputlayer-solves-it-weighted-differences"
      },
      {
        "level": 2,
        "text": "Retraction through recursive chains",
        "id": "retraction-through-recursive-chains"
      },
      {
        "level": 2,
        "text": "Why this matters: three real scenarios",
        "id": "why-this-matters-three-real-scenarios"
      },
      {
        "level": 2,
        "text": "Performance",
        "id": "performance"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  }
]

export const useCases: UseCase[] = [
  {
    "slug": "agentic-ai",
    "title": "Agentic AI",
    "icon": "Brain",
    "subtitle": "Give your AI agents structured memory, multi-hop reasoning, and policy-aware retrieval.",
    "content": "\n# Agentic AI\n\nIf you're building AI agents, you've probably hit the point where vector search isn't enough. Your agent can recall things that *look* relevant, but it can't actually reason about them. It can't follow a chain of facts, enforce access policies, or keep its memory consistent when things change.\n\nThat's the gap InputLayer fills. It gives your agents a structured knowledge graph with deductive reasoning, vector search, and incremental computation - all in a single system that sits alongside your existing tools.\n\n## Where current agent architectures fall short\n\nMost agent frameworks treat memory as a retrieval problem. You embed observations into vectors, store them, and retrieve by similarity when the agent needs context. This works well for simple recall - \"What did the user say about their preferences?\" - and it's the right tool for that job.\n\nBut it breaks down when your agent needs to actually think through a problem.\n\n```chain\nAgent is asked: \"Which suppliers are affected by the port closure?\"\n-- needs to know\nWhich suppliers exist and which ports they use\n-- needs to know\nWhich ports are currently closed\n-- needs to connect these facts\nNo single document contains this answer [highlight]\n=> The agent must traverse a chain of relationships to get there\n```\n\n**Multi-hop questions** like this are the most common challenge. Three separate pieces of information need to be connected. No single document or embedding contains the answer - the agent needs to follow the chain.\n\n**Policy enforcement** comes up when an agent is asked \"Show me all documents I'm allowed to see about Project X.\" It needs to resolve authorization hierarchies: who reports to whom, which teams have access, what classification levels exist. This is logical reasoning, not similarity matching.\n\n**Consistency** is the sneaky one. When an agent learns that a previous fact was wrong, all conclusions derived from that fact should disappear. With vector stores, stale observations just sit there forever unless you manually clean them up.\n\n```steps\nAgent learns: \"Supplier X ships through Shanghai port\" :: Stored as a fact\nAgent learns: \"Shanghai port is closed\" :: Stored as a fact\nEngine derives: \"Supplier X is disrupted\" :: Automatically connected [success]\nAgent learns: \"Shanghai port reopened\" :: Fact retracted\nEngine retracts: \"Supplier X is disrupted\" :: Automatically cleaned up [highlight]\n```\n\n## Agent memory as a knowledge graph\n\nInstead of storing memories as unstructured text chunks, InputLayer lets you represent them as structured facts and rules. The difference is fundamental: your agent can reason about its knowledge, not just search through it.\n\n```tree\nTraditional agent memory [muted]\n  Vector store\n    \"Customer Acme is enterprise tier\" (text chunk)\n    \"Acme contract is $150K\" (text chunk)\n    \"Acme renewal is March 2026\" (text chunk)\n```\n\n```tree\nInputLayer agent memory [primary]\n  Knowledge graph\n    customer(acme, enterprise, $150K)\n    renewal(acme, 2026-03-15)\n    Rule: high-value if enterprise + contract > $100K\n    Rule: churn-risk if high-value + renewal within 90 days\n    Derived: acme is high-value [success]\n    Derived: acme is at churn risk [highlight]\n```\n\nWith vector memory, the agent stores text chunks. It can recall them by similarity, but it can't combine them to reach conclusions.\n\nWith InputLayer, the agent stores structured facts and defines rules. The engine evaluates those rules automatically. When the agent stores a new fact - say it learns a customer's renewal is coming up - all derived conclusions update. When it retracts a fact - say the customer renews - stale conclusions vanish on their own.\n\n## Tool-use orchestration\n\nAgents that use tools generate chains of observations and actions. InputLayer tracks these chains as structured relationships, which lets the agent reason about what it's learned so far and what it should investigate next.\n\n```chain\nAgent calls search API\n-- learns\nPort closure in Shanghai [highlight]\n-- knowledge graph already knows\nSupplier A ships through Shanghai\n-- engine automatically derives\nSupplier A is disrupted [highlight]\n=> Agent didn't need to manually connect these dots [success]\n```\n\nEach new fact feeds into the existing web of relationships, potentially triggering new derived conclusions without the agent having to explicitly connect the dots. The more tool calls the agent makes, the richer the reasoning becomes.\n\n## Multi-agent coordination\n\nWhen multiple agents share a knowledge graph, they can coordinate through shared state instead of complex message-passing protocols.\n\n```flow\nAgent A: detects negative sentiment [primary] -> Shared knowledge graph -> Agent B: detects usage decline [primary]\n```\n\n```note\ntype: tip\nNeither agent had to tell the other anything. Both contributed facts. A shared rule - \"churn risk if negative sentiment AND usage drop > 30%\" - derived the conclusion from the combined knowledge. No pub/sub, no event buses, no eventual consistency headaches.\n```\n\n## Policy-aware retrieval\n\nEvery query can combine vector similarity with logical access control. This is particularly useful for agents that serve multiple users with different permission levels.\n\nWhen an agent searches for documents on behalf of a user, InputLayer evaluates the authorization rules as part of the query itself - not as a separate middleware layer. The engine resolves the user's permission chain (following reporting hierarchies, team memberships, classification levels) and combines it with semantic similarity in a single pass. Results come back already filtered for what the user is allowed to see.\n\nThis means access control is always consistent and never stale. When someone changes roles, the very next query reflects their new permissions.\n\n## Why teams add InputLayer for their agents\n\nThe short version: your vector database handles similarity search well, and you should keep using it for that. InputLayer adds the capabilities it can't provide - multi-hop reasoning, recursive access control, automatic retraction, incremental updates, and structured memory that your agents can actually think with.\n\nMost teams start by using InputLayer for the queries that require reasoning, while keeping their existing vector store for straightforward similarity search. Over time, they often find themselves running more and more of their logic through the knowledge graph as the benefits of structured reasoning become clear.\n\n## Getting started\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) will get you running in about 5 minutes. From there, the [Python SDK](/docs/guides/python-sdk/) is the fastest way to integrate with your agent framework, and the [data modeling guide](/docs/guides/core-concepts/) will help you design a knowledge graph schema that fits your use case.",
    "toc": [
      {
        "level": 2,
        "text": "Where current agent architectures fall short",
        "id": "where-current-agent-architectures-fall-short"
      },
      {
        "level": 2,
        "text": "Agent memory as a knowledge graph",
        "id": "agent-memory-as-a-knowledge-graph"
      },
      {
        "level": 2,
        "text": "Tool-use orchestration",
        "id": "tool-use-orchestration"
      },
      {
        "level": 2,
        "text": "Multi-agent coordination",
        "id": "multi-agent-coordination"
      },
      {
        "level": 2,
        "text": "Policy-aware retrieval",
        "id": "policy-aware-retrieval"
      },
      {
        "level": 2,
        "text": "Why teams add InputLayer for their agents",
        "id": "why-teams-add-inputlayer-for-their-agents"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  },
  {
    "slug": "financial-services",
    "title": "Financial Services",
    "icon": "Shield",
    "subtitle": "Sanctions screening, beneficial ownership chains, and transaction monitoring through entity reasoning.",
    "content": "\n# Financial Services\n\nFinancial compliance is fundamentally a reasoning problem. Whether you're screening transactions against sanctions lists, tracing beneficial ownership through layers of corporate structure, or monitoring for suspicious activity patterns, the answers live in the connections between entities - not in any single document or database record.\n\nInputLayer adds a reasoning layer that follows these entity chains, evaluates compliance rules, and keeps derived assessments up to date as the underlying facts change.\n\n## Sanctions screening\n\nThe basic version of sanctions screening is straightforward: check whether a transaction counterparty appears on a sanctions list. Most compliance teams have this covered. The hard part is indirect exposure - when a sanctioned person controls an entity through layers of corporate ownership.\n\n```chain\nYour client sends $50K to Alpha Corp\n-- subsidiary of\nBeta LLC\n-- 60% owned by\nGamma Holding\n-- 80% owned by\nSANCTIONED ENTITY [highlight]\n=> Each entity looks clean in isolation. The violation is only visible through the chain.\n```\n\nInputLayer handles this with recursive reasoning. You describe the rule: \"An entity has sanctions exposure if it's directly sanctioned, or if it's owned above a certain threshold by an entity that has sanctions exposure.\" The engine follows ownership chains to any depth, checking at every level.\n\n```steps\nNew sanctions designation published :: Add the fact to InputLayer\nEngine traces all ownership chains :: Finds every entity connected to the sanctioned person [primary]\nDownstream assessments update automatically :: Affected counterparties flagged in milliseconds [highlight]\n```\n\nOn the flip side, when someone is removed from a sanctions list, all the downstream flags clear automatically - no manual cleanup needed.\n\n## Beneficial ownership\n\nRegulators around the world are tightening beneficial ownership requirements. The core question: who are the natural persons that ultimately own or control this entity?\n\n```flow\nPerson X (80%) -> Holding A (60%) -> Company B [primary]\n```\n\n```note\ntype: info\nEffective beneficial ownership: 80% x 60% = 48%. If your regulatory threshold is 25%, Person X is a beneficial owner of Company B even though they don't own it directly.\n```\n\nInputLayer computes these percentages through any number of corporate layers automatically. The engine handles the multiplication and propagation. Add more layers and the math compounds:\n\n```flow\nPerson X (80%) -> Holding A (60%) -> Sub B (70%) -> Company C [primary]\n```\n\nEffective ownership: 80% x 60% x 70% = 33.6%. Still above 25%.\n\nYou define a minimum threshold, and InputLayer identifies every natural person who qualifies as a beneficial owner for every entity in your graph. When corporate structures change - new acquisitions, divestitures, ownership transfers - only the affected calculations recompute.\n\n## Transaction monitoring\n\nBeyond sanctions screening, compliance teams need to identify suspicious patterns across transaction flows. Take structuring as an example - splitting a large transaction into smaller ones to avoid reporting thresholds.\n\n```tree\nSanctioned Person [highlight]\n  Entity A\n  Entity B\n  Entity C\n```\n\n```steps\nEntity A sends $4,000 to Target Company :: Below $10K threshold - looks clean\nEntity B sends $3,500 to Target Company :: Below $10K threshold - looks clean\nEntity C sends $3,000 to Target Company :: Below $10K threshold - looks clean\nCombined total: $10,500 :: Above threshold - ALERT [highlight]\n```\n\nEach individual transaction is below $10,000. But the entities are related through common ownership, and their combined transactions exceed the threshold.\n\nInputLayer's recursive reasoning identifies these relationships automatically. It determines which entities are connected through any chain of ownership - not just direct connections, but indirect ones through any number of intermediaries. Then it aggregates transactions from all related entities within a time window.\n\nThe key insight: the \"related entity\" determination is itself a recursive traversal. Entity A owns B, B owns C, so A and C are related even though there's no direct connection. Traditional transaction monitoring systems that only check direct counterparties would miss this entirely.\n\n## Why incremental computation matters for compliance\n\nFinancial data changes constantly. New transactions arrive, entity relationships are updated, sanctions lists are revised.\n\n```flow\nBatch approach: Sanctions list updated [highlight] -> Full recomputation (minutes) -> Alerts stale until done\n```\n\n```flow\nInputLayer: Sanctions list updated [success] -> Incremental update (milliseconds) -> Alerts current immediately\n```\n\nWhen a new transaction arrives, only the affected monitoring rules recompute. When an ownership structure changes, only the beneficial ownership calculations involving the changed entities update.\n\nThe correct retraction property is equally important. When an entity is removed from a sanctions list, all the downstream flags derived from that designation clear automatically. This prevents your compliance team from chasing alerts that are no longer valid.\n\n## Getting started\n\nIf you're working on compliance or transaction monitoring, the [quickstart guide](/docs/guides/quickstart/) is the fastest way to start exploring. The [recursion documentation](/docs/guides/recursion/) is particularly relevant since most compliance rules are recursive in nature.\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```",
    "toc": [
      {
        "level": 2,
        "text": "Sanctions screening",
        "id": "sanctions-screening"
      },
      {
        "level": 2,
        "text": "Beneficial ownership",
        "id": "beneficial-ownership"
      },
      {
        "level": 2,
        "text": "Transaction monitoring",
        "id": "transaction-monitoring"
      },
      {
        "level": 2,
        "text": "Why incremental computation matters for compliance",
        "id": "why-incremental-computation-matters-for-compliance"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  },
  {
    "slug": "retail-commerce-ai",
    "title": "Retail & Commerce AI",
    "icon": "ShoppingBag",
    "subtitle": "Product recommendations, catalog reasoning, and conversational commerce powered by knowledge graphs.",
    "content": "\n# Retail & Commerce AI\n\nIf you're building AI for retail or e-commerce, you're probably familiar with the gap between what your recommendation engine suggests and what would actually be the right answer. The product that's semantically similar to what a customer bought isn't always the product they should buy next.\n\nInputLayer helps bridge that gap by adding a reasoning layer that understands product relationships, customer behavior patterns, and business rules - things that similarity search alone can't capture.\n\n## The recommendation problem\n\nTraditional recommendation engines rely on similarity - collaborative filtering (\"users who bought X bought Y\") or content-based filtering (\"products that look like X\"). Both miss the same thing: logical relationships between products.\n\n```chain\nCustomer buys a DSLR camera\n-- similarity-based system recommends\nMore cameras (semantically similar) [highlight]\n-- but what they actually need\nLens, memory card, camera bag (accessories) [success]\n=> The connection is logical, not semantic\n```\n\nInputLayer lets you express these relationships as rules. \"When a customer buys a product, recommend its accessories - but only if they haven't already bought them and they're currently in stock.\"\n\n```tree\nRecommendation signals [primary]\n  Collaborative filtering\n    \"users who bought X also bought Y\"\n  Category affinity (recursive)\n    \"related product categories, at any depth\"\n  Semantic similarity\n    \"products with similar descriptions\"\n  Accessory relationships\n    \"this product goes with that one\"\n```\n\nThe nice thing about expressing recommendations as rules is that they're auditable. When a recommendation shows up, you can trace exactly why. That kind of explainability is hard to get from black-box models.\n\n## Catalog reasoning\n\nLarge product catalogs have rich internal structure that's hard to capture in vector embeddings.\n\n```tree\nSports [primary]\n  Athletic\n    Footwear\n      Running Shoes\n      Trail Shoes\n    Accessories\n      Running Socks\n      Hydration Pack\n    Electronics\n      GPS Watch\n```\n\nA query like \"show me all products in the athletic category\" requires traversing this hierarchy. A flat metadata filter won't do that. InputLayer's recursive reasoning follows the parent-child chain and returns products from every subcategory, no matter how deep.\n\nCross-category recommendations also become natural. If customers who buy running shoes frequently also buy running socks (a different category), InputLayer can capture that as a rule and surface it as a cross-sell recommendation.\n\n## Conversational commerce\n\nChatbots and conversational agents for e-commerce need to reason about products in context. When a customer says \"I need something waterproof for hiking under $100,\" the agent needs to combine attribute filtering (waterproof, hiking-appropriate), price constraints, and inventory availability - potentially across thousands of products.\n\nInputLayer handles this by letting you express all of these constraints in a single query. The engine filters by structured attributes (waterproof, suitable for hiking), applies price constraints, checks inventory status, and can even layer in semantic similarity to capture nuances the structured attributes might miss - all in one pass.\n\nThe result is products that match both the explicit requirements and the implicit intent behind the customer's question. And because everything runs through the same reasoning engine, adding a new constraint (like \"also factor in the customer's past purchases\") is just another rule.\n\n## Keeping recommendations fresh\n\nWhen a product goes out of stock, gets discontinued, or has a price change, recommendations should reflect that immediately.\n\n```flow\nTraditional ML [highlight] -> Retrain model (hours) -> Rebuild index (minutes) -> Deploy\n```\n\n```flow\nInputLayer [success] -> Retract fact -> Recommendations update (~ms) -> Done\n```\n\nInputLayer's incremental computation handles this naturally. Update a product's stock status, and every recommendation that depended on it being in stock updates automatically. No batch job, no cache invalidation, no delay.\n\nWhen a new product arrives and you add facts about it, it immediately becomes eligible for recommendation through all existing rules. No model retraining or index rebuilding needed.\n\n## Getting started\n\nThe [quickstart guide](/docs/guides/quickstart/) will get you up and running in about 5 minutes. From there, the [data modeling guide](/docs/guides/core-concepts/) covers how to structure your product catalog as a knowledge graph, and the [Python SDK](/docs/guides/python-sdk/) makes it straightforward to integrate with your existing e-commerce platform.\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```",
    "toc": [
      {
        "level": 2,
        "text": "The recommendation problem",
        "id": "the-recommendation-problem"
      },
      {
        "level": 2,
        "text": "Catalog reasoning",
        "id": "catalog-reasoning"
      },
      {
        "level": 2,
        "text": "Conversational commerce",
        "id": "conversational-commerce"
      },
      {
        "level": 2,
        "text": "Keeping recommendations fresh",
        "id": "keeping-recommendations-fresh"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  }
]

export const comparisonPages: ComparisonPage[] = [
  {
    "slug": "vs-all-in-one-ai-data",
    "title": "InputLayer + All-in-One AI Data Platforms",
    "competitors": [
      "AI Data Platforms"
    ],
    "content": "\n# InputLayer + All-in-One AI Data Platforms\n\nA growing category of tools aims to be the single data layer for AI applications - combining vector search, filtering, and analytics in one system. These platforms offer fast vector search with rich filtering at competitive pricing.\n\nThese platforms solve a real problem: the complexity of running separate systems for different query types. But they focus primarily on retrieval (finding data that matches criteria) rather than reasoning (deriving new conclusions from existing data). InputLayer adds the reasoning capabilities that retrieval-focused platforms don't provide.\n\n## Different problems, different tools\n\nAll-in-one AI data platforms are optimized for a specific workflow: ingest vectors and metadata, query by similarity with filters, return results fast. They do this well, and they're a good fit when your queries follow this pattern.\n\nInputLayer solves a different problem: what happens when the answer isn't sitting in your data waiting to be retrieved? What if it needs to be *derived* from a chain of facts using logical rules? That's the gap InputLayer fills.\n\n| Capability | All-in-One AI Data | InputLayer |\n|---|---|---|\n| Vector similarity search | Native, optimized | Native |\n| Metadata filtering | Rich, fast | Via rules and joins |\n| Analytics (aggregation, grouping) | Growing | Via aggregation rules |\n| Rule-based inference | No | Native |\n| Recursive queries | No | Native |\n| Incremental computation | No | Native |\n| Correct retraction | No | Native |\n| Graph traversal | No | Native |\n| Knowledge graph storage | No | Native |\n\n## When retrieval isn't enough\n\nThe retrieval model works great when the information you need is explicitly stored somewhere. But many real-world questions require reasoning that goes beyond retrieval.\n\n```chain\nAI agent asked: \"Which enterprise customers are at risk of churning?\"\n-- needs to combine\nDeclining usage metrics (from analytics)\n-- with\nNegative sentiment in support tickets (from CRM)\n-- with\nUpcoming contract renewals (from billing)\n-- with\nCompetitive mentions in sales calls (from call transcripts)\n=> No single document contains \"churn risk\" - it's a derived conclusion [highlight]\n```\n\nYou tell InputLayer: \"A customer is at churn risk if they're enterprise with usage declining more than 20% and a renewal within 90 days.\" The engine evaluates this rule across all your customer data and surfaces the ones that match.\n\nNo amount of vector search will find \"churn risk\" as a stored concept. It's a conclusion derived from combining multiple facts through business rules.\n\n## The complementary pattern\n\nThe cleanest way to think about these tools is that all-in-one AI data platforms handle the retrieval layer (finding relevant data quickly) and InputLayer handles the reasoning layer (deriving conclusions from connected facts).\n\nYour AI data platform excels at queries like \"find the 50 most similar documents with metadata matching these criteria.\" InputLayer excels at queries like \"given these facts and these rules, what can be concluded, and what changes when I update a fact?\"\n\nIn practice, many teams use both. The AI data platform handles the high-throughput similarity queries where raw retrieval speed matters most. InputLayer handles the reasoning queries where the answer needs to be derived from relationships and rules. Your application routes each query to the right system based on what it needs.\n\n## Incremental reasoning as a differentiator\n\nWhat happens when your data changes? All-in-one platforms handle updates by re-indexing vectors and metadata. That works for retrieval.\n\nBut when you have derived conclusions, updates become more interesting.\n\n```steps\nCustomer usage drops below threshold :: Churn risk assessment should update immediately [highlight]\nA fact is retracted :: Everything derived from it should disappear [highlight]\nA rule changes :: All affected conclusions should recompute [highlight]\n```\n\nInputLayer handles this through incremental computation. Updates propagate through the reasoning rules, recomputing only what's affected. Retractions are correct - derived facts only disappear when all supporting derivation paths are removed. This is the kind of consistency guarantee that retrieval systems don't need to provide, but reasoning systems absolutely do.\n\n## Getting started\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) gets you running in about 5 minutes. The [data modeling guide](/docs/guides/core-concepts/) explains how to structure your knowledge graph, and the [Python SDK](/docs/guides/python-sdk/) makes integration with your existing data stack straightforward.",
    "toc": [
      {
        "level": 2,
        "text": "Different problems, different tools",
        "id": "different-problems-different-tools"
      },
      {
        "level": 2,
        "text": "When retrieval isn't enough",
        "id": "when-retrieval-isnt-enough"
      },
      {
        "level": 2,
        "text": "The complementary pattern",
        "id": "the-complementary-pattern"
      },
      {
        "level": 2,
        "text": "Incremental reasoning as a differentiator",
        "id": "incremental-reasoning-as-a-differentiator"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  },
  {
    "slug": "vs-graph-databases",
    "title": "InputLayer + Graph Databases",
    "competitors": [
      "Graph Databases"
    ],
    "content": "\n# InputLayer + Graph Databases\n\nIf you're using a graph database, you already understand the value of thinking about data as relationships. Graph databases are excellent at traversing known paths through connected data, and they've built mature ecosystems for visualization, querying, and administration.\n\nInputLayer adds capabilities that graph databases weren't designed to handle: rule-based inference, incremental computation, and correct retraction through recursive derivation chains. Think of it as the reasoning layer that sits alongside your graph database.\n\n## What each system does best\n\nGraph databases shine at traversing explicit relationships. Their query languages make it easy to express things like \"find all friends of friends\" or \"what's the shortest path between these two nodes.\" They also offer mature tooling - browser-based explorers, administration procedures, clustering capabilities.\n\nInputLayer handles the reasoning side. It evaluates logical rules, computes recursive fixed points, maintains derived conclusions incrementally, and retracts derived data correctly when source facts change. These are capabilities that graph query languages weren't designed to express.\n\n| Capability | Graph Databases | InputLayer |\n|---|---|---|\n| Property graph storage | Native | Facts and relations |\n| Path traversal | Native | Native |\n| Pattern matching | Native | Native |\n| Rule-based inference | Limited | Native |\n| Recursive fixed-point computation | Limited | Native |\n| Incremental maintenance | No | Native |\n| Correct retraction | No | Native |\n| Vector similarity search | Plugin | Native |\n| Visualization tools | Mature | Via API |\n| Clustering | Native | Single-node |\n\n## Where graph databases reach their limits\n\nGraph databases handle path queries well, but they struggle with certain patterns that come up frequently in production.\n\n**Recursive derivation** is the big one. Graph query languages support variable-length path patterns - \"find all nodes reachable through MANAGES edges.\" But this is pattern matching over the stored graph. It's different from deriving new relationships and then reasoning over the derived ones recursively.\n\n```tree\nAuthority from two sources [primary]\n  Management chain (recursive)\n    Alice manages Bob, Bob manages Charlie\n    Therefore Alice has authority over Charlie\n  Committee membership\n    Alice sits on committee overseeing Engineering\n    Therefore Alice has authority over everyone in Engineering\n```\n\nIn InputLayer, you express both sources of authority as rules, and the engine combines them into a single recursive concept. In a graph query language, you'd need to write multiple queries and stitch the results together in application code.\n\n**Incremental maintenance** is the other gap.\n\n```steps\nA relationship changes in a graph database :: Materialized views and cached results are now stale [highlight]\nManual invalidation or full recomputation needed :: You decide what to rebuild [highlight]\nA fact changes in InputLayer :: Only affected derivations recompute automatically [success]\nA fact is deleted in InputLayer :: Derived conclusions retract, but only if no alternative path exists [success]\n```\n\nThis \"correct retraction\" property is critical for maintaining consistent state in applications like access control, compliance, and recommendation systems.\n\n## How they work together\n\nThe most natural pattern is to use your graph database for interactive exploration and visualization, and InputLayer for the reasoning-heavy queries that graph query languages can't express efficiently.\n\nYour graph database handles questions like \"show me the path between these two entities\" or \"what does this part of the graph look like?\" - the kind of queries where visual exploration and interactive querying add real value.\n\nInputLayer handles questions like \"given these rules, what can be concluded?\" or \"if I change this fact, what else changes?\" - the kind of queries where you need fixed-point computation, incremental updates, and correct retraction.\n\nSome teams also use InputLayer to compute derived relationships and then sync the results back to their graph database for visualization. This gives them the reasoning power they need with the visualization and exploration tools they already know and love.\n\n## Getting started\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) will get you running. If you're coming from a graph database background, the [data modeling guide](/docs/guides/core-concepts/) explains how InputLayer's fact-and-rule model relates to property graphs, and the [recursion documentation](/docs/guides/recursion/) covers the fixed-point computation that makes InputLayer's approach to recursive reasoning different from graph traversal.",
    "toc": [
      {
        "level": 2,
        "text": "What each system does best",
        "id": "what-each-system-does-best"
      },
      {
        "level": 2,
        "text": "Where graph databases reach their limits",
        "id": "where-graph-databases-reach-their-limits"
      },
      {
        "level": 2,
        "text": "How they work together",
        "id": "how-they-work-together"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  },
  {
    "slug": "vs-vector-databases",
    "title": "InputLayer + Vector Databases",
    "competitors": [
      "Vector Databases"
    ],
    "content": "\n# InputLayer + Vector Databases\n\nIf you're already using a vector database, you've probably noticed that some questions can't be answered by similarity search alone. The moment your query needs to follow a chain of relationships, enforce access policies, or derive new conclusions from existing facts, you need something more.\n\nThat's where InputLayer comes in. It's not a replacement for your vector database - it's the reasoning layer that handles the things similarity search wasn't designed for.\n\n## What each system does best\n\nYour vector database excels at finding the k nearest neighbors to a query vector. It's purpose-built for that, and it does it really well. InputLayer adds a different set of capabilities on top: logical reasoning, recursive graph traversal, and incremental computation.\n\n| Capability | Vector DBs | InputLayer | Together |\n|---|---|---|---|\n| Vector similarity search | Native | Native | Use either or both |\n| HNSW indexes at billion-scale | Optimized | Supported | Vector DB for scale, InputLayer for reasoning |\n| Graph traversal | No | Native | InputLayer adds this |\n| Recursive queries | No | Native | InputLayer adds this |\n| Rule-based inference | No | Native | InputLayer adds this |\n| Incremental computation | No | Native | InputLayer adds this |\n| Correct retraction | No | Native | InputLayer adds this |\n| Access control in queries | Metadata filter | Recursive logic | InputLayer handles complex policies |\n\n## Your vector database is great - here's what to add\n\nKeep using your vector DB for similarity search. It's the right tool for \"find documents that look like X.\" InputLayer adds the reasoning layer for the cases where looking similar isn't enough.\n\n**Multi-hop reasoning** is the most common gap. When the answer requires following chains of relationships - not just finding similar documents - similarity search can't get there. Think about tracing supply chain dependencies or connecting a patient's medication to a food interaction through an intermediate substance. These are chains of logical connections, and InputLayer follows them automatically.\n\n**Policy-aware retrieval** matters when access control is more complex than a flat metadata filter. If your permissions involve role hierarchies, transitive authorization, or organizational structures, that's a logical problem that InputLayer handles natively. It evaluates recursive permission chains as part of the search itself, so results come back already filtered for what the user is allowed to see.\n\n**Derived conclusions** become important when facts change. If someone's role changes, their permissions need to update everywhere instantly. If a fact is retracted, everything derived from it should disappear. InputLayer's incremental computation handles this automatically, so you never have stale results.\n\n**Structured memory for AI agents** fills the gap between \"recall similar things\" and \"actually reason about what you know.\" When your agents need to follow chains of logic and maintain consistent state, that's what InputLayer adds.\n\n## How they work together architecturally\n\nYour vector database treats retrieval as a single operation - you give it a query vector, it returns the nearest neighbors. This is exactly the right model for \"find documents similar to X.\"\n\nInputLayer treats retrieval as a reasoning problem. A single query can traverse graphs, evaluate recursive rules, apply vector similarity, and enforce access policies. You reach for it when the answer isn't sitting in a single document but needs to be derived from connected facts.\n\n## Simple similarity - use your vector DB\n\n```flow\nUser query -> Embed -> Vector DB -> Top-k similar documents [success]\n```\n\nFor straightforward similarity lookups, your vector database is the right tool. No need to change anything.\n\n## Policy-filtered retrieval - add InputLayer\n\n```chain\nVP of Engineering searches for \"deployment best practices\"\n-- InputLayer resolves authorization\nWalk org hierarchy: 36 people in reporting chain [primary]\n-- filter documents\n847 documents from authorized authors\n-- rank by similarity\nTop 10 results, all authorized [success]\n=> One query, one pass. No separate auth service.\n```\n\nYour vector DB can filter on flat metadata, but it can't resolve \"does this person have transitive authority over this document's author?\" InputLayer resolves the authorization chain recursively and combines it with vector search in one query.\n\n## Multi-hop reasoning - add InputLayer\n\n```chain\nPort disruption reported\n-- which suppliers ship through this port?\nSupplier A, Supplier C [primary]\n-- which components do they provide?\nComponent X, Component Y\n-- which products use those components?\nProduct Alpha, Product Beta [highlight]\n=> Supply chain risk identified through the chain of facts\n```\n\nYour vector database finds similar documents. InputLayer follows the chain of facts and derives the conclusion.\n\n## Performance\n\nFor pure vector similarity at massive scale (billions of vectors), dedicated vector databases are purpose-built and optimized. Keep using them for that workload.\n\nInputLayer adds capabilities they don't have. When facts change, only affected derivations recompute - that's 1,652x faster than full recomputation on a 2,000-node graph. When you delete a fact, all derived conclusions update automatically. And when you need reasoning plus retrieval, everything runs in a single pass without round-trips between systems.\n\n## How teams use them together\n\nThe most common pattern is to keep your vector DB for straightforward similarity search and add InputLayer for the reasoning-heavy queries. Your vector database handles \"find similar documents.\" InputLayer handles \"follow this chain of facts, check these access permissions, and derive this conclusion.\"\n\nSome teams also take advantage of InputLayer's native vector search for queries that need to combine reasoning with similarity. Instead of making separate calls to a vector DB and then running logic in application code, they run a single query that does both. This is especially useful for policy-filtered retrieval, where you want authorization and similarity in one pass.\n\nThe key insight is that these tools solve different problems. Your vector database is great at what it does, and InputLayer fills in the capabilities it wasn't built to handle.\n\n## Getting started\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) takes about 5 minutes. The [vector search documentation](/docs/guides/vectors/) covers how InputLayer's native vector capabilities work, and the [Python SDK](/docs/guides/python-sdk/) makes integration with your existing stack straightforward.",
    "toc": [
      {
        "level": 2,
        "text": "What each system does best",
        "id": "what-each-system-does-best"
      },
      {
        "level": 2,
        "text": "Your vector database is great - here's what to add",
        "id": "your-vector-database-is-great-heres-what-to-add"
      },
      {
        "level": 2,
        "text": "How they work together architecturally",
        "id": "how-they-work-together-architecturally"
      },
      {
        "level": 2,
        "text": "Simple similarity - use your vector DB",
        "id": "simple-similarity-use-your-vector-db"
      },
      {
        "level": 2,
        "text": "Policy-filtered retrieval - add InputLayer",
        "id": "policy-filtered-retrieval-add-inputlayer"
      },
      {
        "level": 2,
        "text": "Multi-hop reasoning - add InputLayer",
        "id": "multi-hop-reasoning-add-inputlayer"
      },
      {
        "level": 2,
        "text": "Performance",
        "id": "performance"
      },
      {
        "level": 2,
        "text": "How teams use them together",
        "id": "how-teams-use-them-together"
      },
      {
        "level": 2,
        "text": "Getting started",
        "id": "getting-started"
      }
    ]
  }
]

export const customerStories: CustomerStory[] = [
  {
    "slug": "semantic-image-knowledge-graph",
    "title": "Semantic Image Knowledge Graph",
    "industry": "Media",
    "keyMetric": "Millions of images indexed",
    "content": "\n# Semantic Image Knowledge Graph\n\nA European photo company manages one of the largest stock photo libraries on the continent. With millions of images in their collection, they needed a way to go beyond simple keyword tagging and let customers discover images through the *relationships* between them - not just surface-level similarity.\n\nThey added InputLayer as the reasoning layer for their image discovery system, combining vector similarity (finding visually similar images) with structural queries (understanding what's *in* the images and how those things relate to each other).\n\n## The challenge\n\nStock photo libraries have traditionally relied on manual tagging. This approach has two well-known limitations.\n\n```steps\nTagging is expensive and inconsistent :: Different editors tag the same image differently. Volume makes thorough tagging impossible.\nKeyword search misses conceptual relationships :: A customer searching \"business meeting diversity\" won't find images unless that exact phrase was tagged. The concept exists in the image, but not in the metadata.\n```\n\nThe company had already implemented vector search using image embeddings, which helped with visual similarity. You could find images that *looked like* a reference image. But they wanted to go further - they wanted customers to search by the *concepts and relationships* within images.\n\n## The solution\n\nInputLayer was added alongside their existing image processing pipeline. The pipeline extracts structured information from images using computer vision models - detected objects, scenes, colors, compositions, people attributes. These structured outputs are stored as facts in InputLayer's knowledge graph.\n\nSo for each image, InputLayer knows things like: \"this image contains a person and a laptop and a coffee cup,\" \"the scene is an office,\" \"there's a woman in her 30s who appears to be typing.\" Each of these is a structured fact, not a free-text tag. And because they're structured, the reasoning engine can query across them in powerful ways.\n\n## Combining vector search with structural queries\n\nThe real power comes from combining these two approaches in a single query.\n\n```tree\nQuery: \"images with warm lighting showing collaborative work\" [primary]\n  Vector similarity\n    Finds images with similar visual style (warm lighting, professional) [success]\n  Structural query\n    Finds images with multiple people + shared object (whiteboard, laptop) [success]\n  Combined result\n    Images matching both visual style AND content relationships [primary]\n```\n\nA customer uploads a reference image and asks: \"find images with a similar style that also show people in an office setting.\" The vector similarity captures the visual style and composition, while the structural constraints ensure the content matches. Both conditions are evaluated in a single pass.\n\nMore sophisticated queries can traverse relationships between concepts. A customer searching for \"collaborative work\" might want images showing multiple people interacting with a shared object. This kind of query is impossible with pure vector search because it requires reasoning about the *relationships* between detected entities - not just whether certain objects are present, but how they relate to each other.\n\n## Results\n\n```steps\nVisual style + conceptual content :: Customers can search by both simultaneously [success]\nRelationship-based discovery :: Find images based on how objects and people relate in the scene [primary]\nIncremental updates :: New images immediately queryable, removed images retract cleanly [success]\nMillions of images indexed :: Combined vector and structural queries at scale [primary]\n```\n\nThe incremental computation engine keeps the knowledge graph current as new images are processed. When the vision pipeline extracts structured data from a new image, the facts are added and immediately available for queries. When an image is removed, all its associated facts retract cleanly - no orphaned metadata.\n\n## Key technical insight\n\n```note\ntype: tip\nThe key design decision was treating image understanding as a knowledge graph problem, not a pure embedding problem. Vector embeddings capture visual similarity well, but they compress away the structured information about what's in the image. By extracting that structure and storing it as facts, the company made it queryable through logical reasoning - and the combination turned out to be much more powerful than either approach alone.\n```",
    "toc": [
      {
        "level": 2,
        "text": "The challenge",
        "id": "the-challenge"
      },
      {
        "level": 2,
        "text": "The solution",
        "id": "the-solution"
      },
      {
        "level": 2,
        "text": "Combining vector search with structural queries",
        "id": "combining-vector-search-with-structural-queries"
      },
      {
        "level": 2,
        "text": "Results",
        "id": "results"
      },
      {
        "level": 2,
        "text": "Key technical insight",
        "id": "key-technical-insight"
      }
    ]
  },
  {
    "slug": "warehouse-optimization",
    "title": "Warehouse Optimization",
    "industry": "Manufacturing",
    "keyMetric": "<50ms reasoning latency",
    "content": "\n# Warehouse Optimization\n\nA European appliance manufacturer needed real-time reasoning for their warehouse operations. Their existing databases handled inventory tracking and order management well, but when it came to making fast, logic-heavy decisions - like optimal pick routing and dynamic re-prioritization - the application layer was doing too much heavy lifting.\n\nThey added InputLayer as the reasoning layer on top of their existing infrastructure, and the results were immediate: sub-50ms query latency for decisions that previously took hundreds of milliseconds to assemble from multiple data sources.\n\n## The challenge\n\nThe company operates large-scale distribution centers across Europe. Their existing systems were solid for what they were designed to do - track inventory levels, manage orders, store warehouse layouts. The problem wasn't with those systems. It was with the gap between them.\n\n```flow\nPicking robot needs route -> Inventory DB [primary] -> Order priority DB -> Layout DB -> Application code stitches it all together [highlight]\n```\n\nWhen a picking robot needed to determine the optimal route, the application layer had to pull data from inventory, cross-reference it with order priorities, factor in the physical layout, and compute a path. All of this logic lived in application code, making separate queries to different systems.\n\nThis added hundreds of milliseconds to each decision. For a warehouse running thousands of picks per hour, those milliseconds add up fast.\n\n## The solution\n\nInputLayer was added as a dedicated reasoning layer sitting alongside the existing infrastructure. Warehouse layout, inventory positions, and order priorities are ingested as facts. The routing and optimization logic that used to live in scattered application code is now expressed as reasoning rules that the engine evaluates in real time.\n\nHere's what that looks like conceptually. The warehouse layout is represented as a graph - bins connected to aisles, aisles connected to docks. Inventory tells the engine what's in each bin. Order priorities tell it what needs to be picked first. And the routing rules compute optimal paths through this graph, taking all of these factors into account simultaneously.\n\nThe key part is that the path computation is recursive - the engine explores routes through the warehouse graph, finding how to get from the dock to each bin that has items needed for high-priority orders. It evaluates this in one pass rather than requiring the application to make separate calls to different systems.\n\n## Results\n\n```steps\nRouting decisions :: Under 50ms (previously hundreds of ms) [success]\nInventory change propagation :: Only affected routes recompute [primary]\nStale route elimination :: Automatic - no robots sent to empty bins [success]\n```\n\nThe impact was felt almost immediately. Query latency dropped to under 50ms for routing decisions that previously required multiple round-trips between systems.\n\nThe incremental computation engine turned out to be especially valuable. When inventory changes (which happens constantly in a busy warehouse), only the affected routes recompute. No need to rebuild the entire routing graph every time someone picks an item. And when an item is fully picked, the correct retraction mechanism ensures that all dependent routing decisions update automatically.\n\n## Key technical insight\n\n```note\ntype: tip\nThe recursive path computation through the warehouse graph is what makes this practical. InputLayer's incremental computation means adding or removing inventory doesn't require recomputing all paths - only the affected routes update. This is what keeps latency under 50ms even as the warehouse state changes continuously.\n```",
    "toc": [
      {
        "level": 2,
        "text": "The challenge",
        "id": "the-challenge"
      },
      {
        "level": 2,
        "text": "The solution",
        "id": "the-solution"
      },
      {
        "level": 2,
        "text": "Results",
        "id": "results"
      },
      {
        "level": 2,
        "text": "Key technical insight",
        "id": "key-technical-insight"
      }
    ]
  }
]
