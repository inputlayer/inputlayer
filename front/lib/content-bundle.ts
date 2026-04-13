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
  order: number
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
    "slug": "explainable-ai-data-layer",
    "title": "What Explainable AI Actually Requires at the Data Layer",
    "date": "2026-03-27",
    "author": "",
    "category": "Governance",
    "excerpt": "A compliance audit asks 'why was transaction #8472 flagged?' You can explain the model. Can you explain which facts and rules produced the context the model used?",
    "content": "\n# What Explainable AI Actually Requires at the Data Layer\n\nA compliance audit asks: \"Why was transaction #8472 flagged?\" Your team can explain the model. It scored high based on certain features. You can show which inputs mattered most and how confident the model was. The auditor nods, then asks the next question: \"But why did the system have that information in the first place? Which entity relationships, which ownership chains, which sanctions data led it to retrieve the context it used?\" Silence. Your context retrieval pipeline returns similarity scores and distances, but can't trace the logical chain of facts and rules that determined which context was relevant.\n\nThe audit fails. The consequence isn't a note in a report. It's a formal finding, a remediation plan with a 90-day deadline, and the real possibility of restricted AI deployment until the gap is closed. If you're in a regulated industry (financial services, healthcare, defense), a failed explainability audit can mean suspended product launches, mandatory process reversions to manual review, and in severe cases, regulatory action that makes the news.\n\nYou probably focus on model explainability because that's where the tooling is. But the harder question, the one that actually determines whether you pass the audit, is this:\n\n**Why did the system have that information in the first place?**\n\n## The Two Explainability Problems\n\nThere are two distinct explainability problems in enterprise AI. You're probably solving only one of them.\n\n**Problem 1: Model explainability.** Why did the model produce this output given this input? This is where most tooling focuses. Tools that show which inputs influenced the output and why.\n\n**Problem 2: Context explainability.** Why was this context included in the prompt? Why did the system surface this fact as relevant? Why did this user have access to this document?\n\nProblem 2 is where your AI system actually fails audits. The model explanation satisfies the first round of questions. The context explanation, or lack of it, is where the finding gets written.\n\n## The context audit gap\n\nWhen your retrieval pipeline is \"embed the query, find nearest neighbors, inject top-k into the prompt,\" the audit trail is thin. \"This document was included because its embedding was 0.73 similar to the query\" doesn't satisfy a regulator.\n\nYou need to be able to reconstruct, after the fact, exactly which facts, which rules, and which state produced the context that drove the decision. And you need to prove that the access control and relevance logic applied at query time was consistent with your stated policies.\n\n## What the Data Layer Needs to Provide\n\nFor enterprise AI to be genuinely explainable, the context retrieval layer needs to provide three things:\n\n**1. Reasoning trails.** For every piece of context surfaced to the model, a complete trail showing which facts and rules produced it. Not just \"this document was retrieved\" but \"this document was retrieved because rule R fired on facts F1 and F2.\" Without this, your audit response is \"the system retrieved documents that seemed relevant.\" That satisfies no regulator.\n\n**2. Replaying past decisions exactly as they happened.** The ability to reconstruct, for any past decision, exactly what state the knowledge graph was in at the moment the context was retrieved. This requires that the data layer maintain a history of fact changes, not just current state. Without this, you can describe what the system does today but not what it did on the day the decision was made. Regulators care about the latter.\n\n**3. Policies written as auditable rules.** Access control, relevance rules, and business policies expressed as clear, versioned logic that can be inspected and audited. Not buried code that may diverge from stated policy. Without this, your stated access policy and your actual access behavior can diverge silently. The gap is invisible until an audit finds a document that was served to someone who shouldn't have seen it.\n\n## How InputLayer provides this\n\nInputLayer stores facts explicitly and expresses rules as versioned logic. Every derived conclusion traces back to the named rules and named facts that produced it. The `.why` command returns the complete reasoning chain for any result.\n\nWhen a fact is deleted, every conclusion derived from it disappears automatically. The system never serves context based on outdated data. When a fact changes, only the affected conclusions recompute.\n\nThe result: for any past decision, you can reconstruct the complete chain. This output was produced by this model, operating on this context, which was derived by these rules from these facts, which were current as of this timestamp.\n\nThat's what enterprise explainability actually requires at the data layer.",
    "toc": [
      {
        "level": 2,
        "text": "The Two Explainability Problems",
        "id": "the-two-explainability-problems"
      },
      {
        "level": 2,
        "text": "The context audit gap",
        "id": "the-context-audit-gap"
      },
      {
        "level": 2,
        "text": "What the Data Layer Needs to Provide",
        "id": "what-the-data-layer-needs-to-provide"
      },
      {
        "level": 2,
        "text": "How InputLayer provides this",
        "id": "how-inputlayer-provides-this"
      }
    ]
  },
  {
    "slug": "why-agents-use-stale-data",
    "title": "Why Your AI Agent Is Making Decisions With Yesterday's Data",
    "date": "2026-03-26",
    "author": "",
    "category": "Architecture",
    "excerpt": "A supplier was reinstated three days ago. The agent still treats them as suspended. The fix is not a faster refresh cycle.",
    "content": "\n# Why Your AI Agent Is Making Decisions With Yesterday's Data\n\nA supplier was reinstated three days ago. Your procurement agent is still routing orders to more expensive alternatives. Customers are getting delayed shipment notices. The operations team is fielding complaints they can't explain. Nobody knows the block was lifted because the calculated conclusion, \"this supplier is suspended,\" hasn't caught up with the fact that changed on Tuesday.\n\nThe model is fine. The retrieval pipeline is working. The problem is quieter and harder to catch: the conclusions your agent acts on (\"this order is blocked,\" \"this line is unavailable,\" \"this supplier can't fulfill\") are stale. They were true when they were derived. They're not true anymore. And nothing in the system knows the difference.\n\n## How Most Agent Architectures Handle Context\n\nThe standard pattern: at query time, retrieve relevant context from a vector database or data warehouse, inject it into the prompt, let the model reason from there.\n\nThis works when your data changes slowly. It breaks when your data changes faster than your retrieval pipeline refreshes.\n\nThe fundamental issue is that full recomputation doesn't scale. A 2,000-node entity graph takes 11.3 seconds to follow every chain of connections from scratch. Run that hourly, and you're spending 271 seconds per day on recomputation. That's for one graph, one query type. And even then, between refreshes, you can't know whether the context you retrieved is current or one cycle stale.\n\n## The Incremental Alternative\n\nIncremental computation flips this around: only recompute what changed.\n\nWhen a supplier status changes from suspended to active, the only conclusions that need updating are the ones that depended on that supplier's status. Not the entire graph.\n\nThis is what incremental computation engines do. They maintain a dependency graph between facts and conclusions. When a fact changes, they spread the update through only the affected paths.\n\nInputLayer is built on Differential Dataflow, a computation engine designed to process changes efficiently rather than re-running everything. The same 2,000-node query that takes 11.3 seconds to follow every chain of connections from scratch takes 6.83ms when a single connection is added, because only the paths affected by that connection are re-evaluated.\n\n## What This Means by Domain\n\n**Manufacturing:** When an equipment hold is lifted, every production plan blocked by that hold updates automatically. Your planning agent sees current reality, not Monday's snapshot.\n\n**Supply chain:** When a supplier comes off a sanctions watch list, the orders blocked by that flag are immediately unblocked. No manual reconciliation, no stale flags in the next batch job.\n\n**Financial risk:** When a beneficial ownership relationship changes, the affected transaction flags update in real time. You see the current ownership graph, not last week's.\n\n## The Implementation Pattern\n\nYour data pipeline writes facts to InputLayer as they change:\n\n```iql\n+supplier(\"sup_02\", \"status\", \"active\")  // previously suspended\n```\n\nInputLayer spreads the update. Conclusions that depended on `sup_02` being suspended are removed. New conclusions based on the active status are computed. Your agent queries against current state, not a snapshot.\n\nThe key property is smart cleanup: when a fact is deleted or updated, every conclusion that was built on top of it disappears automatically. Nothing stale lingers. Differential Dataflow handles this natively.\n\n## The bottom line\n\nWhen a supplier status changes at 3pm, the next query at 3:01pm should reflect that change. Not the next morning after the batch job runs.\n\nInputLayer maintains its conclusions incrementally. When a fact changes, only the affected conclusions update, in milliseconds, not hours. And every decision traces back to the specific facts and rules in effect at the time it was made.",
    "toc": [
      {
        "level": 2,
        "text": "How Most Agent Architectures Handle Context",
        "id": "how-most-agent-architectures-handle-context"
      },
      {
        "level": 2,
        "text": "The Incremental Alternative",
        "id": "the-incremental-alternative"
      },
      {
        "level": 2,
        "text": "What This Means by Domain",
        "id": "what-this-means-by-domain"
      },
      {
        "level": 2,
        "text": "The Implementation Pattern",
        "id": "the-implementation-pattern"
      },
      {
        "level": 2,
        "text": "The bottom line",
        "id": "the-bottom-line"
      }
    ]
  },
  {
    "slug": "why-vector-search-alone-fails",
    "title": "Why Search Alone Fails Your AI Agent",
    "date": "2026-02-25",
    "author": "",
    "category": "Architecture",
    "excerpt": "When the answer lives across three separate facts in three separate systems, similarity search can't get there. Here's what does.",
    "content": "\n# Why Search Alone Fails Your AI Agent\n\nA patient asks your healthcare AI agent: \"Can I eat shrimp tonight?\" The system returns shrimp recipes, nutritional info, some allergy FAQs. Helpful content, high similarity scores, completely confident. Also potentially dangerous, because this patient takes Amiodarone, a cardiac medication that interacts with iodine, and shrimp is high in iodine. The correct answer could keep them out of the hospital, and it's nowhere in the search results.\n\nThe answer lives across three facts in three separate systems: the patient's medication list, a drug interaction database, and a nutritional profile. No single document contains the connection. The phrase \"shrimp dinner\" looks nothing like \"medication contraindication\" to a search engine. This isn't a relevance problem. The system returned exactly what looked most related. It's a reasoning problem, and search doesn't reason.\n\n## What went wrong\n\nThe correct answer is \"No, and it could be dangerous.\" But that answer doesn't live in any single document. It lives across three separate pieces of information:\n\n```graph\nSarah --takes-> Amiodarone --interacts-> Iodine --found in-> Shrimp [highlight]\n```\n\nThe patient takes a specific medication. That medication interacts with iodine. Shrimp is high in iodine. Each fact sits in a different database. And the phrase \"shrimp dinner\" has zero similarity to \"medication contraindications.\" They share no words, no concepts, nothing a search engine would connect.\n\n**The connection between these facts is logical, not textual.** You can't find it by looking for similar words. You have to follow a chain of relationships from one fact to the next. That's what InputLayer does.\n\n## This shows up everywhere\n\nThe healthcare example is vivid, but this same pattern appears in every domain where answers require connecting multiple facts.\n\n```steps\n\"Is this transaction suspicious?\" :: Trace ownership to sanctions list\n\"Can I see Q3 revenue reports?\" :: Resolve access via org hierarchy\n\"Which products are disrupted?\" :: Connect port closures to suppliers to products\n```\n\nIn every case, the answer requires **following a chain of connected facts**. The information exists, but it's spread across different sources, and the connections between them are structural, not textual.\n\n## What you end up building without a reasoning layer\n\nTo work around this, you start adding systems. A graph database for relationships. A rules engine for business logic. An authorization service for access control. Application code to stitch it all together.\n\n```flow\nQuestion -> Vector DB -> Graph DB -> Rules -> Auth -> App code [primary]\n```\n\nEach boundary between systems is a place where things can fall out of sync. A permission revoked in one system but not yet propagated to another, a fact updated in the source but stale in the cache. The reasoning logic ends up scattered across services. When a fact changes, you have to propagate the change across all of them. It works, but it's fragile, and each new capability makes it more fragile.\n\n## What it looks like with InputLayer\n\nInputLayer handles the chains of logic and derived conclusions in one place.\n\n```flow\nQuestion -> Vector DB -> Similar documents\n```\n\n```flow\nQuestion -> InputLayer [primary] -> Derived conclusions\n```\n\nYour vector database finds similar content. InputLayer follows chains of facts and derives conclusions.\n\nWhen the patient's medication list changes, InputLayer automatically updates every downstream risk assessment. When an employee changes departments, their permissions recalculate through the org hierarchy. When a corporate ownership structure shifts, the compliance analysis adjusts. All of this happens incrementally. Only the affected conclusions recompute, not the entire knowledge base.\n\n## Getting started\n\nInputLayer is open-source and runs in a single Docker container:\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) walks you through building your first knowledge graph in about 10 minutes.",
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
        "text": "What you end up building without a reasoning layer",
        "id": "what-you-end-up-building-without-a-reasoning-layer"
      },
      {
        "level": 2,
        "text": "What it looks like with InputLayer",
        "id": "what-it-looks-like-with-inputlayer"
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
    "author": "",
    "category": "Tutorial",
    "excerpt": "A hands-on tutorial to get InputLayer running and build your first knowledge graph with rules, recursive queries, and vector search.",
    "content": "\n# InputLayer in 10 Minutes: From Docker to Your First Knowledge Graph\n\nSomeone changes a role in your org chart. Now you need to figure out which permissions changed, which documents need re-filtering, and which downstream reports are stale. In custom-built permission systems, that's a ticket, a script, and a prayer that you caught everything.\n\nIn 10 minutes, you'll build a knowledge graph that derives conclusions automatically, updates them in real time when facts change, and explains every result with a full reasoning chain. Not a toy demo. These are the same mechanics that handle access control hierarchies, compliance chains, and recommendation graphs in production. Let's build it.\n\n## Step 1: Start InputLayer\n\nRun this in your terminal:\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nYou'll see InputLayer start up and print a message telling you it's ready. That's it. No config files, no setup.\n\n## Step 2: Open the REPL\n\nNow you need a way to talk to InputLayer. Open your browser and go to:\n\n```\nhttp://localhost:8080\n```\n\nThis opens InputLayer's interactive REPL, a command line where you can type queries and see results immediately. Think of it like a SQL console, but for knowledge graphs.\n\nYou can also use the [Python SDK](/docs/guides/python-sdk/) or the [REST API](/docs/guides/configuration/), but the REPL is the fastest way to explore.\n\n## Step 3: Store some facts\n\nLet's model a small organization. In the REPL, you'll store three facts about who manages whom:\n\n```\nAlice manages Bob\nBob manages Charlie\nBob manages Diana\n```\n\nHere's what that looks like as a structure:\n\n```tree\nAlice [primary]\n  Bob\n    Charlie\n    Diana\n```\n\nThat's the entire org chart. Three facts, four people. InputLayer stores these immediately. No schema to define, no tables to create.\n\nYou can already ask simple questions: \"Who does Bob manage?\" returns Charlie and Diana. \"Who manages Bob?\" returns Alice. These are direct lookups, nothing special yet.\n\n## Step 4: Define a rule\n\nThis is the step where InputLayer becomes fundamentally different from a regular database.\n\nWe want to answer: *\"Who does Alice have authority over?\"* Alice manages Bob directly. But does she have authority over Charlie? She doesn't manage Charlie. Bob does. But intuitively, yes, because she manages the person who manages Charlie.\n\nYou express this intuition as a rule:\n\n```note\ntype: tip\nIf A manages B, then A has authority over B.\nIf A has authority over B, and B has authority over C, then A has authority over C too.\n```\n\nThat second sentence is the important part. It's recursive. It says: authority flows down through the management chain, no matter how deep it goes.\n\nWhen you enter this rule, InputLayer immediately starts reasoning. It applies the rule over and over until there are no more conclusions to draw. Here's what it figures out:\n\n```steps\nAlice -> Bob :: Direct [primary]\nBob -> Charlie, Diana :: Direct [primary]\nAlice -> Charlie :: Derived through Bob [success]\nAlice -> Diana :: Derived through Bob [success]\n```\n\nFive authority relationships, derived automatically from three facts and one rule.\n\n## Step 5: Ask a question\n\nNow query: *\"Who does Alice have authority over?\"*\n\nThe answer: **Bob, Charlie, and Diana.**\n\nAlice doesn't manage Charlie or Diana directly. But the engine followed the chain and derived it automatically. SQL can compute this too, but InputLayer maintains the result in real time. When facts change, conclusions update in milliseconds without re-running the query.\n\n## Step 6: Add vector search\n\nInputLayer supports vector embeddings alongside logical reasoning. You can combine both in a single query.\n\nSay each person has authored some documents, and each document has an embedding vector. You can now ask something that would normally require multiple systems:\n\n*\"Find documents similar to my query, but only from people that Alice has authority over.\"*\n\n```steps\nResolve authority :: Bob, Charlie, Diana\nFilter documents :: By those authors\nRank by similarity :: Most relevant first [success]\n```\n\nReasoning and retrieval, combined in one pass. No separate authorization service, no glue code.\n\n## Step 7: See incremental updates\n\nAdd a new fact: Diana now manages a new employee, Frank.\n\n```tree\nAlice [primary]\n  Bob\n    Charlie\n    Diana\n      Frank [success]\n```\n\nQuery authority again. Frank shows up in Alice's results immediately, even though you never told the system about Alice's relationship to Frank. The engine derived it: Alice has authority over Diana, Diana manages Frank, therefore Alice has authority over Frank.\n\nThe important part: InputLayer didn't recompute everything from scratch. It identified that the new fact only affects a small part of the graph and updated just that. On a 2,000-node graph, this is over **1,600x faster** than recomputing everything.\n\n## Step 8: See correct retraction\n\nRemove the fact that Bob manages Diana.\n\n```tree\nAlice [primary]\n  Bob\n    Charlie\n```\n\nQuery authority again. Diana and Frank are gone from Alice's results. But Bob still has authority over Charlie. That relationship doesn't depend on Diana at all.\n\nHere's the subtle part: what if Diana had reported to Alice through *two* paths? Say both Bob and Eve managed Diana. Removing Bob's management of Diana shouldn't remove Alice's authority over Diana if the Eve path still exists. InputLayer tracks this automatically. A conclusion only disappears when *all* paths supporting it are gone.\n\n## What you just built\n\nIn about 10 minutes, you've used:\n\n| Capability | What happened |\n|---|---|\n| Knowledge graph | Stored facts about people and relationships |\n| Recursive reasoning | A rule derived authority chains automatically |\n| Vector search | Combined similarity with logical reasoning |\n| Incremental updates | New facts spread in milliseconds |\n| Correct retraction | Removed facts cleaned up precisely |\n\nThese capabilities often live in separate systems: graph traversal in one, vector search in another, business rules in application code. InputLayer handles them in one place.\n\n## Next steps\n\nThe [data modeling guide](/docs/guides/core-concepts/) covers how to design your knowledge graph schema. The [vectors guide](/docs/guides/vectors/) dives deeper into similarity search and HNSW indexes. And the [Python SDK](/docs/guides/python-sdk/) is the fastest way to integrate InputLayer into your applications.",
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
    "title": "Benchmarks: 1,652x Faster Recursive Queries with Incremental Computation",
    "date": "2026-02-15",
    "author": "",
    "category": "Engineering",
    "excerpt": "One fact changes in a graph with 400,000 derived relationships. Full recompute: 11.3 seconds. InputLayer: 6.83 milliseconds. Here's how.",
    "content": "\n# Benchmarks: 1,652x Faster Recursive Queries with Incremental Computation\n\nIf your compliance system takes 11 seconds to update after an ownership change, that's 11 seconds where a transaction can slip through against a sanctioned entity. If your recommendation engine takes 11 seconds to reflect a stockout, that's 11 seconds of customers clicking \"buy\" on products you can't ship. If your access control takes 11 seconds to propagate a role change, that's 11 seconds where a terminated employee's permissions are still active. These aren't hypotheticals. They're the cost of recalculating everything from scratch in systems that need to reason over connected data.\n\nOne fact changes in a knowledge graph with 400,000 derived relationships. Full recompute: 11.3 seconds. InputLayer: 6.83 milliseconds.\n\nThat's a **1,652x** difference. Not a faster version of the same thing, but a fundamentally different class of system. One that turns batch-only workloads into real-time operations.\n\n## The benchmark setup\n\nWe wanted to test something that reflects real-world usage, not a synthetic micro-benchmark. So we picked a common pattern: following chains of authority in an organizational graph. Think \"Alice manages Bob, Bob manages Charlie, so Alice has indirect authority over Charlie.\" This is the same kind of computation you'd need for access control chains, supply chain risk propagation, or entity resolution across corporate structures.\n\n```flow\n2,000 nodes -> 6,000 edges -> 400,000 derived relationships\n```\n\n```note\ntype: info\nThe test: add one new edge, then measure how long it takes to update all derived relationships.\n```\n\nThe 400,000 derived relationships come from following chains of authority. If A manages B and B manages C, then A has indirect authority over C. Follow that logic through 2,000 nodes with an average depth of 8-10 levels, and the number of derived relationships grows fast.\n\n## The results\n\n| Approach | Time | What it does |\n|---|---|---|\n| Recalculate everything | 11,280 ms | Throws away all 400,000 derived relationships, re-derives them all |\n| InputLayer (incremental) | 6.83 ms | Identifies affected relationships, updates only those |\n\nRecalculating everything doesn't care that you only changed one edge. It treats the entire graph as dirty and rebuilds everything. InputLayer's engine, on the other hand, traces the impact of the change through the chain of reasoning and touches only what's affected.\n\nTo put 6.83ms in perspective: that's fast enough to run inline with an API request. You can check permissions, compute supply chain exposure, or resolve entity relationships at query time rather than pre-computing them in a batch process.\n\n## What this means in practice\n\nThe practical takeaway here is about which architectural patterns become possible.\n\n**Without incremental computation**, you're stuck with batch processing. Pre-compute permissions overnight. Rebuild recommendation indexes hourly. Re-run compliance checks on a schedule. And accept that between runs, your derived data is stale.\n\n**With incremental computation**, you can do these things live:\n\n| Use case | Batch approach | Incremental approach |\n|---|---|---|\n| Access control | Nightly permission rebuild | Live permission check at query time |\n| Supply chain risk | Hourly risk recalculation | Instant risk update when a supplier status changes |\n| Compliance screening | Daily sanctions check | Real-time flag when ownership structure changes |\n| Recommendations | Model retrain every few hours | Instant update when user behavior or inventory changes |\n\nThe 1,652x speedup isn't about making a slow thing faster. It's about making batch-only workloads work in real time. That's a qualitative difference in what you can build.\n\n## The scaling story\n\nHere's where it gets really interesting. The incremental advantage doesn't stay constant as your graph grows. It gets *dramatically* better.\n\n| Graph size | Derived relationships | Recalculate everything | Incremental update | Speedup |\n|---|---|---|---|---|\n| 500 nodes | ~25,000 | 420 ms | 1.2 ms | **350x** |\n| 1,000 nodes | ~100,000 | 2,800 ms | 3.1 ms | **903x** |\n| 2,000 nodes | ~400,000 | 11,280 ms | 6.83 ms | **1,652x** |\n\nLook at how the two columns grow. Recalculating everything grows much faster than linearly. Double the nodes, quadruple the time. But incremental updates grow much slower, because most single-fact changes only ripple through a small portion of the graph.\n\n```steps\n500 nodes: 420ms vs 1.2ms :: 350x [success]\n1,000 nodes: 2.8s vs 3.1ms :: 903x [success]\n2,000 nodes: 11.3s vs 6.8ms :: 1,652x [primary]\n```\n\nThis scaling behavior is fundamental, not accidental. Recalculating everything has to process the entire graph regardless of what changed. Incremental updates process only the ripple effect of the change, which stays relatively small even as the total graph grows.\n\nAt 10,000 nodes, recalculating everything would take over a minute. The incremental update would still be in the low tens of milliseconds. That's the difference between a feature that's practical in production and one that isn't.\n\n## Why the numbers work this way\n\nInputLayer is built on [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow), a Rust library for incremental computation created by Frank McSherry. The core idea is simple: instead of storing derived results as static data, the engine tracks *what changed* and efficiently passes those changes along.\n\nHere's how a fact change flows through the system:\n\n```graph\nAlice --authority-> Diana --manages-> Eve [primary]\nBob --authority-> Diana --manages-> Eve [primary]\nEve --manages-> Frank [success]\n```\n\nThe engine didn't scan the entire graph. It didn't recompute relationships for nodes that weren't affected. It started from the change, followed the ripple effects, and stopped as soon as the ripple died out.\n\nFor recursive reasoning, like indirect authority where conclusions feed back into the computation, the engine runs a loop until it reaches a stable point where no new changes are produced. When something changes later, it re-enters that loop at the point of change and computes only the new changes.\n\nInputLayer also uses an optimization that makes queries on-demand rather than exhaustive. When you ask \"who does Alice have authority over?\", the engine doesn't compute authority for every person in the organization. It starts from Alice and follows only the relevant paths. Query time becomes proportional to Alice's portion of the graph, not the entire organization.\n\n## Correct retraction: the hard part\n\nAdding facts is relatively straightforward to handle incrementally. Removing them is where things get genuinely hard.\n\nSay Alice has authority over Charlie through two independent paths:\n\n```graph\nAlice --manages-> Bob --manages-> Charlie [primary]\nAlice --manages-> Diana --manages-> Charlie [primary]\n```\n\nIf you remove Bob's management of Charlie, Alice should still have authority over Charlie through Diana. But if you remove Diana's management of Charlie too, the authority should disappear entirely.\n\nThe engine tracks this through weighted differences. Each derived relationship has a weight based on the number of independent paths that support it. When a path is removed, the weight goes down. Only when it reaches zero does the conclusion go away.\n\n```steps\nBoth paths: weight = 2 :: via Bob + Diana\nRemove Bob path: weight = 1 :: survives [success]\nRemove Diana path: weight = 0 :: retracted [highlight]\n```\n\nOn our benchmark graph, retracting a single edge and propagating all downstream changes takes under 10ms. Bulk retractions (removing 100 edges) complete in about a second. Fast enough for real-time applications where facts change frequently.\n\n## Try it yourself\n\nInputLayer is open-source:\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nStart with the [quickstart guide](/docs/guides/quickstart/) to build your first knowledge graph, or dive into the [recursion documentation](/docs/guides/recursion/) to see how recursive reasoning works under the hood.",
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
        "text": "What this means in practice",
        "id": "what-this-means-in-practice"
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
        "text": "Try it yourself",
        "id": "try-it-yourself"
      }
    ]
  },
  {
    "slug": "building-product-recommendation-engine",
    "title": "Building a Product Recommendation Engine with InputLayer",
    "date": "2026-02-05",
    "author": "",
    "category": "Tutorial",
    "excerpt": "A step-by-step guide to building a recommendation engine that combines collaborative filtering, product relationships, and similarity by meaning - all in a single knowledge graph.",
    "content": "\n# Building a Product Recommendation Engine with InputLayer\n\nA customer buys a Canon EOS R5 camera. Your recommendation engine suggests a Nikon lens, a Sony memory card holder, and a generic phone case. The customer clicks through, orders the Nikon lens, discovers it doesn't fit, returns it, and writes a one-star review about how your site \"doesn't even know what goes with what.\" Meanwhile, the Canon-compatible lens was in stock the whole time. Your model just couldn't see the logical relationship between a camera and its compatible accessories, because those items look nothing alike when you compare their descriptions numerically.\n\nThis is the gap between similarity by description and actual product logic. A camera and a camera bag aren't similar. One is an electronic device and the other is a fabric container. But one is an accessory for the other, and your customers expect you to know that. The same gap shows up when inventory changes faster than your model retrains: recommending out-of-stock items, suggesting products in categories the customer has never shown interest in, or missing the obvious cross-sell because the connection is structural, not textual.\n\nIn this tutorial, we'll build a recommendation engine that combines four signals: collaborative filtering, category affinity, similarity by meaning, and explicit accessory relationships. Each signal is a readable rule. The engine combines them automatically and keeps recommendations fresh as inventory and purchase history change. No model retraining, no index rebuilding.\n\n## What we're building\n\nBy the end of this tutorial, you'll have a recommendation engine with four distinct signals:\n\n```tree\nRecommendation Engine [primary]\n  Collaborative Filtering\n  Category Affinity\n  Similarity by Meaning\n  Accessory Relationships\n```\n\n```note\ntype: info\nResults are combined, de-duplicated, and filtered: already purchased items are excluded, out-of-stock items are excluded, and discontinued items are removed automatically.\n```\n\nEach signal is expressed as a simple, readable rule. The engine combines them automatically. And because this runs on a knowledge graph with incremental computation, the recommendations stay fresh without model retraining or index rebuilding.\n\n## Step 1: Model your product catalog\n\nEverything starts with your product data. In InputLayer, this means storing structured facts about products, their categories, and how categories relate to each other.\n\nYou store each product with its name and direct category. Then you describe the category hierarchy. Running shoes fall under athletic footwear, which falls under footwear, which falls under apparel. This hierarchy is the backbone for one of our recommendation signals.\n\nYou also store numerical descriptions for each product, generated by converting product text into numbers the engine can compare. These power the similarity-by-meaning signal.\n\n```tree\nSports [primary]\n  Athletic\n    Footwear: Running Shoes, Trail Shoes\n    Accessories: Socks, Hydration Pack\n    Electronics: GPS Watch\n```\n\n## Step 2: Feed in user behavior\n\nNext, purchase history and browsing data. Who bought what, and what have they been looking at recently. In production, you'd ingest this from your transaction database as events happen.\n\n```tree\nPurchases\n  user_1: Running Shoes, Socks\n  user_2: Running Shoes, Hydration Pack\n  user_3: Trail Shoes, GPS Watch\nBrowsing\n  user_1: Trail Shoes, GPS Watch [muted]\n```\n\nThe important thing: these aren't just rows in a table. They're facts in a knowledge graph that the reasoning engine can combine with other facts through rules. That's the key difference from a static lookup table.\n\n## Step 3: Define recommendation rules\n\nThis is where the approach diverges from traditional ML recommendations. Instead of training a model, we express recommendation logic as rules. Each rule captures a different signal, and each rule is readable in plain English.\n\n**Rule 1 - Collaborative filtering:** \"If two users bought the same product, the other products each user bought become recommendations for the other.\" This is the classic \"customers who bought X also bought Y\" pattern. But it's expressed as a rule, not a statistical model, which means you can read it, debug it, and explain exactly why a recommendation appeared. When a customer asks \"why was I shown this?\", the answer traces back to specific purchases by specific users, not an opaque number the model learned.\n\nWhat this looks like in practice for user_1:\n\n```graph\nuser_1 --bought-> Running Shoes\nuser_2 --bought-> Running Shoes [primary]\nuser_2 --bought-> Hydration Pack [success]\n```\n\n**Rule 2 - Category affinity (recursive):** \"If a user bought something in one category, recommend products from related categories.\" This rule is recursive. It follows the category hierarchy to find related categories at any depth.\n\n```graph\nRunning Shoes --in-> Footwear --under-> Athletic [primary]\nAthletic --contains-> Accessories [success]\nAthletic --contains-> Electronics [success]\n```\n\nBuying running shoes surfaces recommendations not just from footwear, but from accessories and electronics too, because they share a parent category. And this works no matter how deep or wide your category tree goes.\n\n**Rule 3 - Similarity by meaning:** Products whose descriptions mean similar things become recommendations. This catches relationships that the category hierarchy misses. Two products from completely different categories that people tend to use together.\n\n**Rule 4 - Accessory relationships:** \"When a customer buys a product, recommend its accessories, but only if they haven't already bought them and they're in stock.\" This is the explicit knowledge that a camera bag goes with a camera, expressed directly rather than inferred statistically.\n\n## Step 4: Combine and query\n\nNow you ask: \"What should we recommend to user_1?\"\n\nThe engine evaluates all four rules, combines their results, filters out products user_1 has already bought, checks stock availability, and returns the final list:\n\n```tree\nSignals for user_1 [primary]\n  Collaborative: Hydration Pack\n  Category: Hydration Pack, GPS Watch, Trail Shoes\n  Similar meaning: Trail Shoes (0.92)\n  Accessory: none [muted]\n```\n\n```steps\nTrail Shoes: category + similar meaning :: strongest signal [primary]\nHydration Pack: collaborative + category :: two signals [primary]\nGPS Watch: category only :: single signal\n```\n\nEach recommendation carries its reasoning trail. You can explain to the user *why* each item was recommended, and you can explain to your product team which signals are driving the most engagement. The explanations are deterministic rule chains, not after-the-fact guesses bolted on top of a black box.\n\n## Step 5: Watch it stay fresh\n\nHere's where the knowledge graph approach really shines compared to model-based recommenders.\n\n**A new purchase comes in.** User_1 buys a GPS Watch. You add that fact. All recommendations update instantly. GPS Watch drops out of user_1's recommendations (already purchased), and any collaborative filtering signals that involve GPS Watch recalculate. No model retraining needed.\n\n**A product goes out of stock.** You update the stock status for Trail Shoes. Every recommendation that included Trail Shoes disappears from results automatically. When it's back in stock, the recommendations come back. No index rebuild needed.\n\n**A product is discontinued.** You retract it from the catalog entirely. InputLayer's correct retraction mechanism removes it from every recommendation result, every collaborative filtering signal, every category association, automatically and immediately. No stale suggestions pointing customers to a product page that returns a 404.\n\n```flow\nBatch ML recommender [highlight] -> Retrain (hours) -> Rebuild index -> Deploy\n```\n\n```flow\nInputLayer [success] -> Retract fact -> Updated (ms)\n```\n\n## Where to take this next\n\nWhat we've built is the foundation. Here are the layers you'd add for production:\n\n**Inventory-aware filtering.** Only recommend products that are actually in stock and available in the customer's region. This is one more condition on the recommendation rule.\n\n**Time decay.** Weight recent purchases more heavily than old ones. A customer who bought running shoes yesterday is more likely to need accessories than a customer who bought them two years ago.\n\n**Price affinity.** Recommend products in the customer's typical price range. If they buy premium products, don't recommend budget options.\n\n**Seasonal rules.** Boost winter gear in November, swimwear in May. Express seasonality as a rule rather than baking it into a training set.\n\nEach of these is just another rule in the knowledge graph. The engine handles the interactions between all rules automatically. You don't need to worry about how time decay interacts with category affinity, or how inventory filtering affects collaborative signals. Define the rules, and the engine composes them.\n\nCheck out the [data modeling guide](/docs/guides/core-concepts/) for patterns that work well at scale, and the [Python SDK](/docs/guides/python-sdk/) for integrating this into your e-commerce platform.",
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
    "author": "",
    "category": "Engineering",
    "excerpt": "One fact changes. Two million derived conclusions exist. How many need to update? Usually a few hundred. Differential Dataflow finds exactly those, in milliseconds.",
    "content": "\n# Why We Built InputLayer on Differential Dataflow\n\nWe needed an engine where deleting one fact from a graph with two million derived conclusions would correctly retract exactly the right subset in milliseconds, not minutes. Getting this wrong means phantom permissions that should have been revoked, stale compliance flags that miss a sanctions hit, or recommendations pointing to discontinued products. These aren't edge cases. They're the normal state of any system where facts change and derived conclusions don't keep up.\n\nThe choice of computation engine determines whether these bugs are even possible. We chose Differential Dataflow because it makes an entire category of consistency failures structurally impossible. Not caught by tests, not handled by cleanup jobs, but eliminated at the engine level. Here's the story behind that choice.\n\n## The problem that started everything\n\nWe wanted to build a knowledge graph engine that could do something deceptively simple: keep derived conclusions up to date when facts change.\n\nThat sounds straightforward until you think about scale. Imagine a knowledge graph with 100,000 facts and 50 rules that derive new conclusions from those facts. Some of those rules are recursive, meaning their output feeds back into their input. The initial computation produces millions of derived facts. Fine, that's a one-time cost.\n\nBut then a single fact changes. One employee transfers departments. One entity gets added to a sanctions list. One product goes out of stock.\n\n```flow\n100K facts + 50 rules -> 2M derived facts [primary] -> 1 fact changes -> ~100 affected [highlight]\n```\n\nWith a naive approach, you throw away all 2 million derived facts and recompute them from scratch. For small graphs, that's fast enough. For production workloads, it doesn't work. At 11 seconds per recomputation on a 2,000-node graph, you're locked into batch processing. Real-time permission checks, live compliance screening, instant recommendation updates. None of that is practical.\n\nWe needed an engine that could update just the affected derivations, correctly, in milliseconds.\n\n## Finding Differential Dataflow\n\nWe found it in Frank McSherry's work on [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow), built on top of [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow). Both are Rust libraries. The performance was a bonus. The computational model was the real discovery.\n\nThe core idea is simple enough to explain in a paragraph: instead of storing derived data as static results, the engine tracks *changes*. Adding a fact is a +1. Removing a fact is a -1. Every computation in the system takes changes in and produces changes out. This means every operation is naturally incremental. It never looks at the whole dataset, only at what changed.\n\n```flow\nFull recompute: Changed fact -> Recompute ALL [highlight] -> Rebuilt results\n```\n\n```flow\nDifferential: Changed fact -> Compute diff only [success] -> Updated derivations\n```\n\n## How it handles the hard part: recursive retraction\n\nThe real test of an incremental system isn't additions. It's deletions. And specifically, deletions through recursive chains of reasoning.\n\nHere's the scenario that breaks naive incremental systems. Alice has authority over Charlie through two independent paths:\n\n```graph\nAlice --manages-> Bob --manages-> Charlie [primary]\nAlice --manages-> Diana --manages-> Charlie [primary]\n```\n\nRemove Bob's management of Charlie. Does Alice lose authority over Charlie? *No*, the path through Diana still supports it. Now remove Diana's management of Charlie too. Does Alice lose authority over Charlie? *Yes*, there are no remaining paths.\n\nDifferential Dataflow handles this through its weight-based model. Each derived fact carries a weight representing the number of independent reasoning paths that support it. Removing a path decreases the weight. The fact only retracts when the weight hits zero.\n\n```steps\nBoth paths: weight = 2 :: via Bob + Diana\nRemove Bob path: weight = 1 :: survives [success]\nRemove Diana path: weight = 0 :: retracted [highlight]\n```\n\nThis sounds simple in theory. In practice, getting it right through multiple levels of recursive reasoning, where intermediate conclusions can also have multiple support paths, is extraordinarily difficult. Differential Dataflow solves it at the engine level, which means we didn't have to.\n\n## What this gives you\n\nBuilding on Differential Dataflow gave us three properties that show up directly in what you can build with InputLayer.\n\n**Incremental maintenance:** When a fact changes, only the affected derivations recompute. On a 2,000-node graph with 400,000 derived relationships, updating a single edge takes 6.83ms instead of 11.3 seconds. That's a 1,652x speedup that turns batch-only workloads into real-time operations.\n\n**Correct retraction:** Delete a fact, and everything derived through it disappears, but only if there's no alternative reasoning path. Phantom permissions, stale recommendations, lingering compliance flags. These bugs simply don't exist when the engine handles retraction correctly.\n\n**On-demand computation:** We combined Differential Dataflow with an optimization called Magic Sets, which rewrites recursive rules so the engine only computes what's needed for a specific query. Ask \"who does Alice have authority over?\" and the engine starts from Alice and follows only her paths. It doesn't compute authority for the entire organization. Query time is proportional to the relevant portion of the graph.\n\n## The tradeoffs\n\nNo engineering decision is free. Here's what we trade.\n\n**Memory:** Differential Dataflow keeps its working memory in RAM. For very large datasets, memory usage grows with the size of the maintained results. We handle this with persistent storage (Parquet files plus a write-ahead log) that lets us recover state without keeping everything in memory indefinitely. But it's a real consideration for very large knowledge graphs.\n\n**Complexity floor:** The Timely/Differential Dataflow programming model is powerful but has a steep learning curve. We invested significant engineering time building the abstraction layer that compiles high-level rules into efficient computation pipelines. You never touch the dataflow layer directly, but we do, and it required deep expertise to get right.\n\n**Single-node:** Currently, InputLayer runs on a single node. Timely Dataflow supports distributed computation, and that's on our roadmap. But today, the engine is bounded by what a single machine can handle. For most knowledge graph workloads, that's millions of facts and derived relationships, but it's a real limit for truly massive datasets.\n\n## Where the choice matters most\n\nThe Differential Dataflow foundation matters most for use cases where data changes frequently and derived conclusions need to stay current. Access control hierarchies where people change roles regularly. Supply chain graphs where supplier status changes daily. Compliance systems where entity relationships and sanctions lists are updated constantly. Agent memory systems where new observations arrive continuously.\n\nFor batch-once-query-many workloads with no updates, a simpler engine would be fine. But the moment your facts change and you need derived conclusions to stay correct, the incremental approach pays for itself immediately.\n\nOur [benchmarks post](/blog/benchmarks-1587x-faster-recursive-queries/) has the specific numbers. And the [quickstart guide](/docs/guides/quickstart/) gets you running in about 5 minutes so you can see it in action.",
    "toc": [
      {
        "level": 2,
        "text": "The problem that started everything",
        "id": "the-problem-that-started-everything"
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
        "text": "What this gives you",
        "id": "what-this-gives-you"
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
    "author": "",
    "category": "Use Case",
    "excerpt": "A $50K wire to Alpha Corp looks clean. But Alpha is a subsidiary of Beta, which is 60% owned by Gamma, which is controlled by a sanctioned entity. Four hops deep.",
    "content": "\n# Fraud Detection Through Entity Chain Reasoning\n\nA wire transfer from your client to Alpha Corp for $50,000. Your compliance system checks Alpha Corp against the sanctions list, finds nothing, and clears the transaction. Six months later, you're in front of a regulator explaining why you processed a payment to an entity that was four hops away from a sanctioned person. The fine is seven figures. The enforcement action is public. And the worst part: the information was sitting in your own data the whole time. Corporate registries, ownership filings, KYC records, all spread across databases that nobody had connected.\n\nAlpha Corp is a subsidiary of Beta LLC. Beta LLC is 60% owned by Gamma Holding. And Gamma Holding is 80% controlled by someone on a sanctions list. No single record in that chain looks suspicious. The risk only becomes visible when you follow the ownership path end to end, computing effective control at every hop.\n\n```graph\nAlpha Corp --subsidiary-> Beta LLC\nBeta LLC --60% owned-> Gamma Holding\nGamma Holding --80% owned-> Sanctioned Entity [highlight]\n```\n\nA simple sanctions-list lookup checks the direct counterparty against a list. That catches the obvious cases. It completely misses the layered structures that sophisticated actors actually use.\n\n## Why per-transaction rules miss this\n\nStandard fraud detection checks fields on individual transactions: amount thresholds, jurisdiction flags, direct sanctions matches. These catch the obvious cases. But the layered ownership structures that sophisticated actors use are invisible at the single-transaction level.\n\nThe problem is structural. The risk isn't in any single record. It's in the *path* through a network of entity relationships. And that path might span multiple registries and databases.\n\n```tree\nPattern matching [muted]\n  TX-001: Client -> Alpha Corp ($50K)\n    Sanctions: NO\n    PEP: NO\n    High-risk jurisdiction: NO\n```\n\n```graph\nAlpha Corp --subsidiary-> Beta LLC\nBeta LLC --60% owned-> Gamma Holding [highlight]\nGamma Holding --80% owned-> Sanctioned Person [highlight]\n```\n\nThe information exists across separate registries: corporate records, ownership filings, sanctions lists. No single database contains the complete picture. You have to follow the chain.\n\n## How InputLayer traces these chains\n\nIn InputLayer, you model entity relationships as facts: \"Person X owns 80% of Company Y.\" \"Company A is a subsidiary of Company B.\" These facts come from corporate registries, ownership databases, and KYC records, data you probably already collect.\n\nThen you define the compliance logic as a rule: \"An entity has sanctions exposure if it's directly sanctioned, or if it's owned (above a threshold) by an entity that has sanctions exposure.\"\n\nThat second clause is recursive, meaning it keeps repeating itself, following the chain as deep as it goes. At every level, it checks whether the owner has sanctions exposure. If the owner does, whether directly or through its own ownership chain, the exposure flows down.\n\nHere's what the engine does when it evaluates this rule against our example:\n\n```steps\nAlpha Corp: not sanctioned :: direct check\nOwner: Beta LLC (subsidiary) :: walk up\nBeta LLC: not sanctioned :: direct check\nOwner: Gamma Holding (60%) :: walk up\nGamma Holding: not sanctioned :: direct check\nOwner: Sanctioned Person (80%) :: walk up\nSanctioned Person: YES :: match found [highlight]\n```\n\n```graph\nSanctioned Person --owns-> Gamma Holding [highlight]\nGamma Holding --owns-> Beta LLC [highlight]\nBeta LLC --owns-> Alpha Corp [highlight]\n```\n\nThe engine didn't just check the direct counterparty. It walked the full ownership and control chain, evaluated the sanctions exposure rule at every level, and passed the result back down. All automatically, from a single rule definition.\n\nAnd this works for chains of any depth. Five layers of shell companies? No problem. Ten intermediaries? The engine follows the chain until there's nowhere left to go.\n\n## Beneficial ownership: the same pattern, different question\n\nRegulators worldwide are tightening beneficial ownership requirements. The core question is: who are the natural persons that ultimately own or control this entity?\n\nThe computation is surprisingly similar to sanctions screening, with one twist: you need to multiply ownership percentages through the layers.\n\n```graph\nPerson X --80%-> Holding A --60%-> Company B [primary]\n```\n\nEffective beneficial ownership of Person X in Company B: 80% x 60% = 48%. If your regulatory threshold is 25%, Person X is a beneficial owner of Company B even though they don't own it directly.\n\nAdd more layers, and the math compounds:\n\n```graph\nPerson X --80%-> Holding A --60%-> Sub B --70%-> Company C [primary]\n```\n\nEffective ownership: 80% x 60% x 70% = 33.6%. Still above 25%. Person X is a beneficial owner of Company C.\n\nInputLayer handles the multiplication and passes ownership percentages through any number of layers. Define a threshold, and the engine identifies every natural person who qualifies as a beneficial owner for every entity in your graph.\n\n## Structuring detection: connecting related entities\n\nBeyond direct sanctions, you need to detect structuring, where large transactions are split into smaller ones to avoid reporting thresholds. The standard approach checks individual transactions against the $10,000 threshold. Sophisticated actors split transactions across related entities to stay below it.\n\n```graph\nSanctioned Person --owns-> Entity A [highlight]\nSanctioned Person --owns-> Entity B [highlight]\nSanctioned Person --owns-> Entity C [highlight]\nEntity A --$4K-> Target Co\nEntity B --$3.5K-> Target Co\nEntity C --$3K-> Target Co\n```\n\nEach individual transaction is below $10,000. But the entities are related through common ownership, and their combined transactions to the same target exceed the threshold.\n\nInputLayer identifies these relationships automatically by following chains of ownership. It determines which entities are connected, aggregates their transactions within a time window, and fires an alert when the combined total exceeds the threshold. The \"related entity\" determination is itself a chain walk. Entity A and Entity C might be connected through multiple intermediate layers.\n\n## What happens when facts change\n\nThis is where the knowledge graph approach becomes especially valuable for compliance. Entity relationships change constantly. Companies are acquired. Ownership stakes are transferred. New sanctions designations are published. Old ones are lifted.\n\nWhen you add a new sanctions designation, say Gamma Holding's owner gets added to the list, InputLayer picks up the change instantly. It identifies every entity in that person's ownership chain, evaluates whether the ownership thresholds are met, and flags the affected transactions. On a graph with thousands of entities, this takes milliseconds.\n\n```flow\nFull recompute approach [highlight] -> Sanctions updated -> Recompute all (minutes) -> Stale until done\n```\n\n```flow\nInputLayer: Sanctions updated [success] -> Instant update (ms) -> Current immediately\n```\n\nThe reverse is equally important. When someone is removed from a sanctions list, all the downstream flags that were derived through their ownership chain clear automatically. No manual cleanup, no stale alerts clogging up your queue. And if an entity had sanctions exposure through *multiple* paths (e.g., owned by two sanctioned individuals), removing one designation correctly preserves the remaining exposure.\n\n## Getting started\n\nIf you're working on compliance, sanctions screening, or transaction monitoring, this approach to entity chain reasoning is worth exploring.\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) gets you running in about 5 minutes. The [recursion documentation](/docs/guides/recursion/) covers the chain-following reasoning that powers entity chain traversal.",
    "toc": [
      {
        "level": 2,
        "text": "Why per-transaction rules miss this",
        "id": "why-per-transaction-rules-miss-this"
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
        "text": "Structuring detection: connecting related entities",
        "id": "structuring-detection-connecting-related-entities"
      },
      {
        "level": 2,
        "text": "What happens when facts change",
        "id": "what-happens-when-facts-change"
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
    "title": "When Similarity Is Not Enough",
    "date": "2026-01-20",
    "author": "",
    "category": "Architecture",
    "excerpt": "Some queries need the system to follow a chain of relationships to reach the answer. Here's how to recognize them and what to do about it.",
    "content": "\n# When Similarity Is Not Enough\n\nA healthcare AI agent gets a simple question from a patient: \"Can I eat shrimp tonight?\" The system does exactly what it's designed to do. It embeds the query, runs a similarity search, and returns shrimp recipes, nutritional facts, and some seafood allergy FAQs. All highly relevant to the words \"eat shrimp.\" All completely wrong for this patient.\n\nThe patient takes Amiodarone, a cardiac medication that interacts with iodine. Shrimp is high in iodine. The correct answer is \"No, and here's why,\" but that answer doesn't live in any single document. It lives across three facts in three different systems: the patient's medication list, a drug interaction database, and a nutritional profile. The phrase \"shrimp dinner\" has zero textual similarity with \"medication contraindication.\" You can't find the connection by looking for similar text. You have to follow a chain of relationships from one fact to the next.\n\nThis is the gap between retrieval and reasoning, and it shows up far beyond healthcare. Every time the answer requires connecting facts across sources rather than finding a matching document, similarity search alone will return results that are semantically relevant but miss the logical connection.\n\n## Three signs you need reasoning alongside retrieval\n\n### 1. Results require stitching across multiple systems\n\nYou find yourself making multiple database calls and reconciling the results: query the vector database for relevant docs, query a graph for relationships, hit an auth service, merge everything in application code.\n\n```\n// This pattern means you're solving a reasoning problem with custom stitching code\nconst docs = await vectorDB.search(queryEmbedding, topK=50);\nconst userPerms = await authService.getPermissions(userId);\nconst filteredDocs = docs.filter(d =>\n  userPerms.departments.includes(d.metadata.department) ||\n  userPerms.teams.includes(d.metadata.team) ||\n  (d.metadata.author && await orgChart.isSubordinate(d.metadata.author, userId))\n);\n// ^ This recursive check is the tell\n```\n\nThat recursive `isSubordinate` check at the end is the tell. You've hit a reasoning problem and you're solving it with manual code and API calls. It works, but it's fragile, slow, and hard to keep consistent.\n\nInputLayer handles the entire thing in one query, resolving the org hierarchy, checking permissions, and filtering results in a single pass.\n\n### 2. Access policies involve hierarchies\n\nYour permission model started simple, department-based, maybe role-based. But now it involves hierarchies: managers can see their reports' documents, and their reports' reports, and so on down the chain.\n\n```graph\nUser --dept filter-> Documents [success]\n```\n\n```graph\nAlice --manages-> Bob --manages-> Carol --manages-> ??? [highlight]\n```\n\nYou can't express \"everyone in Alice's full chain of who-reports-to-whom\" as a flat filter because you don't know who's in that chain until you recursively traverse the org chart. And that chain changes every time someone joins, leaves, or transfers. InputLayer evaluates the hierarchy as part of the query, against the current org chart, every time.\n\n### 3. Derived conclusions outlive their source facts\n\nA partner relationship ended three months ago. The partnership flag was removed from the CRM. But the integration recommendations, the priority support routing, the shared document access. Those derived conclusions are still sitting in various caches and indexes. Nobody cleaned them up because nobody knows all the places they spread to.\n\n```tree\nPartnership ended [highlight]\n  Vector index: stale priority support [muted]\n  Recommendations: stale integration [muted]\n  Access list: stale partner docs [muted]\n```\n\n## How InputLayer handles these\n\nInputLayer follows chains of facts to derive conclusions. The mental model:\n\n```graph\nRetrieval [primary] --matches-> Content\n```\n\n```graph\nInputLayer [primary] --concludes-> Derived facts\n```\n\nMost real applications need both. A customer support agent needs to find relevant help articles (retrieval) and check the customer's subscription tier (reasoning). A research assistant needs to find related papers (retrieval) and trace the citation graph to foundational work (reasoning). A financial advisor needs to find matching investment products (retrieval) and verify regulatory compliance (reasoning).\n\nUse each system for what it does best. For cases where you need both at the same time, like \"find documents similar to X that this user is authorized to see through their reporting chain,\" InputLayer handles the combined query in a single pass with its native vector search capabilities.\n\n## Getting started\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) takes about 5 minutes. If you're specifically interested in combining vector search with reasoning, the [vectors documentation](/docs/guides/vectors/) covers InputLayer's native vector capabilities.",
    "toc": [
      {
        "level": 2,
        "text": "Three signs you need reasoning alongside retrieval",
        "id": "three-signs-you-need-reasoning-alongside-retrieval"
      },
      {
        "level": 3,
        "text": "1. Results require stitching across multiple systems",
        "id": "1-results-require-stitching-across-multiple-systems"
      },
      {
        "level": 3,
        "text": "2. Access policies involve hierarchies",
        "id": "2-access-policies-involve-hierarchies"
      },
      {
        "level": 3,
        "text": "3. Derived conclusions outlive their source facts",
        "id": "3-derived-conclusions-outlive-their-source-facts"
      },
      {
        "level": 2,
        "text": "How InputLayer handles these",
        "id": "how-inputlayer-handles-these"
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
    "author": "",
    "category": "Engineering",
    "excerpt": "Bob left the company. His direct access was revoked. But Alice's indirect access to documents through Bob's position was never cleaned up. For three months.",
    "content": "\n# Correct Retraction: Why Delete Should Actually Delete\n\nThree months after a security incident, your forensics team discovers something troubling. A former employee, Bob, had his access revoked on the day he left. His account was deactivated. His role was removed from the auth system. Everything looked clean. But Bob had authority over a team of six people, and those six people had authored sensitive documents. The system had figured out that Bob's manager, Alice, could access those documents through Bob's position. When Bob left, his direct access disappeared. But Alice's indirect access, the part she had only because of Bob, was never cleaned up. For three months, Alice could see documents she had no business seeing. Your compliance team spent 200 hours on the resulting investigation, and the company disclosed the access violation to two regulators.\n\nThis isn't a contrived scenario. It's what happens when derived permissions don't retract correctly. And it's not limited to access control. Stale compliance flags keep triggering investigation queues for weeks after the underlying risk is resolved. Phantom entity relationships cause false positives in sanctions screening. Recommendation signals keep surfacing products long after they've been discontinued. If you're building any system that automatically builds up conclusions from connected facts, you face this problem, and the three common approaches each have significant tradeoffs.\n\nInputLayer solves this by counting support. Every conclusion the system reaches tracks how many independent paths lead to it. Remove a path, and the count goes down. Only when it reaches zero does the conclusion disappear. This handles the simple cases and the hard ones, like when Alice has authority over Charlie through both Bob and Diana, and removing one path should preserve the other.\n\n## Simple on the surface, hard underneath\n\nAt first glance, retraction seems trivial. Delete a fact, delete everything that depended on it. Done.\n\nLet's walk through why it's not that simple.\n\nAlice manages Bob. Bob manages Charlie. The system derives indirect authority:\n\n```tree\nAlice [primary]\n  Bob (direct report)\n    Charlie (Bob's direct report)\n```\n\n```graph\nAlice --manages-> Bob --manages-> Charlie\n```\n\nBob leaves the company. You remove \"Alice manages Bob.\" What should happen?\n\n```steps\nauthority(Alice, Bob) :: RETRACT [highlight]\nauthority(Alice, Charlie) :: RETRACT - derived via Bob [highlight]\nauthority(Bob, Charlie) :: KEEP - independent [success]\n```\n\nAlice loses authority over both Bob and Charlie. But Bob keeps authority over Charlie because that relationship doesn't depend on Alice's management of Bob. The retraction needs to be precise. It can't just blindly walk down the chain and delete everything it finds.\n\nOK, that's manageable. But now consider the harder case.\n\n## The diamond problem\n\nAlice manages both Bob and Diana. Both Bob and Diana manage Charlie.\n\n```tree\nAlice [primary]\n  Bob\n    Charlie\n  Diana\n    Charlie\n```\n\nAlice has authority over Charlie through *two independent paths*: one through Bob and one through Diana. The conclusion `authority(Alice, Charlie)` has two reasons to exist.\n\nNow Bob stops managing Charlie:\n\n```tree\nAlice [primary]\n  Bob [muted]\n  Diana\n    Charlie [success]\n```\n\nShould Alice lose authority over Charlie? **No.** The path through Diana still supports it.\n\nNow Diana also stops managing Charlie:\n\n```tree\nAlice [primary]\n  Bob [muted]\n  Diana [muted]\nCharlie (no paths remain) [highlight]\n```\n\n*Now* Alice should lose authority over Charlie. Both supporting paths are gone.\n\nThis is the multiple paths problem, and it's what makes correct retraction genuinely difficult. A conclusion should only disappear when *every* path that supports it has been removed. Not when the first path is removed. Not when most paths are removed. Only when the count reaches zero.\n\n## The three common approaches\n\n**Append-only.** You mark a source fact as deleted, but leave derived facts in whatever cache, index, or materialized view they were written to. Fast, but you get phantom permissions and ghost recommendations. Stale conclusions that you can't clean up because you don't know where they all spread to.\n\n**Full recomputation.** You throw away all derived data and re-derive from scratch. Correct, but expensive. Seconds to minutes on large knowledge graphs. Between batch runs, your data is potentially inconsistent.\n\n**Follow-the-chain deletion.** You walk from the retracted fact and delete anything downstream. Fast, but wrong whenever the diamond problem appears. You'll delete conclusions that should have survived because they had alternative paths.\n\n```tree\nApproaches [primary]\n  Append-only\n    Retraction: No [highlight]\n    Diamond: No [highlight]\n  Full recomputation\n    Retraction: Yes [success]\n    Diamond: Yes [success]\n    Speed: Slow [highlight]\n  Follow-the-chain deletion\n    Retraction: Yes [success]\n    Diamond: No [highlight]\n    Speed: Fast but wrong [highlight]\n  Support counting (InputLayer)\n    Retraction: Yes [success]\n    Diamond: Yes [success]\n    Speed: Fast and correct [success]\n```\n\n## How InputLayer solves it: counting support\n\nInputLayer is built on Differential Dataflow, which tracks a support count for every conclusion the system reaches. That count reflects the number of independent paths that lead to the conclusion.\n\nHere's the diamond example, step by step:\n\n```steps\nInitial: count = 2 (via Bob + Diana) :: authority(Alice, Charlie)\nRemove Bob path: count = 1 :: survives [success]\nRemove Diana path: count = 0 :: retracted [highlight]\n```\n\nThe engine doesn't need to search for alternative paths or do any special-case reasoning. The counting handles it automatically. And this works through any number of recursive levels. If your reasoning chain is 10 hops deep with branching paths at every level, the counts still track correctly.\n\n## Retraction through recursive chains\n\nThe diamond problem is hard enough with a single level of reasoning. With recursion, it gets harder. But the counting approach still handles it.\n\nConsider a deeper hierarchy:\n\n```graph\nAlice --manages-> Bob --manages-> Charlie --manages-> Diana --manages-> Eve [primary]\n```\n\nThe conclusion `authority(Alice, Eve)` goes through 4 hops. If you remove \"Charlie manages Diana,\" the engine needs to retract not just `authority(Charlie, Diana)` but also `authority(Alice, Diana)`, `authority(Bob, Diana)`, `authority(Alice, Eve)`, `authority(Bob, Eve)`, and `authority(Charlie, Eve)`. Every derived authority that passed through the Charlie-Diana link.\n\nBut if Diana also reports to someone else (say, Frank, who reports to Alice through a different branch), some of those authority relationships might survive through the alternative path.\n\nThe engine tracks all of this through its counting mechanism. Each removal ripples through the reasoning chain as a -1 adjustment. At each step, the adjustment combines with the existing count. Conclusions retract when and only when their count reaches zero. No manual reasoning about paths needed.\n\n## Why this matters: three real scenarios\n\n**Access control:** When someone leaves your company, every permission derived through their position needs to disappear. But only the permissions that were *exclusively* derived through their position. If a document was accessible through two independent authorization paths and you remove one, access should continue through the remaining path. Getting this wrong means either phantom permissions (security risk) or over-retraction (broken access for people who should still have it).\n\n**Recommendations:** When you discontinue a product, every recommendation that included it should vanish. If a recommendation was \"users who bought X also bought Y,\" and Y is discontinued, the recommendation disappears. But if Y was also recommended through a different signal (semantic similarity, category affinity), that recommendation should survive through the remaining signal.\n\n**Compliance:** When an entity is removed from a sanctions list, every downstream flag derived from that designation should clear. But if an entity had sanctions exposure through two different ownership paths, removing one designation should correctly preserve the remaining exposure. Your compliance team should not be chasing alerts that are no longer valid, and should also not miss alerts that are still valid because the retraction was too aggressive.\n\n## Performance\n\nCorrect retraction is only useful if it's fast enough to happen in real time. If propagating a retraction takes seconds, you're back to batch processing.\n\n| Operation | Time (2,000-node graph) |\n|---|---|\n| Retract 1 edge, propagate all downstream changes | <10ms |\n| Retract 10 edges, propagate all downstream changes | ~100ms |\n| Retract 100 edges, propagate all downstream changes | ~1 second |\n\nThese numbers come from our benchmark graph with ~400,000 derived relationships. The incremental approach means each retraction only touches the affected portion of the reasoning chain. The total graph size barely matters. What matters is the size of the ripple effect from the specific retraction.\n\n## Getting started\n\nIf you want to see correct retraction in action, the [quickstart guide](/docs/guides/quickstart/) walks through a hands-on example. The [recursion documentation](/docs/guides/recursion/) explains how recursive rules interact with retraction. And our [benchmarks post](/blog/benchmarks-1587x-faster-recursive-queries/) covers the performance characteristics in detail.\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```",
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
        "text": "The three common approaches",
        "id": "the-three-common-approaches"
      },
      {
        "level": 2,
        "text": "How InputLayer solves it: counting support",
        "id": "how-inputlayer-solves-it-counting-support"
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
    "slug": "financial-risk",
    "title": "Financial Risk and Compliance",
    "icon": "Shield",
    "subtitle": "Auditable entity reasoning for sanctions screening, beneficial ownership traversal, and policy enforcement.",
    "order": 1,
    "content": "\n# Sanctions screening through ownership chains\n\nIn 2014, a major European bank paid $8.9 billion for processing transactions with sanctioned entities. The transactions themselves looked clean. The exposure was buried multiple levels deep in ownership chains that nobody traced in time.\n\nThis is the core challenge of sanctions compliance: the entity you're transacting with might be perfectly legitimate, but if you follow the ownership chain upward - through holding companies, subsidiaries, and partial stakes - you might find a sanctioned person or organization at the top. Miss that chain, and you're liable. Finding it requires recursive reasoning through corporate structures that can be dozens of layers deep, with multiple paths to the same entity.\n\nThere's a subtlety that makes this genuinely hard. When an entity gets cleared from a sanctions list, every flag that was derived through that entity needs to retract. But only the flags that depended exclusively on that entity - if there's a second, independent ownership path that still connects to a sanctioned entity, the flag needs to stay. This is called the diamond problem, and getting it wrong means either phantom flags that waste your compliance team's time, or missed exposures that create regulatory risk.\n\n---\n\n## The setup\n\nAlpha owns two subsidiaries: Beta and Delta. Both Beta and Delta own stakes in Gamma. Gamma is on a sanctions list.\n\n```graph\nAlpha --owns-> Beta\nAlpha --owns-> Delta\nBeta --owns-> Gamma [highlight]\nDelta --owns-> Gamma [highlight]\n```\n\nTwo independent paths from Alpha to the sanctioned entity. Let's trace what happens.\n\n---\n\n## Loading facts and defining the rule\n\n```iql\n// The ownership structure\n+owns[(\"alpha\", \"beta\"), (\"alpha\", \"delta\"), (\"beta\", \"gamma\"), (\"delta\", \"gamma\")]\n\n// Gamma is sanctioned\n+sanctions_list[(\"gamma\")]\n```\n\nThe rule says: an entity is exposed if it owns a sanctioned entity, or if it owns something that is itself exposed. That second clause is recursive - it follows the chain to any depth, whether it's 2 hops or 20.\n\n```iql\n+exposed(E, S) <- owns(E, S), sanctions_list(S)\n+exposed(E, S) <- owns(E, Mid), exposed(Mid, S)\n```\n\n---\n\n## Query: is Alpha exposed?\n\n```iql\n?exposed(\"alpha\", Who)\n```\n\n```\n┌─────────┬─────────┐\n│ alpha   │ Who     │\n├─────────┼─────────┤\n│ \"alpha\" │ \"gamma\" │\n└─────────┴─────────┘\n1 rows\n```\n\nYes. Alpha is exposed to Gamma through two independent ownership paths.\n\n---\n\n## The diamond problem in action\n\nBeta divests its stake in Gamma.\n\n```iql\n-owns(\"beta\", \"gamma\")\n```\n\n```iql\n?exposed(\"alpha\", Who)\n```\n\n```\n┌─────────┬─────────┐\n│ alpha   │ Who     │\n├─────────┼─────────┤\n│ \"alpha\" │ \"gamma\" │\n└─────────┴─────────┘\n1 rows\n```\n\nAlpha is still exposed. The path through Delta still supports the flag. If this retracted prematurely, a compliance team would look at Alpha, see no flag, and approve a transaction that should have been held. InputLayer tracks both paths independently - the conclusion only retracts when every supporting path is gone.\n\nNow Delta also divests.\n\n```iql\n-owns(\"delta\", \"gamma\")\n```\n\n```iql\n?exposed(\"alpha\", Who)\n```\n\n```\nNo results.\n```\n\nBoth paths are gone. The exposure retracts cleanly. No phantom flag lingering in a queue for someone to investigate. No manual cleanup.\n\n---\n\n## Showing the work\n\nWhen a flag is active, `.why` returns the exact chain of ownership and rules that produced it:\n\n```iql\n.why ?exposed(\"alpha\", Who)\n```\n\nThe proof tree shows: Alpha is exposed to Gamma because Alpha owns Delta, Delta owns Gamma, and Gamma is on the sanctions list. Each link in the chain traces to a specific fact and a specific rule. This is what goes in the case file. This is what the regulator sees.\n\nWhen a flag is missing and shouldn't be, `.why_not` identifies exactly which condition failed:\n\n```iql\n.why_not exposed(\"delta\", \"gamma\")\n```\n\n```\nexposed(\"delta\", \"gamma\") was NOT derived:\n\n  Rule: exposed (clause 0)\n    exposed(E, S) <- owns(E, S), sanctions_list(S)\n    Blocker: owns(\"delta\", \"gamma\") - No matching tuples\n\n  Rule: exposed (clause 1)\n    exposed(E, S) <- owns(E, Mid), exposed(Mid, S)\n    Blocker: owns(\"delta\", _) - No matching tuples\n```\n\nDelta is not exposed because it no longer owns anything. The blocker is specific and auditable.\n\n---\n\n## Try it\n\nEvery code block on this page runs against a live InputLayer instance. Paste them into the [demo](https://demo.inputlayer.ai) to see the results yourself.",
    "toc": [
      {
        "level": 2,
        "text": "The setup",
        "id": "the-setup"
      },
      {
        "level": 2,
        "text": "Loading facts and defining the rule",
        "id": "loading-facts-and-defining-the-rule"
      },
      {
        "level": 2,
        "text": "Query: is Alpha exposed?",
        "id": "query-is-alpha-exposed"
      },
      {
        "level": 2,
        "text": "The diamond problem in action",
        "id": "the-diamond-problem-in-action"
      },
      {
        "level": 2,
        "text": "Showing the work",
        "id": "showing-the-work"
      },
      {
        "level": 2,
        "text": "Try it",
        "id": "try-it"
      }
    ]
  },
  {
    "slug": "commerce",
    "title": "Conversational Commerce",
    "icon": "ShoppingBag",
    "subtitle": "Compatible product recommendations from purchase history and live inventory, in one query.",
    "order": 2,
    "content": "\n# Product recommendations that understand compatibility\n\nA shopper types \"I need ink for my printer.\" There are hundreds of ink cartridges in the catalog. In embedding space, a Canon PG-245 and an Epson 202 are nearly identical - they're both black ink cartridges with similar descriptions, similar prices, similar use cases. A vector search returns both with almost the same score.\n\nBut the shopper owns a Canon printer. The Epson doesn't fit. That's not a similarity problem - no amount of embedding refinement will fix it. The connection between a specific printer and its compatible cartridges is a structured fact in a compatibility table, not a distance in vector space.\n\nThis matters because recommending an incompatible product isn't just irrelevant - it's a return, a support ticket, and a customer who trusts your suggestions less next time. And the fix isn't post-filtering (checking compatibility after retrieval) because you'd need to stitch together purchase history, compatibility data, inventory status, and similarity ranking across multiple systems for every single query.\n\n---\n\n## The setup\n\nThree cartridges, one printer, one shopper. Two cartridges are compatible with Canon printers. One is not. All three have very similar embeddings.\n\n```iql\n// Products with embedding vectors\n+product[\n    (\"pg245\", \"Canon PG-245 Black Ink\", 14.99, [0.82, 0.15, 0.91, 0.44]),\n    (\"cl246\", \"Canon CL-246 Color Ink\", 16.99, [0.79, 0.18, 0.88, 0.41]),\n    (\"ep202\", \"Epson 202 Black Ink\", 12.99, [0.83, 0.14, 0.90, 0.43])\n]\n\n// Compatibility: which cartridges fit which printers\n+compatible[(\"canon_mg3620\", \"pg245\"), (\"canon_mg3620\", \"cl246\")]\n\n// Shopper 42 owns a Canon printer. All three are in stock.\n+owns[(\"shopper_42\", \"canon_mg3620\")]\n+in_stock[(\"pg245\"), (\"cl246\"), (\"ep202\")]\n```\n\nLook at the embeddings. PG-245 is `[0.82, 0.15, 0.91, 0.44]`. Epson 202 is `[0.83, 0.14, 0.90, 0.43]`. Almost identical. A vector search alone can't distinguish them.\n\n---\n\n## The rule\n\nA product is recommendable to a shopper if they own a compatible device and the product is in stock. One line.\n\n```iql\n+recommendable(S, P) <- owns(S, Dev), compatible(Dev, P), in_stock(P)\n```\n\nThis connects three separate facts - purchase history, compatibility matrix, and inventory - into one derivation chain. The engine evaluates it every time any of those facts change.\n\n---\n\n## Rules filter first\n\n```iql\n?recommendable(\"shopper_42\", Pid)\n```\n\n```\n┌──────────────┬─────────┐\n│ shopper_42   │ Pid     │\n├──────────────┼─────────┤\n│ \"shopper_42\" │ \"cl246\" │\n│ \"shopper_42\" │ \"pg245\" │\n└──────────────┴─────────┘\n2 rows\n```\n\nTwo results. The Epson 202 is excluded - not because of its embedding, but because it's not compatible with a Canon printer. The rule did the filtering before similarity ever ran.\n\n---\n\n## Then vectors rank\n\nNow add cosine distance to rank the compatible results by relevance. Lower distance means more similar.\n\n```iql\n?recommendable(\"shopper_42\", Pid),\n product(Pid, Desc, Price, Emb),\n Dist = cosine(Emb, [0.81, 0.16, 0.89, 0.42]),\n Dist < 0.05\n```\n\n```\n┌──────────────┬─────────┬──────────────────────────┬───────┬────────────────────────┐\n│ shopper_42   │ Pid     │ Desc                     │ Price │ Dist                   │\n├──────────────┼─────────┼──────────────────────────┼───────┼────────────────────────┤\n│ \"shopper_42\" │ \"pg245\" │ \"Canon PG-245 Black Ink\" │ 14.99 │ 0.0001                 │\n│ \"shopper_42\" │ \"cl246\" │ \"Canon CL-246 Color Ink\" │ 16.99 │ 0.0002                 │\n└──────────────┴─────────┴──────────────────────────┴───────┴────────────────────────┘\n2 rows\n```\n\nOne query. Rules filtered to what's compatible, vectors ranked by relevance. The Epson - which would have scored nearly identically on cosine distance - was never considered.\n\n---\n\n## When stock changes\n\nThe PG-245 sells out.\n\n```iql\n-in_stock(\"pg245\")\n```\n\n```iql\n?recommendable(\"shopper_42\", Pid)\n```\n\n```\n┌──────────────┬─────────┐\n│ shopper_42   │ Pid     │\n├──────────────┼─────────┤\n│ \"shopper_42\" │ \"cl246\" │\n└──────────────┴─────────┘\n1 rows\n```\n\nGone immediately. The shopper never sees a product they can't buy. When it's restocked, the recommendation comes back. No reindex, no cache invalidation - the rule re-evaluates against the current facts.\n\n---\n\n## The pattern\n\nThis applies anywhere the connection between \"what I have\" and \"what fits it\" is a structured fact: replacement parts for appliances, cables for electronics, lenses for cameras, blades for power tools. In every case, similarity search finds things that look right but might not fit. The compatibility rule is what makes the recommendation trustworthy.\n\nEvery code block on this page runs against a live InputLayer instance. Paste them into the [demo](https://demo.inputlayer.ai) to see the results yourself.",
    "toc": [
      {
        "level": 2,
        "text": "The setup",
        "id": "the-setup"
      },
      {
        "level": 2,
        "text": "The rule",
        "id": "the-rule"
      },
      {
        "level": 2,
        "text": "Rules filter first",
        "id": "rules-filter-first"
      },
      {
        "level": 2,
        "text": "Then vectors rank",
        "id": "then-vectors-rank"
      },
      {
        "level": 2,
        "text": "When stock changes",
        "id": "when-stock-changes"
      },
      {
        "level": 2,
        "text": "The pattern",
        "id": "the-pattern"
      }
    ]
  },
  {
    "slug": "manufacturing",
    "title": "Manufacturing Operations",
    "icon": "Factory",
    "subtitle": "Live operational reasoning for production planning, equipment availability, and job spec fulfillment.",
    "order": 3,
    "content": "\n# Production planning through dependency chains\n\nA planning agent says Line 4 can run the night shift. It checked the equipment status, the parts inventory, the maintenance schedule. Everything looked good at 6:00am. But at 6:03am, an operator's safety certification expired because her training lapsed. Nobody told the planning agent. The shift runs. An auditor finds the uncertified operator three weeks later.\n\nThe problem isn't the data - it was there. The problem is that the answer to \"can this line run?\" depends on a chain of facts that goes several levels deep: a production line depends on a qualified operator, the operator's qualification depends on a current certification, and the certification depends on completed training. When any link in that chain breaks, every conclusion built on it should update immediately. Not at the next refresh. Not when someone checks manually. Immediately.\n\n---\n\n## The setup\n\nLine 4 needs Operator Kim. Line 5 needs Operator Lee. Both are qualified through a shared safety certification. That certification is backed by training Kim completed in January.\n\n```iql\n// Which lines need which operators\n+requires[(\"line_4\", \"operator_kim\"), (\"line_5\", \"operator_lee\")]\n\n// Which operators hold which certifications\n+qualified_by[(\"operator_kim\", \"cert_safety\"), (\"operator_lee\", \"cert_safety\")]\n\n// Which training backs the certification\n+valid_certification[(\"cert_safety\", \"training_jan_2026\")]\n\n// The training record\n+training_completed[(\"training_jan_2026\")]\n```\n\nA four-level dependency chain: line depends on operator, operator depends on certification, certification depends on training.\n\n---\n\n## The rules\n\nSomething is available if everything it depends on is also available. This is recursive - it follows the chain from training up to production line, at any depth.\n\n```iql\n+available(X) <- training_completed(X)\n+available(X) <- valid_certification(X, Dep), available(Dep)\n+available(X) <- qualified_by(X, Dep), available(Dep)\n+available(X) <- requires(X, Dep), available(Dep)\n```\n\nTraining completion is the base case. Everything else is available if its dependency is available. The engine chains these together automatically.\n\n---\n\n## Everything is available\n\n```iql\n?available(X)\n```\n\n```\n┌─────────────────────┐\n│ X                   │\n├─────────────────────┤\n│ \"cert_safety\"       │\n│ \"line_4\"            │\n│ \"line_5\"            │\n│ \"operator_kim\"      │\n│ \"operator_lee\"      │\n│ \"training_jan_2026\" │\n└─────────────────────┘\n6 rows\n```\n\nSix things are available. The engine traced the full chain: training exists, so the certification is valid, so the operators are qualified, so the lines can run.\n\n---\n\n## Training expires\n\nOne fact removed. Kim's January training expires.\n\n```iql\n-training_completed(\"training_jan_2026\")\n```\n\n```iql\n?available(X)\n```\n\n```\nNo results.\n```\n\nEverything collapsed. Training gone, certification invalid, both operators unqualified, both lines unavailable. Four levels of retraction from a single fact change, propagated in milliseconds. This is the moment - in architectures where each system maintains its own derived state independently, that expired training would sit in one database while the planning system in another database still shows Kim as qualified. The planning agent builds a shift plan on stale state. With InputLayer, the planning agent's next query sees current reality.\n\n---\n\n## Recovery\n\nKim completes new safety training in March.\n\n```iql\n+training_completed[(\"training_mar_2026\")]\n+valid_certification[(\"cert_safety\", \"training_mar_2026\")]\n```\n\n```iql\n?available(X)\n```\n\n```\n┌─────────────────────┐\n│ X                   │\n├─────────────────────┤\n│ \"cert_safety\"       │\n│ \"line_4\"            │\n│ \"line_5\"            │\n│ \"operator_kim\"      │\n│ \"operator_lee\"      │\n│ \"training_mar_2026\" │\n└─────────────────────┘\n6 rows\n```\n\nBoth lines are back. The new training fact propagated up through the entire dependency chain. No manual reconciliation between systems.\n\n---\n\n## The proof trail\n\n```iql\n.why ?available(\"line_4\")\n```\n\nThe proof tree shows: line_4 is available because it requires operator_kim, and operator_kim is available because she's qualified by cert_safety, and cert_safety is available because training_mar_2026 is completed. When an auditor asks why Line 4 was allowed to run, the answer traces through four levels of dependencies to the specific training record.\n\nEvery code block on this page runs against a live InputLayer instance. Paste them into the [demo](https://demo.inputlayer.ai) to see the results yourself.",
    "toc": [
      {
        "level": 2,
        "text": "The setup",
        "id": "the-setup"
      },
      {
        "level": 2,
        "text": "The rules",
        "id": "the-rules"
      },
      {
        "level": 2,
        "text": "Everything is available",
        "id": "everything-is-available"
      },
      {
        "level": 2,
        "text": "Training expires",
        "id": "training-expires"
      },
      {
        "level": 2,
        "text": "Recovery",
        "id": "recovery"
      },
      {
        "level": 2,
        "text": "The proof trail",
        "id": "the-proof-trail"
      }
    ]
  },
  {
    "slug": "supply-chain",
    "title": "Supply Chain",
    "icon": "Truck",
    "subtitle": "Supplier risk propagation, sanctions screening, and order fulfillment reasoning over live entity graphs.",
    "order": 4,
    "content": "\n# Disruption cascades across supply chains\n\nIn March 2021, a container ship blocked the Suez Canal for six days. The disruption cascaded through global supply chains for months - but many companies didn't know which of their orders were affected until days after the blockage began. The information was there (which suppliers used which shipping routes, which orders depended on which suppliers), but connecting the dots across systems took time. By then, the window to reroute had passed.\n\nThe challenge isn't knowing that a port is closed. It's knowing, within milliseconds, which of your suppliers ship through that port, which orders depend on those suppliers, which of those orders have SLA penalty clauses, and which customers are about to be impacted. That's a chain of reasoning across your entire supply graph, and it needs to update every time a fact changes - a port closes, a supplier reroutes, a new order is placed.\n\n---\n\n## The setup\n\nThree suppliers. Supplier A and B ship through Shanghai. Supplier C ships through Busan. They feed four orders. Two of those orders have SLA penalty clauses.\n\n```iql\n// Suppliers and their shipping ports\n+ships_via[(\"supplier_a\", \"shanghai\"), (\"supplier_b\", \"shanghai\"), (\"supplier_c\", \"busan\")]\n\n// Which suppliers feed which orders\n+supplies[\n    (\"supplier_a\", \"order_101\"), (\"supplier_a\", \"order_102\"),\n    (\"supplier_b\", \"order_103\"), (\"supplier_c\", \"order_104\")\n]\n\n// SLA penalty clauses\n+has_sla[(\"order_101\", \"globex_corp\", \"penalty_5pct\"), (\"order_103\", \"initech\", \"penalty_10pct\")]\n\n// Port status\n+port_status[(\"shanghai\", \"open\"), (\"busan\", \"open\")]\n```\n\n---\n\n## The rules\n\nThree rules, each one step in the cascade. A supplier is disrupted if its port is closed. An order is at risk if its supplier is disrupted. An SLA is triggered if an at-risk order has a penalty clause.\n\n```iql\n+supplier_disrupted(Sup) <- ships_via(Sup, Port), port_status(Port, \"closed\")\n+order_at_risk(Order) <- supplies(Sup, Order), supplier_disrupted(Sup)\n+sla_triggered(Order, Customer, Penalty) <- order_at_risk(Order), has_sla(Order, Customer, Penalty)\n```\n\n---\n\n## Everything is fine\n\n```iql\n?supplier_disrupted(X)\n```\n\n```\nNo results.\n```\n\nNo disruptions. All ports are open. All orders are on track.\n\n---\n\n## Shanghai closes\n\nOne fact changes.\n\n```iql\n-port_status(\"shanghai\", \"open\")\n+port_status[(\"shanghai\", \"closed\")]\n```\n\nThe cascade propagates through the entire graph:\n\n```iql\n?supplier_disrupted(X)\n```\n\n```\n┌──────────────┐\n│ X            │\n├──────────────┤\n│ \"supplier_a\" │\n│ \"supplier_b\" │\n└──────────────┘\n2 rows\n```\n\n```iql\n?order_at_risk(X)\n```\n\n```\n┌─────────────┐\n│ X           │\n├─────────────┤\n│ \"order_101\" │\n│ \"order_102\" │\n│ \"order_103\" │\n└─────────────┘\n3 rows\n```\n\n```iql\n?sla_triggered(Order, Customer, Penalty)\n```\n\n```\n┌─────────────┬───────────────┬─────────────────┐\n│ Order       │ Customer      │ Penalty         │\n├─────────────┼───────────────┼─────────────────┤\n│ \"order_101\" │ \"globex_corp\" │ \"penalty_5pct\"  │\n│ \"order_103\" │ \"initech\"     │ \"penalty_10pct\" │\n└─────────────┴───────────────┴─────────────────┘\n2 rows\n```\n\nOne fact change. Two suppliers disrupted, three orders at risk, two SLA penalties triggered - identified across three levels of the supply graph. Supplier C and order 104 are unaffected (Busan is still open). This is the kind of fan-out that takes hours to trace manually and seconds in a dashboard query - but with InputLayer, the derived state is already current by the time you ask.\n\n---\n\n## Partial recovery\n\nSupplier A reroutes to Busan.\n\n```iql\n-ships_via(\"supplier_a\", \"shanghai\")\n+ships_via[(\"supplier_a\", \"busan\")]\n```\n\n```iql\n?sla_triggered(Order, Customer, Penalty)\n```\n\n```\n┌─────────────┬───────────┬─────────────────┐\n│ Order       │ Customer  │ Penalty         │\n├─────────────┼───────────┼─────────────────┤\n│ \"order_103\" │ \"initech\" │ \"penalty_10pct\" │\n└─────────────┴───────────┴─────────────────┘\n1 rows\n```\n\nSupplier A recovered. Orders 101 and 102 are no longer at risk. Globex's SLA penalty retracted. But Supplier B is still disrupted (still shipping through Shanghai), so order 103 and Initech's penalty remain. Partial recovery, correctly tracked - each path is independent.\n\n---\n\n## The proof trail\n\n```iql\n.why ?order_at_risk(\"order_103\")\n```\n\nThe proof shows: order_103 is at risk because it's supplied by supplier_b, and supplier_b is disrupted because it ships via Shanghai, and Shanghai is closed. Three facts, three rules, one chain. When a procurement team needs to explain to Initech why their order is delayed, the answer is a structured trace, not a phone call to someone who might know.\n\nEvery code block on this page runs against a live InputLayer instance. Paste them into the [demo](https://demo.inputlayer.ai) to see the results yourself.",
    "toc": [
      {
        "level": 2,
        "text": "The setup",
        "id": "the-setup"
      },
      {
        "level": 2,
        "text": "The rules",
        "id": "the-rules"
      },
      {
        "level": 2,
        "text": "Everything is fine",
        "id": "everything-is-fine"
      },
      {
        "level": 2,
        "text": "Shanghai closes",
        "id": "shanghai-closes"
      },
      {
        "level": 2,
        "text": "Partial recovery",
        "id": "partial-recovery"
      },
      {
        "level": 2,
        "text": "The proof trail",
        "id": "the-proof-trail"
      }
    ]
  },
  {
    "slug": "agentic-ai",
    "title": "Agentic AI",
    "icon": "Brain",
    "subtitle": "Structured memory, multi-hop reasoning, and policy-aware retrieval for AI agents.",
    "order": 5,
    "content": "\n# Agent memory that can explain itself\n\nA customer success agent flags Acme Corp as a churn risk. The VP of Customer Success asks: \"Why?\" The agent says \"based on the available data.\" That's not an answer. Which data? Which logic? What would need to change for Acme to not be at risk? If the agent can't show its work, the VP can't trust the flag, can't prioritize it, and can't act on it.\n\nThis is the fundamental problem with agent memory stored as text chunks in a vector database. The agent retrieves relevant passages, generates a conclusion, and the reasoning is whatever happened inside the model. There's no trace to inspect, no rule to audit, no way to ask \"what would need to change for this conclusion to be different?\"\n\nWhen agents store observations as structured facts and derive conclusions through explicit rules, every conclusion has a proof tree. You can ask why. You can ask why not. And when the underlying facts change, the conclusions update automatically - no stale beliefs lingering in a context window.\n\n---\n\n## The setup\n\nA customer success agent monitors two accounts. It has stored three observations about each.\n\n```iql\n// Acme: enterprise customer, $150K contract, declining usage, renewal in April\n+customer[(\"acme\", \"enterprise\", 150000)]\n+usage_trend[(\"acme\", \"declining\")]\n+renewal[(\"acme\", \"2026-04-15\")]\n\n// Globex: startup, $25K contract, growing usage, renewal in September\n+customer[(\"globex\", \"startup\", 25000)]\n+usage_trend[(\"globex\", \"growing\")]\n+renewal[(\"globex\", \"2026-09-01\")]\n```\n\nSix facts. Each one is a specific, testable observation - not a text passage that might or might not be retrieved in the right context window.\n\n---\n\n## The rules\n\nChurn risk requires three conditions: the customer is high-value, their usage is dropping, and their renewal is imminent. Each condition is its own rule with clear criteria.\n\n```iql\n// High-value: enterprise tier with contract over $100K\n+high_value(C) <- customer(C, \"enterprise\", Amt), Amt > 100000\n\n// Engagement dropping\n+engagement_drop(C) <- usage_trend(C, \"declining\")\n\n// Renewal within the next few months\n+renewal_soon(C) <- renewal(C, Date), Date < \"2026-06-01\"\n\n// Churn risk requires all three\n+churn_risk(C) <- high_value(C), engagement_drop(C), renewal_soon(C)\n```\n\nThese rules are readable, auditable, and versionable. When the definition of \"high-value\" changes (say the threshold moves from $100K to $75K), you change one rule and every derivation updates.\n\n---\n\n## Who is at risk?\n\n```iql\n?churn_risk(C)\n```\n\n```\n┌────────┐\n│ C      │\n├────────┤\n│ \"acme\" │\n└────────┘\n1 rows\n```\n\nAcme is flagged. Globex is not.\n\n---\n\n## Why is Acme flagged?\n\nThis is the point of the entire page. The `.why` command returns the proof tree - the exact chain of facts and rules that produced the conclusion.\n\n```iql\n.why ?churn_risk(\"acme\")\n```\n\nThe proof tree shows three branches: Acme is high-value because it's an enterprise customer with a $150K contract (which exceeds the $100K threshold). Acme has an engagement drop because its usage trend is \"declining.\" Acme's renewal is soon because April 15th is before June 1st. All three conditions met, so the churn_risk rule fired.\n\nThis isn't a model's interpretation. It's a deterministic chain you can inspect, challenge, and reproduce. The VP sees exactly which conditions triggered the flag, and can decide which intervention addresses which condition.\n\n---\n\n## Why is Globex NOT flagged?\n\nEqually important: understanding why a conclusion was not reached.\n\n```iql\n.why_not churn_risk(\"globex\")\n```\n\n```\nchurn_risk(\"globex\") was NOT derived:\n\n  Rule: churn_risk (clause 0)\n    churn_risk(C) <- high_value(C), engagement_drop(C), renewal_soon(C)\n    Blocker: high_value(\"globex\") - No matching tuples\n```\n\nGlobex is not at churn risk because it's not high-value. It's a startup with a $25K contract, which doesn't meet the enterprise + >$100K threshold. The blocker is specific: the first condition in the rule failed, and here's why. No ambiguity.\n\n---\n\n## The situation changes\n\nThe customer success team runs an intervention. Acme's usage stabilizes.\n\n```iql\n-usage_trend(\"acme\", \"declining\")\n+usage_trend[(\"acme\", \"stable\")]\n```\n\n```iql\n?churn_risk(C)\n```\n\n```\nNo results.\n```\n\nThe churn risk retracted. The engagement_drop condition no longer holds, so the conclusion disappeared automatically. No cleanup logic, no manual flag removal, no stale belief sitting in the agent's memory. The agent's next answer reflects the current state of the world.\n\n---\n\n## Why this matters\n\nThree properties work together here. The rules guarantee that every conclusion follows from specific, inspectable conditions - not from whatever a model generates in a particular context window. The proof tree (`.why` and `.why_not`) makes the reasoning transparent and auditable. And correct retraction means that when facts change, conclusions update - the agent never acts on a belief that's no longer supported by the evidence.\n\nEvery code block on this page runs against a live InputLayer instance. Paste them into the [demo](https://demo.inputlayer.ai) to see the results yourself.",
    "toc": [
      {
        "level": 2,
        "text": "The setup",
        "id": "the-setup"
      },
      {
        "level": 2,
        "text": "The rules",
        "id": "the-rules"
      },
      {
        "level": 2,
        "text": "Who is at risk?",
        "id": "who-is-at-risk"
      },
      {
        "level": 2,
        "text": "Why is Acme flagged?",
        "id": "why-is-acme-flagged"
      },
      {
        "level": 2,
        "text": "Why is Globex NOT flagged?",
        "id": "why-is-globex-not-flagged"
      },
      {
        "level": 2,
        "text": "The situation changes",
        "id": "the-situation-changes"
      },
      {
        "level": 2,
        "text": "Why this matters",
        "id": "why-this-matters"
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
    "content": "\n# InputLayer + All-in-One AI Data Platforms\n\nAll-in-one AI data platforms find data that matches your criteria. InputLayer derives new conclusions from connected facts. They solve different problems, and many teams use both.\n\nHere's the distinction. An AI data platform answers: \"Find the 50 most similar documents matching these filters.\" InputLayer answers: \"Given these facts and these business rules, which enterprise customers are at churn risk - and here's the derivation proof for each one.\"\n\n## What each system does\n\n| Capability | All-in-One AI Data | InputLayer |\n|---|---|---|\n| Vector similarity search | Native, optimized | Native |\n| Metadata filtering | Rich, fast | Via rules and joins |\n| Analytics (aggregation, grouping) | Growing | Via aggregation rules |\n| Derive conclusions from rules | Outside scope | Native |\n| Recursive queries | Outside scope | Native |\n| Update conclusions when facts change | Re-index | Incremental (milliseconds) |\n| Retract stale conclusions automatically | Outside scope | Native |\n| Trace relationships across data sources | Limited | Native |\n| Knowledge graph storage | Some platforms | Native |\n\n## When retrieval alone can't get you the answer\n\nSome questions can't be answered by finding the right document. The answer has to be derived.\n\n```chain\nAI agent asked: \"Which enterprise customers are at risk of churning?\"\n-- needs to combine\nDeclining usage metrics (from analytics)\n-- with\nNegative sentiment in support tickets (from CRM)\n-- with\nUpcoming contract renewals (from billing)\n-- with\nCompetitive mentions in sales calls (from call transcripts)\n=> \"Churn risk\" isn't stored anywhere - it's a conclusion derived from combining facts [highlight]\n```\n\nYou tell InputLayer: \"A customer is at churn risk if they're enterprise, usage declined more than 20%, and renewal is within 90 days.\" The engine evaluates this rule across your customer data and surfaces matches - with a derivation proof for each one showing exactly which facts triggered it.\n\n## What happens when data changes\n\nAI data platforms handle updates by re-indexing vectors and metadata. That works for retrieval.\n\nBut when you have derived conclusions, updates get more interesting.\n\n```steps\nCustomer usage drops below threshold :: InputLayer updates the churn risk assessment immediately [success]\nA fact is retracted :: Everything derived from it disappears automatically [success]\nA rule changes :: All affected conclusions recompute - nothing else does [success]\n```\n\nInputLayer's incremental computation means updates propagate through the rules, recomputing only what's affected. A single fact change in a 2,000-node graph takes 6.83ms. Retractions are correct - derived conclusions only disappear when all supporting derivation paths are removed.\n\n## How teams use them together\n\nThe most common pattern: the AI data platform handles high-throughput similarity queries where raw retrieval speed matters most. InputLayer handles the reasoning queries where answers need to be derived from relationships and rules. Your application routes each query to the right system.\n\nSome teams also use InputLayer's native vector search for queries that need reasoning and similarity combined - like \"find documents similar to X that this user is authorized to see through their org hierarchy.\" One query, one pass, no glue code.\n\n## Getting started\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) gets you running in about 5 minutes. The [data modeling guide](/docs/guides/core-concepts/) explains how to structure your knowledge graph, and the [Python SDK](/docs/guides/python-sdk/) makes integration with your existing data stack straightforward.",
    "toc": [
      {
        "level": 2,
        "text": "What each system does",
        "id": "what-each-system-does"
      },
      {
        "level": 2,
        "text": "When retrieval alone can't get you the answer",
        "id": "when-retrieval-alone-cant-get-you-the-answer"
      },
      {
        "level": 2,
        "text": "What happens when data changes",
        "id": "what-happens-when-data-changes"
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
  },
  {
    "slug": "vs-graph-databases",
    "title": "InputLayer + Graph Databases",
    "competitors": [
      "Graph Databases"
    ],
    "content": "\n# InputLayer + Graph Databases\n\nGraph databases traverse relationships that already exist in your data. InputLayer derives new relationships through rules - and keeps them current as facts change.\n\nThat's the core difference. A graph database answers \"what's connected to what?\" InputLayer answers \"what can we conclude from these facts and rules?\"\n\n## What each system does\n\nGraph databases are built for exploring and traversing stored relationships. They have mature tooling - visual explorers, admin interfaces, clustering. When you need to see the path between two entities or explore a neighborhood of a graph, they're the right tool.\n\nInputLayer evaluates logical rules over facts, computes recursive conclusions, and maintains those conclusions incrementally. When a fact changes, only the affected conclusions update. When a fact is deleted, everything derived from it disappears - but only if no alternative path still supports it.\n\n| Capability | Graph Databases | InputLayer |\n|---|---|---|\n| Traverse stored relationships | Native | Native |\n| Pattern matching on graphs | Native | Native |\n| Derive new relationships from rules | Via procedures | Native |\n| Recursive traversal | Native | Native |\n| Update conclusions when facts change | Recompute per query | Automatic, incremental |\n| Retract derived state when facts are deleted | Recompute per query | Automatic (weight-based) |\n| Vector similarity search | Plugin | Native |\n| Visual exploration tools | Mature | Via API |\n| Clustering | Native | Single-node |\n\n## Where the distinction matters\n\n**Deriving conclusions from multiple sources.** Alice has authority over Charlie through two independent paths: the management chain and a committee membership. A graph database can traverse each path separately. InputLayer expresses both as rules and combines them into a single recursive concept - authority - that the engine evaluates automatically.\n\n```tree\nAuthority from two sources [primary]\n  Management chain (recursive)\n    Alice manages Bob, Bob manages Charlie\n    Therefore Alice has authority over Charlie\n  Committee membership\n    Alice sits on committee overseeing Engineering\n    Therefore Alice has authority over everyone in Engineering\n```\n\nIn InputLayer, you define both sources of authority as rules. The engine derives the combined result. In a graph query language, you can combine patterns in a single query, but without automatic incremental maintenance when the underlying data changes.\n\n**Keeping derived state consistent.** When someone leaves the company, a graph database still has all the edges you stored. You decide what to invalidate and rebuild. InputLayer tracks which conclusions depend on which facts. Remove \"Alice manages Bob\" and every conclusion that was derived through that relationship retracts automatically - but conclusions supported by alternative paths survive.\n\n```steps\nA relationship changes in a graph database :: You decide what to rebuild and invalidate [highlight]\nA fact changes in InputLayer :: Only affected conclusions recompute automatically [success]\nA fact is deleted in InputLayer :: Derived conclusions retract, but only if no alternative path exists [success]\n```\n\n## How teams use them together\n\nThe most natural pattern: your graph database handles interactive exploration and visualization, and InputLayer handles the reasoning-heavy queries.\n\nYour graph database answers: \"Show me the path between these two entities.\" \"What does this part of the graph look like?\" These are the queries where visual tools and interactive exploration add real value.\n\nInputLayer answers: \"Given these rules, who does Alice have authority over?\" \"If I change this supplier's status, which orders are affected?\" These are the queries that need recursive rule evaluation, incremental updates, and correct retraction.\n\nSome teams compute derived relationships in InputLayer and sync the results back to their graph database for visualization. Reasoning power on one side, exploration tools on the other.\n\n## Getting started\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) will get you running. If you're coming from a graph database background, the [data modeling guide](/docs/guides/core-concepts/) explains how InputLayer's fact-and-rule model relates to property graphs, and the [recursion documentation](/docs/guides/recursion/) covers recursive reasoning.",
    "toc": [
      {
        "level": 2,
        "text": "What each system does",
        "id": "what-each-system-does"
      },
      {
        "level": 2,
        "text": "Where the distinction matters",
        "id": "where-the-distinction-matters"
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
  },
  {
    "slug": "vs-vector-databases",
    "title": "InputLayer + Vector Databases",
    "competitors": [
      "Vector Databases"
    ],
    "content": "\n# InputLayer + Vector Databases\n\nYour vector database finds content that looks like your query. InputLayer follows chains of facts to derive conclusions. Most real applications need both.\n\nA shopper asks \"ink for my printer.\" Every cartridge in the catalog is semantically similar to \"printer ink\" - a Canon PG-245 and an Epson 202 are nearly identical in embedding space. But only one fits the printer the shopper bought eight months ago. That connection - purchase history to compatibility matrix to live inventory - is a chain of structured facts, not a similarity match.\n\nInputLayer traces that chain in a single query and keeps the answer current as inventory changes.\n\n## What each system does\n\n| Capability | Vector DBs | InputLayer | Together |\n|---|---|---|---|\n| Find similar content | Native, optimized at scale | Native | Use either or both |\n| Follow chains of relationships | Outside scope | Native | InputLayer handles this |\n| Evaluate rules recursively | Outside scope | Native | InputLayer handles this |\n| Update conclusions when facts change | Re-index | Native (6.83ms per change) | InputLayer handles this |\n| Retract stale conclusions automatically | Outside scope | Native | InputLayer handles this |\n| Enforce access policies in the query | Metadata filters only | Recursive logic | InputLayer handles hierarchies |\n\n## Three things InputLayer handles that vector databases weren't built for\n\n**Following multi-hop chains.** When the answer requires tracing relationships across separate data sources - supply chain dependencies, corporate ownership structures, medication interactions - InputLayer follows the chain automatically. You define the relationships as rules. The engine walks them to any depth.\n\n```chain\nPort disruption reported\n-- which suppliers ship through this port?\nSupplier A, Supplier C [primary]\n-- which components do they provide?\nComponent X, Component Y\n-- which products use those components?\nProduct Alpha, Product Beta [highlight]\n=> InputLayer traced the chain from disruption to affected products\n```\n\n**Evaluating access policies at query time.** When permissions involve hierarchies - managers see their reports' documents, and their reports' reports, and so on - InputLayer resolves the full chain as part of the search. Results come back already filtered for what the user is allowed to see.\n\n```chain\nVP of Engineering searches for \"deployment best practices\"\n-- InputLayer resolves authorization\nWalk org hierarchy: 36 people in reporting chain [primary]\n-- filter documents\n847 documents from authorized authors\n-- rank by similarity\nTop 10 results, all authorized [success]\n=> One query, one pass. Authorization and search together.\n```\n\n**Keeping conclusions current.** When someone changes roles, their permissions update in the next query. When a supplier is reinstated, blocked orders unblock automatically. When a product goes out of stock, recommendations retract in milliseconds. InputLayer tracks which conclusions depend on which facts and updates only what's affected.\n\n## How teams use them together\n\nThe most common pattern: your vector database handles straightforward similarity search, and InputLayer handles queries that need reasoning. \"Find similar documents\" goes to the vector DB. \"Find documents similar to X that this user is authorized to see through their reporting chain\" goes to InputLayer, which combines the authorization logic with vector search in a single pass.\n\nSome teams use both in parallel. Others move their reasoning-heavy queries entirely to InputLayer and keep the vector DB for high-throughput similarity at scale.\n\n## Performance\n\nInputLayer's incremental computation means that when facts change, only affected conclusions recompute. On a 2,000-node graph with 400,000 derived relationships, a single fact change takes 6.83ms - not the 11.3 seconds a full recompute would take. That's 1,652x faster.\n\nFor pure vector similarity at massive scale (billions of vectors), dedicated vector databases are purpose-built and optimized for that specific workload.\n\n## Getting started\n\n```bash\ndocker run -p 8080:8080 ghcr.io/inputlayer/inputlayer\n```\n\nThe [quickstart guide](/docs/guides/quickstart/) takes about 5 minutes. The [vector search documentation](/docs/guides/vectors/) covers how InputLayer's native vector capabilities work, and the [Python SDK](/docs/guides/python-sdk/) makes integration with your existing stack straightforward.",
    "toc": [
      {
        "level": 2,
        "text": "What each system does",
        "id": "what-each-system-does"
      },
      {
        "level": 2,
        "text": "Three things InputLayer handles that vector databases weren't built for",
        "id": "three-things-inputlayer-handles-that-vector-databases-werent-built-for"
      },
      {
        "level": 2,
        "text": "How teams use them together",
        "id": "how-teams-use-them-together"
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

export const customerStories: CustomerStory[] = [
  {
    "slug": "semantic-image-knowledge-graph",
    "title": "Semantic Image Knowledge Graph",
    "industry": "Media",
    "keyMetric": "Millions of images indexed",
    "content": "\n# Semantic Image Knowledge Graph\n\nA European photo company manages one of the largest stock photo libraries on the continent - millions of images. They wanted customers to discover images through the *relationships* between what's in them, not just visual similarity or keyword tags.\n\nThey added InputLayer to combine vector similarity (finding visually similar images) with structural queries (understanding what's *in* the images and how those things relate to each other) in a single search.\n\n## The challenge\n\nStock photo libraries have traditionally relied on manual tagging. This approach has two well-known limitations.\n\n```steps\nTagging is expensive and inconsistent :: Different editors tag the same image differently. Volume makes thorough tagging impossible.\nKeyword search misses conceptual relationships :: A customer searching \"business meeting diversity\" won't find images unless that exact phrase was tagged. The concept exists in the image, but not in the metadata.\n```\n\nThe company had already implemented vector search using image embeddings, which helped with visual similarity. You could find images that *looked like* a reference image. But they wanted to go further - they wanted customers to search by the *concepts and relationships* within images.\n\n## The solution\n\nInputLayer was added alongside their existing image processing pipeline. The pipeline extracts structured information from images using computer vision models - detected objects, scenes, colors, compositions, people attributes. These structured outputs are stored as facts in InputLayer's knowledge graph.\n\nSo for each image, InputLayer knows things like: \"this image contains a person and a laptop and a coffee cup,\" \"the scene is an office,\" \"there's a woman in her 30s who appears to be typing.\" Each of these is a structured fact, not a free-text tag. Because they're structured, the engine can query across them - not just \"find images with a laptop\" but \"find images where multiple people are interacting with a shared object in an office setting.\"\n\n## Combining vector search with structural queries\n\nThe real power comes from combining these two approaches in a single query.\n\n```tree\nQuery: \"images with warm lighting showing collaborative work\" [primary]\n  Vector similarity\n    Finds images with similar visual style (warm lighting, professional) [success]\n  Structural query\n    Finds images with multiple people + shared object (whiteboard, laptop) [success]\n  Combined result\n    Images matching both visual style AND content relationships [primary]\n```\n\nA customer uploads a reference image and asks: \"find images with a similar style that also show people in an office setting.\" The vector similarity captures the visual style and composition, while the structural constraints ensure the content matches. Both conditions are evaluated in a single pass.\n\nMore sophisticated queries can traverse relationships between concepts. A customer searching for \"collaborative work\" might want images showing multiple people interacting with a shared object. This kind of query is impossible with pure vector search because it requires reasoning about the *relationships* between detected entities - not just whether certain objects are present, but how they relate to each other.\n\n## Results\n\n```steps\nVisual style + conceptual content :: Customers can search by both simultaneously [success]\nRelationship-based discovery :: Find images based on how objects and people relate in the scene [primary]\nIncremental updates :: New images immediately queryable, removed images retract cleanly [success]\nMillions of images indexed :: Combined vector and structural queries at scale [primary]\n```\n\nThe incremental computation engine keeps the knowledge graph current as new images are processed. When the vision pipeline extracts structured data from a new image, the facts are added and immediately available for queries. When an image is removed, all its associated facts retract cleanly - no orphaned metadata.\n\n## Key technical insight\n\n```note\ntype: tip\nThe key design decision was treating image understanding as a knowledge graph problem. Vector embeddings capture visual similarity well, but they compress away the structured information about what's in the image - who's doing what, how objects relate to each other. By extracting that structure and storing it as facts, the company made it queryable through rules. A customer searching for \"collaborative work\" gets images where multiple people are interacting with a shared object - a relationship that pure vector search can't express.\n```",
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
    "content": "\n# Warehouse Optimization\n\nA European appliance manufacturer needed sub-50ms routing decisions for their warehouse picking robots. Their existing databases handled inventory tracking and order management well. The problem was the gap between them: when a robot needed the optimal route, application code had to pull from inventory, cross-reference with order priorities, factor in the warehouse layout, and compute a path. That added hundreds of milliseconds per decision.\n\nThey added InputLayer as the reasoning layer. Warehouse layout, inventory positions, and order priorities are ingested as facts. The routing logic is expressed as rules. Result: sub-50ms query latency, down from hundreds of milliseconds.\n\n## The challenge\n\nThe company operates large-scale distribution centers across Europe. Inventory tracking, order management, and warehouse layouts each lived in separate systems. They were good at what they did. The problem was stitching them together for real-time decisions.\n\n```flow\nPicking robot needs route -> Inventory DB [primary] -> Order priority DB -> Layout DB -> Application code stitches it all together [highlight]\n```\n\nFor a warehouse running thousands of picks per hour, hundreds of milliseconds per routing decision adds up fast. A robot waiting 400ms per decision was spending significant time idle.\n\n## The solution\n\nInputLayer was added alongside the existing infrastructure. Warehouse layout, inventory positions, and order priorities are ingested as facts. The routing logic is expressed as rules that the engine evaluates in real time.\n\nThe warehouse layout is encoded as facts: \"Aisle 3 connects to Dock B,\" \"Bin 3-14 contains Item X,\" \"Order 2847 needs Item X from high-priority queue.\" The routing rules compute optimal paths through this graph, taking all factors into account simultaneously.\n\nThe path computation is recursive - the engine explores routes through the warehouse graph in one pass rather than requiring separate calls to different systems.\n\n## Results\n\n```steps\nRouting decisions :: Under 50ms (previously hundreds of ms) [success]\nInventory change propagation :: Only affected routes recompute [primary]\nStale route elimination :: Automatic - no robots sent to empty bins [success]\n```\n\nThe impact was felt almost immediately. Query latency dropped to under 50ms for routing decisions that previously required multiple round-trips between systems.\n\nThe incremental computation turned out to be especially valuable. When someone picks Item X from Bin 3-14, the engine only recalculates routes that need Item X - routes for other items stay the same. When an item is fully picked, all dependent routing decisions update automatically. No need to rebuild the entire routing graph every time inventory changes.\n\n## Key technical insight\n\n```note\ntype: tip\nThe recursive path computation through the warehouse graph is what makes this practical. InputLayer's incremental computation means adding or removing inventory doesn't require recomputing all paths - only the affected routes update. This is what keeps latency under 50ms even as the warehouse state changes continuously.\n```",
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
