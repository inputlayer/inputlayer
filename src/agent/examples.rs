//! Example registry - scripted teaching lessons for the onboarding agent.
//!
//! Each example is a step-by-step lesson that builds a knowledge graph
//! WITH the user. The KG starts empty and grows through the conversation.

/// A single step in a teaching lesson.
#[derive(Debug, Clone)]
pub struct TeachingStep {
    /// What the agent says (3-5 sentences max)
    pub message: &'static str,
    /// The IQL statement for the user to execute
    pub iql: &'static str,
}

/// A teaching example with scripted steps.
#[derive(Debug, Clone)]
pub struct TeachingExample {
    /// Unique identifier (used as KG name)
    pub id: &'static str,
    /// Human-readable name
    pub name: &'static str,
    /// Short description
    pub description: &'static str,
    /// Category for grouping
    pub category: &'static str,
    /// Difficulty level
    pub difficulty: &'static str,
    /// Scripted lesson steps (empty KG -> guided build)
    pub steps: Vec<TeachingStep>,
    /// System prompt for Claude (handles free-form questions between steps)
    pub system_prompt: &'static str,
}

impl TeachingExample {
    /// Build the system prompt for Claude (handles off-script questions).
    pub fn build_system_prompt(&self, kg_context: &str, lesson_done: bool) -> String {
        let ctx = if kg_context.len() > 2000 {
            &kg_context[..2000]
        } else {
            kg_context
        };
        let guidance = if lesson_done {
            "- The lesson is complete. Answer the user's questions about InputLayer, IQL syntax, the Studio GUI, or their current knowledge graph.\n\
             - Use the IQL Reference below and the KG State to give accurate, specific answers.\n\
             - Suggest concrete IQL commands when relevant using ```iql code blocks.\n\
             - Messages may start with [last query: ...] showing the IQL the user just executed. Use this to give specific answers about their query.\n\
             - When the user asks how to \"explain\" a query, they mean provenance (`.why`) not the debug command. `.why ?query(...)` shows the derivation proof tree."
        } else {
            "- If the user asks a question, answer it briefly, then guide them back to the lesson.\n\
             - Use ```iql code blocks for any suggested queries."
        };
        format!(
            "{}\n\n## KG State\n{}\n\n\
             ## Response Rules\n\
             - Keep responses concise (3-5 sentences).\n\
             {}\n\
             - Never use em dashes, use regular dashes instead.\n\n\
             {}",
            self.system_prompt,
            ctx,
            guidance,
            super::IQL_REFERENCE
        )
    }

    /// Get the message for a specific step, formatted with ```iql block.
    pub fn step_message(&self, step_idx: usize) -> Option<String> {
        self.steps
            .get(step_idx)
            .map(|step| format!("{}\n\n```iql\n{}\n```", step.message, step.iql))
    }

    /// Total number of steps.
    pub fn step_count(&self) -> usize {
        self.steps.len()
    }
}

/// Get a teaching example by ID.
pub fn get_example(id: &str) -> Option<&'static TeachingExample> {
    EXAMPLES.iter().find(|e| e.id == id)
}

/// Get all teaching examples.
pub fn all_examples() -> &'static [TeachingExample] {
    &EXAMPLES
}

static EXAMPLES: std::sync::LazyLock<Vec<TeachingExample>> = std::sync::LazyLock::new(|| {
    vec![
        // ── Flights: The Complete InputLayer Tour ──────────────────────
        TeachingExample {
            id: "flights",
            name: "Flight Reachability",
            description: "Build a flight network from scratch. Learn facts, rules, recursion, provenance, and incremental updates.",
            category: "Complete Tour",
            difficulty: "beginner",
            steps: vec![
                TeachingStep {
                    message: "Welcome! Let's build a flight network from scratch and learn InputLayer along the way.\n\nIn InputLayer, data lives as **facts** - simple statements about the world. The `+` means \"add this fact.\" Click the button below to add your first flight.",
                    iql: "+direct_flight(\"new_york\", \"london\", 7.0)",
                },
                TeachingStep {
                    message: "You just created the `direct_flight` relation and inserted one fact into it. Let's add a few more routes to build out our network.",
                    iql: "+direct_flight(\"london\", \"paris\", 1.5)\n+direct_flight(\"paris\", \"tokyo\", 12.0)\n+direct_flight(\"tokyo\", \"sydney\", 9.5)",
                },
                TeachingStep {
                    message: "Now let's ask a question. Queries start with `?` and use **uppercase variables** as placeholders that match any value. `From`, `To`, and `Hours` will be filled in with every matching fact.",
                    iql: "?direct_flight(From, To, Hours)",
                },
                TeachingStep {
                    message: "You can filter by mixing variables with specific values. Let's find all flights departing from London - notice `\"london\"` is fixed in the first position.",
                    iql: "?direct_flight(\"london\", To, Hours)",
                },
                TeachingStep {
                    message: "Here's an interesting question - can you fly from New York to Sydney? Try querying for a direct flight.",
                    iql: "?direct_flight(\"new_york\", \"sydney\", Hours)",
                },
                TeachingStep {
                    message: "No direct flight! But you CAN get there with connections. Let's teach InputLayer to reason about this.\n\nA **rule** uses `<-` (read: \"if\"). This says: you can reach B from A **if** there's a direct flight. The `_` is a wildcard - we don't care about hours.",
                    iql: "+can_reach(A, B) <- direct_flight(A, B, _)",
                },
                TeachingStep {
                    message: "The rule `can_reach` is now a **derived relation** - InputLayer automatically computes its contents from the facts. Let's see what it derived.",
                    iql: "?can_reach(From, To)",
                },
                TeachingStep {
                    message: "Same 4 connections as the direct flights. Now here's where it gets powerful.\n\nThis second rule says: you can reach C from A if you can fly to B, and B can reach C. Notice **can_reach appears on both sides** - this is recursion.",
                    iql: "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
                },
                TeachingStep {
                    message: "Now query `can_reach` again. From just 4 flights, InputLayer has derived **every** reachable city pair by chaining connections automatically.",
                    iql: "?can_reach(From, To)",
                },
                TeachingStep {
                    message: "10 reachable pairs from 4 flights! Remember when there was no direct flight from New York to Sydney? Let's check again.",
                    iql: "?can_reach(\"new_york\", \"sydney\")",
                },
                TeachingStep {
                    message: "You can reach Sydney! But HOW? InputLayer can show the **proof** - the exact chain of reasoning. The `.why` command traces the derivation.",
                    iql: ".why ?can_reach(\"new_york\", \"sydney\")",
                },
                TeachingStep {
                    message: "The derivation shows: NY -> London (direct) -> Paris (direct) -> Tokyo (direct) -> Sydney (direct). Every conclusion has a traceable proof.\n\nNow let's see incremental updates. We'll add alternative routes through Dubai.",
                    iql: "+direct_flight(\"london\", \"dubai\", 7.0)\n+direct_flight(\"dubai\", \"singapore\", 7.5)\n+direct_flight(\"singapore\", \"sydney\", 8.0)",
                },
                TeachingStep {
                    message: "We added 3 flights. **Without re-running anything**, InputLayer has already updated all reachable pairs. Query again and see the new connections that appeared automatically.",
                    iql: "?can_reach(From, To)",
                },
                TeachingStep {
                    message: "Many more connections now! This is **incremental computation** - when facts change, only affected conclusions recompute. On large graphs this is 1000x+ faster than recomputing everything.\n\nNow let's count connections per city using **aggregation**.",
                    iql: "+hub_score(City, count<Dest>) <- can_reach(City, Dest)\n?hub_score(City, Score)",
                },
                TeachingStep {
                    message: "Now let's test retraction. We'll cancel the London-Dubai route and see what happens to the conclusions that depended on it.",
                    iql: "-direct_flight(\"london\", \"dubai\", 7.0)",
                },
                TeachingStep {
                    message: "The route is gone. But can you still reach Sydney from New York? There's an alternative path through Paris-Tokyo. InputLayer only retracts conclusions when ALL supporting paths are removed.",
                    iql: "?can_reach(\"new_york\", \"sydney\")",
                },
                TeachingStep {
                    message: "Still reachable! This is **correct retraction** - the conclusion persists because the Paris-Tokyo-Sydney path still works, even though the Dubai path was removed.\n\nYou've just learned: facts (`+`), rules (`<-`), queries (`?`), recursion, provenance (`.why`), incremental updates, and correct retraction. That's InputLayer.\n\nFeel free to ask me anything or try your own queries!",
                    iql: "?can_reach(From, To)",
                },
            ],
            system_prompt: "You are a teaching assistant for InputLayer. The user is going through a guided lesson about flight reachability. They may ask questions between steps. Answer briefly and guide them back to the lesson. You know the KG contains flight data that the user has been building step by step.",
        },

        // ── Retraction Deep Dive ──────────────────────────────────────
        TeachingExample {
            id: "retraction",
            name: "Correct Retraction",
            description: "The diamond problem - why removing one reason shouldn't remove a conclusion that has other support.",
            category: "Truth Maintenance",
            difficulty: "intermediate",
            steps: vec![
                TeachingStep {
                    message: "Let's explore a subtle but critical problem: **correct retraction**.\n\nImagine a customer who is blocked from purchasing. We'll start by adding some customers.",
                    iql: "+customer(\"alice\", \"premium\")\n+customer(\"bob\", \"standard\")",
                },
                TeachingStep {
                    message: "Now let's add TWO reasons why Alice is blocked - an unpaid bill AND an unverified card. This creates what's called the **diamond problem**.",
                    iql: "+unpaid_bill(\"alice\", 150.00)\n+unverified_card(\"alice\", \"2025-01-10\")",
                },
                TeachingStep {
                    message: "Let's define the blocking rules. Either reason independently blocks a customer.",
                    iql: "+blocked(C) <- unpaid_bill(C, _)\n+blocked(C) <- unverified_card(C, _)",
                },
                TeachingStep {
                    message: "And a purchase rule - only unblocked customers can buy. The `!` means \"NOT\".",
                    iql: "+can_purchase(C) <- customer(C, _), !blocked(C)\n?blocked(C)",
                },
                TeachingStep {
                    message: "Alice is blocked. Bob is not. Now here's the critical question: if Alice pays her bill, should she be unblocked?",
                    iql: "-unpaid_bill(\"alice\", 150.00)\n?blocked(\"alice\")",
                },
                TeachingStep {
                    message: "**Still blocked!** The unverified card still blocks her. InputLayer tracks each derivation path independently - removing one reason only clears the conclusion when no other paths remain. Let's see why she's still blocked.",
                    iql: ".why ?blocked(\"alice\")",
                },
                TeachingStep {
                    message: "The proof shows the unverified card path is still active. Now verify her card and check again.",
                    iql: "-unverified_card(\"alice\", \"2025-01-10\")\n?blocked(\"alice\")",
                },
                TeachingStep {
                    message: "Now she's unblocked! Both blocking paths are gone. Can she purchase now?",
                    iql: "?can_purchase(C)",
                },
                TeachingStep {
                    message: "Both Alice and Bob can purchase. This is **correct retraction** - conclusions only retract when ALL supporting derivation paths are removed, handled automatically by the engine.\n\nFeel free to experiment or ask questions!",
                    iql: "?can_purchase(C)",
                },
            ],
            system_prompt: "You are teaching the diamond problem in truth maintenance. The user is building a customer blocking scenario step by step. Answer questions briefly.",
        },

        // ── Provenance: AI Agent Procurement Audit ─────────────────────
        TeachingExample {
            id: "provenance",
            name: "AI Agent Procurement Audit",
            description: "An AI agent approves purchases. A regulator asks: why was this approved? Show the proof.",
            category: "Explainability",
            difficulty: "intermediate",
            steps: vec![
                TeachingStep {
                    message: "Imagine an AI agent that automatically approves purchase orders. A regulator walks in and asks: \"Why did you approve this $8,500 GPU purchase?\" You need a structured answer, not just logs.\n\nLet's build this system. First, teams with budgets.",
                    iql: "+team(\"alpha\", \"Engineering\", 15000)\n+team(\"beta\", \"Marketing\", 3000)",
                },
                TeachingStep {
                    message: "Now add the vendors your company has pre-approved.",
                    iql: "+approved_vendor(\"acme\", \"tier_1\")\n+approved_vendor(\"techmart\", \"tier_2\")",
                },
                TeachingStep {
                    message: "Here come the purchase orders. Notice Beta is trying to buy $6,000 worth of dev servers on a $3,000 budget.",
                    iql: "+order(\"alpha\", \"acme\", 3200, \"Server rack\")\n+order(\"alpha\", \"techmart\", 8500, \"GPU cluster\")\n+order(\"beta\", \"techmart\", 6000, \"Dev servers\")\n+order(\"beta\", \"acme\", 500, \"Cables\")",
                },
                TeachingStep {
                    message: "Now the approval rule. A purchase is OK if: the vendor is approved AND the amount is within the team's budget. This is a **multi-condition join** - all conditions must hold simultaneously.",
                    iql: "+purchase_ok(T, V, Amt, Item) <- order(T, V, Amt, Item), approved_vendor(V, _), team(T, _, Budget), Amt <= Budget",
                },
                TeachingStep {
                    message: "Let's see which purchases the AI agent would approve.",
                    iql: "?purchase_ok(Team, Vendor, Amount, Item)",
                },
                TeachingStep {
                    message: "Three approved, one denied. But which one was denied and why? Let's add a rule to find denied orders - an order that exists but wasn't approved.",
                    iql: "+order_denied(T, V, Amt, Item) <- order(T, V, Amt, Item), !purchase_ok(T, V, Amt, Item)\n?order_denied(Team, Vendor, Amount, Item)",
                },
                TeachingStep {
                    message: "Beta's $6,000 dev servers order was denied. Now the regulator asks: \"Why was Alpha's GPU cluster approved?\" The `.why` command shows the complete proof chain.",
                    iql: ".why ?purchase_ok(\"alpha\", \"techmart\", 8500, \"GPU cluster\")",
                },
                TeachingStep {
                    message: "The proof tree shows every fact that contributed: the order exists, TechMart is approved, and 8500 <= 15000 (within Engineering's budget). Every link is traceable.\n\nNow: why was Beta's order DENIED?",
                    iql: ".why_not purchase_ok(\"beta\", \"techmart\", 6000, \"Dev servers\")",
                },
                TeachingStep {
                    message: "The blocker is crystal clear: 6000 > 3000 (over budget). Not a vague error - the exact condition that failed.\n\nLet's see what happens when we increase Beta's budget.",
                    iql: "-team(\"beta\", \"Marketing\", 3000)\n+team(\"beta\", \"Marketing\", 10000)",
                },
                TeachingStep {
                    message: "Check denied orders again. Beta's dev servers should now be approved - the budget constraint is satisfied.",
                    iql: "?order_denied(Team, Vendor, Amount, Item)",
                },
                TeachingStep {
                    message: "No more denied orders! The conclusion updated automatically when the budget fact changed. Every approval and denial has a structured, auditable proof. This is how you build AI agents that regulators can trust.\n\nFeel free to experiment!",
                    iql: "?purchase_ok(Team, Vendor, Amount, Item)",
                },
            ],
            system_prompt: "You are teaching provenance through a procurement audit scenario. The user is building the system step by step. Answer questions briefly.",
        },

        // ── Incremental: Company Access Control ──────────────────────
        TeachingExample {
            id: "incremental",
            name: "Company Access Control",
            description: "Build a company's access hierarchy. Watch how one new hire automatically gets all the right permissions.",
            category: "Incremental Updates",
            difficulty: "intermediate",
            steps: vec![
                TeachingStep {
                    message: "You're building access control for a company. Managers grant access to resources, and that access flows down the reporting chain.\n\nLet's start with the org chart.",
                    iql: "+manages(\"alice\", \"bob\")\n+manages(\"alice\", \"diana\")\n+manages(\"bob\", \"charlie\")",
                },
                TeachingStep {
                    message: "Now let's assign some resource access. Alice has access to the company vault, Bob has access to the engineering servers.",
                    iql: "+has_access(\"alice\", \"company_vault\")\n+has_access(\"bob\", \"eng_servers\")\n+has_access(\"diana\", \"sales_data\")",
                },
                TeachingStep {
                    message: "The rule: authority flows transitively down the management chain. If Alice manages Bob and Bob manages Charlie, Alice has authority over Charlie.",
                    iql: "+authority(X, Y) <- manages(X, Y)\n+authority(X, Z) <- authority(X, Y), manages(Y, Z)",
                },
                TeachingStep {
                    message: "The access rule: you can access a resource if you have direct access OR your manager (transitively) has access and granted it down.",
                    iql: "+can_access(P, R) <- has_access(P, R)\n+can_access(P, R) <- manages(Mgr, P), can_access(Mgr, R)",
                },
                TeachingStep {
                    message: "Who has authority over whom?",
                    iql: "?authority(Boss, Report)",
                },
                TeachingStep {
                    message: "Alice has authority over everyone. Now let's see who can access what.",
                    iql: "?can_access(Person, Resource)",
                },
                TeachingStep {
                    message: "Access flows down: Charlie inherits eng_servers from Bob, and company_vault from Alice. Diana gets company_vault from Alice too.\n\nNow here's the key moment. A new hire joins under Diana.",
                    iql: "+manages(\"diana\", \"frank\")",
                },
                TeachingStep {
                    message: "Just ONE fact added. Check what Frank can access now - **without re-running any rules**.",
                    iql: "?can_access(\"frank\", Resource)",
                },
                TeachingStep {
                    message: "Frank automatically has access to sales_data (from Diana) and company_vault (from Alice, through Diana). InputLayer traced the impact of that one new fact through the entire access graph.\n\nNow what happens when someone leaves? Let's remove Bob from the hierarchy.",
                    iql: "-manages(\"alice\", \"bob\")",
                },
                TeachingStep {
                    message: "Check Charlie's access now. Without Bob in the chain, what happens?",
                    iql: "?can_access(\"charlie\", Resource)",
                },
                TeachingStep {
                    message: "Charlie lost access to company_vault (no path to Alice anymore) and eng_servers (Bob is no longer his manager). All dependent conclusions retracted automatically.\n\nThis is **incremental computation** - one fact change ripples through only the affected paths. On a 2,000-person company, this is 1,652x faster than recomputing everything.\n\nTry adding people and resources!",
                    iql: "?can_access(Person, Resource)",
                },
            ],
            system_prompt: "You are teaching incremental computation through a company access control scenario. The user is building the system step by step. Answer questions briefly.",
        },

        // ── Rules + Vectors: Smart Product Recommendations ───────────
        TeachingExample {
            id: "rules_vectors",
            name: "Smart Product Recommendations",
            description: "A customer asks for printer ink. Similarity alone isn't enough - rules add compatibility constraints.",
            category: "Hybrid Reasoning",
            difficulty: "advanced",
            steps: vec![
                TeachingStep {
                    message: "A customer walks into your online store and says: \"I need ink for my printer.\" Similarity ranking finds relevant products, but can't check compatibility. Let's see how rules add that layer.\n\nFirst, let's add products with vector embeddings that capture similarity.",
                    iql: "+product(\"canon_pg245\", \"Canon PG-245 Black\", 14.99, [0.82, 0.15, 0.91])\n+product(\"epson_202\", \"Epson 202 Black\", 12.99, [0.83, 0.14, 0.90])\n+product(\"canon_cl246\", \"Canon CL-246 Color\", 16.99, [0.79, 0.18, 0.88])",
                },
                TeachingStep {
                    message: "Look at those embeddings: Canon PG-245 `[0.82, 0.15, 0.91]` and Epson 202 `[0.83, 0.14, 0.90]`. They're almost identical! A vector search would rank them as interchangeable.\n\nBut they fit DIFFERENT printers. Let's add that knowledge.",
                    iql: "+compatible(\"canon_pg245\", \"canon_mg3620\")\n+compatible(\"canon_cl246\", \"canon_mg3620\")\n+compatible(\"epson_202\", \"epson_et2850\")",
                },
                TeachingStep {
                    message: "Now let's track what printers customers own.",
                    iql: "+owns_printer(\"alice\", \"canon_mg3620\")\n+owns_printer(\"bob\", \"epson_et2850\")",
                },
                TeachingStep {
                    message: "For a recommendation to be valid, the customer needs to be in good standing. Let's add payment history.",
                    iql: "+paid_invoice(\"alice\", \"inv_001\")\n+paid_invoice(\"bob\", \"inv_002\")\n+good_standing(C) <- paid_invoice(C, _)",
                },
                TeachingStep {
                    message: "Now the key rule: a product is recommendable only if the customer is in good standing AND the product is compatible with their printer. This is **multi-hop reasoning** - 3 logical hops.",
                    iql: "+recommendable(C, P, Name, Price) <- good_standing(C), owns_printer(C, Printer), compatible(P, Printer), product(P, Name, Price, _)",
                },
                TeachingStep {
                    message: "What should Alice see? (Remember, she has a Canon printer.)",
                    iql: "?recommendable(\"alice\", Product, Name, Price)",
                },
                TeachingStep {
                    message: "Only Canon inks! The Epson 202 was filtered out despite having nearly identical embeddings. A pure similarity search would have ranked it equally - the compatibility rule is what ensures Alice gets ink that actually fits her printer.\n\nNow check Bob's recommendations.",
                    iql: "?recommendable(\"bob\", Product, Name, Price)",
                },
                TeachingStep {
                    message: "Bob sees only the Epson 202 - the one compatible with his printer. Each customer gets the RIGHT products, not just the SIMILAR ones.\n\nLet's add a new customer with no payment history.",
                    iql: "+owns_printer(\"charlie\", \"canon_mg3620\")",
                },
                TeachingStep {
                    message: "What does Charlie see?",
                    iql: "?recommendable(\"charlie\", Product, Name, Price)",
                },
                TeachingStep {
                    message: "Nothing! Charlie isn't in good standing (no paid invoices). The rules enforced a 3-hop check: payment -> standing -> compatibility -> recommendation. Normally you'd need application code on top of similarity search to enforce these constraints.\n\nThis is **hybrid reasoning**: rules enforce business logic, vectors rank by relevance. One query, zero application code.\n\nTry experimenting!",
                    iql: "?recommendable(Customer, Product, Name, Price)",
                },
            ],
            system_prompt: "You are teaching hybrid reasoning (rules + vectors) through a product recommendation scenario. The user is building the system step by step. Answer questions briefly.",
        },

        // ── Agentic AI: Customer Churn Detection ─────────────────────
        TeachingExample {
            id: "agentic_ai",
            name: "Customer Churn Detection",
            description: "Build an AI agent's churn risk system. When the VP asks 'why did you flag this customer?', show the proof.",
            category: "Agentic AI",
            difficulty: "advanced",
            steps: vec![
                TeachingStep {
                    message: "Your AI customer success agent just flagged Acme Corp as a churn risk. The VP walks over: \"Acme is our biggest account. Why did your system flag them?\"\n\nLet's build this system and answer that question. First, the customer portfolio.",
                    iql: "+customer(\"acme\", \"enterprise\", 150000)\n+customer(\"globex\", \"startup\", 25000)\n+customer(\"initech\", \"enterprise\", 200000)",
                },
                TeachingStep {
                    message: "Now the behavioral signals your agent monitors.",
                    iql: "+usage_trend(\"acme\", \"declining\")\n+usage_trend(\"globex\", \"growing\")\n+usage_trend(\"initech\", \"stable\")",
                },
                TeachingStep {
                    message: "Renewal dates and support load.",
                    iql: "+renewal(\"acme\", \"2026-04-15\")\n+renewal(\"globex\", \"2026-09-01\")\n+renewal(\"initech\", \"2026-12-01\")\n+support_tickets(\"acme\", 12)\n+support_tickets(\"globex\", 2)",
                },
                TeachingStep {
                    message: "Now let's define what makes a customer high-risk. Each condition is its own rule - clear, auditable, individually testable.",
                    iql: "+high_value(C) <- customer(C, \"enterprise\", Amt), Amt > 100000\n+engagement_drop(C) <- usage_trend(C, \"declining\")\n+renewal_soon(C) <- renewal(C, Date), Date < \"2026-06-01\"",
                },
                TeachingStep {
                    message: "Let's check each signal independently. Who is high-value?",
                    iql: "?high_value(C)",
                },
                TeachingStep {
                    message: "Acme and Initech are both high-value. Who has declining usage?",
                    iql: "?engagement_drop(C)",
                },
                TeachingStep {
                    message: "Only Acme. Now the churn risk rule: ALL THREE conditions must hold. High value, declining engagement, and renewal coming up soon.",
                    iql: "+churn_risk(C) <- high_value(C), engagement_drop(C), renewal_soon(C)\n?churn_risk(C)",
                },
                TeachingStep {
                    message: "Only Acme is flagged. Now the VP asks: **\"WHY?\"** This is the moment that matters. InputLayer shows the actual derivation proof - a structured chain of facts and rules, not just a confidence score.",
                    iql: ".why ?churn_risk(\"acme\")",
                },
                TeachingStep {
                    message: "The proof tree shows the complete chain: Acme is enterprise ($150K > $100K threshold), usage is declining, and renewal is April 2026 (before June 2026 cutoff). Three facts, three conditions, one traceable conclusion.\n\nNow the VP asks: \"What about Initech? They're even bigger.\"",
                    iql: ".why_not churn_risk(\"initech\")",
                },
                TeachingStep {
                    message: "The blocker is clear: Initech's usage is \"stable\", not \"declining\". The engagement_drop condition fails. The exact reason, traceable to a specific fact.\n\nNow let's define retention actions that trigger automatically.",
                    iql: "+needs_exec_review(C) <- churn_risk(C), high_value(C)\n+needs_discount(C) <- churn_risk(C), !high_value(C)\n?needs_exec_review(C)",
                },
                TeachingStep {
                    message: "Acme needs executive review because they're both at-risk AND high-value. Now watch what happens when the situation improves - Acme's usage stabilizes.",
                    iql: "-usage_trend(\"acme\", \"declining\")\n+usage_trend(\"acme\", \"stable\")",
                },
                TeachingStep {
                    message: "Check churn risk again.",
                    iql: "?churn_risk(C)",
                },
                TeachingStep {
                    message: "Acme is no longer flagged! The retention action retracted too. When the underlying fact changed, every conclusion that depended on it updated automatically.\n\nThis is how InputLayer powers AI agents: every decision has a proof, every explanation is structured, and every change propagates correctly. Every decision backed by structured, traceable logic.\n\nTry asking me anything!",
                    iql: "?needs_exec_review(C)",
                },
            ],
            system_prompt: "You are teaching AI agent explainability through a customer churn detection scenario. The user is building the system step by step. Answer questions briefly.",
        },
    ]
});
