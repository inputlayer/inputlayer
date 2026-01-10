# GUI Integration & Documentation Platform Plan

## Executive Summary

This document analyzes the work required to:
1. Integrate the GUI with the InputLayer database server
2. Build REST API endpoints for frontend connectivity
3. Set up VitePress for comprehensive documentation
4. Evaluate Vue migration for admin interface

---

## Part 1: Current State Analysis

### 1.1 GUI Application (Next.js/React)

**Location**: `/gui/`

**Tech Stack**:
- Next.js 16.0.7 + React 19.2.0 + TypeScript 5
- Tailwind CSS 4.1.9 + Radix UI (56 components)
- Zustand 5.0.9 (state management)
- Recharts (visualization)

**Implemented Features** (all with mock data):

| Feature | Status | API Needed |
|---------|--------|------------|
| Connection management | ✅ Complete | `POST /connect`, `GET /health` |
| Database selection | ✅ Complete | `GET /databases`, `GET /databases/:id` |
| Query editor | ✅ Complete | `POST /query/execute` |
| Query history | ✅ Complete | `GET /query/history` |
| Relations explorer | ✅ Complete | `GET /relations`, `GET /relations/:id/data` |
| Views explorer | ✅ Complete | `GET /views`, `GET /views/:id/data` |
| View dependency graph | ✅ Complete | `GET /views/:id/dependencies` |
| Performance metrics | ✅ Complete | `GET /views/:id/performance` |
| Metrics dashboard | ✅ Complete | `GET /stats` |
| Theme toggle | ✅ Complete | N/A (client-side) |
| Settings page | ⚠️ Route only | TBD |
| Database management | ⚠️ Route only | `POST/DELETE /databases` |
| Help page | ⚠️ Route only | N/A |

**Data Models** (from `datalog-store.ts`):
```typescript
DatalogConnection { id, name, host, port, status }
DatalogDatabase { id, name, description, relationsCount, viewsCount }
Relation { id, name, arity, tupleCount, columns, data, isView }
View { id, name, definition, dependencies, computationSteps }
QueryResult { id, query, data, columns, executionTime, timestamp, status }
```

### 1.2 Server Architecture (Current)

**Protocol**: Custom RPC over QUIC+TLS 1.3 (NOT HTTP)

**Existing Services** (16 endpoints):
| Service | Endpoints | GUI Equivalent |
|---------|-----------|----------------|
| DatabaseService | create, drop, list, info, register_view, drop_view, list_views, describe_view | Database mgmt, Views |
| QueryService | query, query_stream, explain | Query editor |
| DataService | insert, delete, bulk_insert, get_schema | Relations |
| AdminService | health, stats, backup, shutdown, clear_caches | Dashboard, Settings |

**Key Files**:
- `src/bin/server.rs` - Main server, RPC registration
- `src/protocol/unified_handler.rs` - All service implementations
- `src/protocol/wire.rs` - Serializable types (WireValue, WireTuple)

**Missing for GUI Integration**:
- ❌ No HTTP/REST endpoints
- ❌ No static file serving
- ❌ No JSON serialization
- ❌ No CORS support
- ❌ No WebSocket for streaming

### 1.3 Documentation Structure

**Current**: 17 markdown files, 5,530 lines

**Organization**:
```
docs/
├── getting-started/     (4 files) - Installation, first program, concepts, REPL
├── tutorials/           (2 files) - Basic queries, recursion
├── guide/              (1 file)  - Data modeling
├── reference/          (2 files) - Syntax, commands
├── internals/          (2 files) - Type system, record syntax
├── troubleshooting/    (1 file)  - Common errors
├── how-it-works/       (empty)   - Placeholder
└── (root files)        (5 files) - Roadmap, features, cheatsheet, etc.
```

**Gaps for Learning Platform**:
| Topic | Current Coverage | Priority |
|-------|-----------------|----------|
| What is Datalog? | None | High |
| RAR/RAG Systems | 1 example only | Critical |
| Query Optimization | Minimal | High |
| Vector/Embeddings | Basic | High |
| Operational Guides | None | Medium |
| API Reference | None | High |

---

## Part 2: Required API Endpoints

### 2.1 REST API Design

Based on GUI requirements, the following endpoints are needed:

#### Connection & Health
```
POST   /api/v1/connect
       Request: { host, port, username?, password?, database? }
       Response: { connectionId, status, token? }

GET    /api/v1/health
       Response: { status, uptime, version }

POST   /api/v1/disconnect
       Response: { success }
```

#### Database Management
```
GET    /api/v1/databases
       Response: { databases: [{ id, name, relationsCount, viewsCount }] }

GET    /api/v1/databases/:name
       Response: { id, name, description, relationsCount, viewsCount, relations, views }

POST   /api/v1/databases
       Request: { name, description? }
       Response: { id, name, created }

DELETE /api/v1/databases/:name
       Response: { success }

POST   /api/v1/databases/:name/select
       Response: { success, relations, views }
```

#### Relations
```
GET    /api/v1/databases/:db/relations
       Response: { relations: [{ id, name, arity, tupleCount, columns }] }

GET    /api/v1/databases/:db/relations/:name
       Response: { id, name, arity, tupleCount, columns, schema }

GET    /api/v1/databases/:db/relations/:name/data
       Query: ?limit=100&offset=0&sort=col&order=asc&filter=...
       Response: { data: [][], totalCount, columns }

GET    /api/v1/databases/:db/relations/:name/schema
       Response: { columns: [{ name, type, constraints }] }
```

#### Views (Computed Relations)
```
GET    /api/v1/databases/:db/views
       Response: { views: [{ id, name, definition, dependencies }] }

GET    /api/v1/databases/:db/views/:name
       Response: { id, name, definition, dependencies, computationSteps }

GET    /api/v1/databases/:db/views/:name/data
       Response: { data: [][], columns }

GET    /api/v1/databases/:db/views/:name/dependencies
       Response: { graph: { nodes: [], edges: [] } }

GET    /api/v1/databases/:db/views/:name/performance
       Response: { totalTime, tuplesProcessed, memoryUsed, steps: [] }

POST   /api/v1/databases/:db/views
       Request: { name, definition }
       Response: { id, name, created }

DELETE /api/v1/databases/:db/views/:name
       Response: { success }
```

#### Query Execution
```
POST   /api/v1/query/execute
       Request: { query, database, timeout? }
       Response: { columns, data, executionTime, status }

POST   /api/v1/query/explain
       Request: { query, database }
       Response: { plan, estimatedCost }

GET    /api/v1/query/history
       Query: ?limit=50&database=...
       Response: { queries: [{ id, query, status, timestamp, executionTime }] }
```

#### Data Operations
```
POST   /api/v1/databases/:db/relations/:name/insert
       Request: { tuples: [[...], [...]] }
       Response: { inserted, duplicates }

POST   /api/v1/databases/:db/relations/:name/delete
       Request: { tuples: [[...], [...]] }
       Response: { deleted }
```

#### Admin/Stats
```
GET    /api/v1/stats
       Response: { relationsCount, viewsCount, queriesRun, avgQueryTime, uptime }

POST   /api/v1/backup
       Response: { backupId, path }

POST   /api/v1/cache/clear
       Response: { success }
```

### 2.2 WebSocket for Streaming (Optional)
```
WS     /api/v1/query/stream
       Send: { query, database }
       Receive: { type: "row", data } | { type: "done", count } | { type: "error", message }
```

---

## Part 3: Architecture Decisions

### 3.1 Vue vs React Decision

**Current State**: GUI is React/Next.js

**Options**:

| Option | Pros | Cons |
|--------|------|------|
| A) Keep React | No rewrite, faster delivery | Tech stack mismatch with VitePress |
| B) Convert to Vue | Consistent with VitePress, modern Composition API | 2-3 week rewrite effort |
| C) Hybrid (React GUI + Vue Admin) | Best of both, incremental | Two frameworks to maintain |

**Recommendation**: Option B (Full Vue conversion) if you want consistency with VitePress, OR Option A if speed to market matters more.

### 3.2 Frontend Architecture (If Vue)

```
/frontend
├── /admin                    # Vue 3 Admin App
│   ├── /src
│   │   ├── /components       # Reusable components
│   │   ├── /views           # Page views
│   │   ├── /stores          # Pinia stores
│   │   ├── /composables     # Composition utilities
│   │   └── /api             # API client
│   ├── vite.config.ts
│   └── package.json
│
└── /docs                     # VitePress Documentation
    ├── /.vitepress
    │   ├── config.ts        # VitePress config
    │   └── theme/           # Custom theme
    ├── /guide               # Learning guides
    ├── /reference           # API reference
    ├── /tutorials           # Step-by-step tutorials
    └── /rar                  # RAR-specific docs
```

### 3.3 Server Architecture Changes

```
src/
├── bin/
│   └── server.rs            # Dual server (RPC + HTTP)
├── protocol/
│   ├── rpc/                 # Existing RPC code
│   └── rest/                # NEW: REST API
│       ├── mod.rs           # HTTP server setup
│       ├── routes.rs        # Route definitions
│       ├── handlers/        # Request handlers
│       │   ├── database.rs
│       │   ├── query.rs
│       │   ├── data.rs
│       │   ├── views.rs
│       │   └── admin.rs
│       ├── middleware/
│       │   ├── cors.rs
│       │   ├── auth.rs
│       │   └── logging.rs
│       └── static_files.rs  # GUI serving
```

### 3.4 Configuration

```toml
# config.toml additions
[http]
enabled = true
port = 8080
host = "0.0.0.0"
cors_origins = ["http://localhost:3000"]

[http.static]
enabled = true
path = "./gui/dist"         # Built GUI files
index = "index.html"

[http.auth]
enabled = false             # Optional auth
```

---

## Part 4: Documentation Platform (VitePress)

### 4.1 Proposed Structure

```
/docs
├── index.md                          # Landing page
├── /learn
│   ├── /datalog                      # Datalog fundamentals
│   │   ├── what-is-datalog.md
│   │   ├── facts-rules-queries.md
│   │   ├── recursion.md
│   │   ├── negation.md
│   │   └── aggregations.md
│   ├── /inputlayer                   # InputLayer specifics
│   │   ├── getting-started.md
│   │   ├── repl-guide.md
│   │   ├── persistent-vs-session.md
│   │   └── multi-database.md
│   └── /rar                          # RAR Learning Path
│       ├── introduction.md
│       ├── knowledge-graphs.md
│       ├── policy-based-access.md
│       ├── vector-similarity.md
│       ├── multi-hop-reasoning.md
│       └── building-rar-system.md
├── /tutorials
│   ├── social-network.md
│   ├── access-control.md
│   ├── graph-analysis.md
│   ├── rar-chatbot.md
│   └── recommendation-engine.md
├── /reference
│   ├── syntax.md
│   ├── commands.md
│   ├── functions.md
│   ├── /api                          # REST API docs
│   │   ├── overview.md
│   │   ├── databases.md
│   │   ├── queries.md
│   │   ├── relations.md
│   │   └── views.md
│   └── configuration.md
├── /operations
│   ├── deployment.md
│   ├── backup-restore.md
│   ├── monitoring.md
│   └── troubleshooting.md
└── /examples
    ├── code-snippets.md
    └── full-applications.md
```

### 4.2 VitePress Features to Enable

- **Search**: Algolia DocSearch or local search
- **Code blocks**: Syntax highlighting for Datalog
- **Live playground**: Embedded query runner (connects to demo server)
- **API documentation**: Auto-generated from OpenAPI spec
- **Version switching**: For different InputLayer versions
- **Dark/Light theme**: Consistent with admin UI

---

## Part 5: Task Breakdown

### Phase 1: Server REST API (P0)

#### TASK-GUI-001: Add HTTP Framework to Server
**Effort**: Medium | **Priority**: P0

**Files to modify**:
- `Cargo.toml` - Add axum, tower, tower-http dependencies
- `src/bin/server.rs` - Dual server startup
- `src/config.rs` - HTTP configuration options

**Requirements**:
1. Add Axum web framework dependency
2. Add tower-http for CORS, compression, static files
3. Update config schema for HTTP settings
4. Create dual-mode server (RPC + HTTP)

---

#### TASK-GUI-002: Implement Core REST Routes
**Effort**: Large | **Priority**: P0

**Files to create**:
- `src/protocol/rest/mod.rs`
- `src/protocol/rest/routes.rs`
- `src/protocol/rest/handlers/database.rs`
- `src/protocol/rest/handlers/query.rs`
- `src/protocol/rest/handlers/data.rs`
- `src/protocol/rest/handlers/views.rs`
- `src/protocol/rest/handlers/admin.rs`

**Endpoints to implement**:
1. `/api/v1/health` - Health check
2. `/api/v1/databases` - List, create, delete databases
3. `/api/v1/databases/:name` - Get database info
4. `/api/v1/query/execute` - Execute query
5. `/api/v1/databases/:db/relations` - List relations
6. `/api/v1/databases/:db/relations/:name/data` - Get relation data
7. `/api/v1/databases/:db/views` - List views
8. `/api/v1/stats` - Server statistics

---

#### TASK-GUI-003: JSON Serialization for Wire Types
**Effort**: Small | **Priority**: P0

**Files to modify**:
- `src/protocol/wire.rs` - Add serde JSON derives
- `src/protocol/rest/types.rs` - Create JSON response types

**Requirements**:
1. Add `serde::Serialize` to WireValue, WireTuple
2. Create JSON-friendly response wrappers
3. Handle type conversions (Vector → JSON array)

---

#### TASK-GUI-004: CORS and Middleware
**Effort**: Small | **Priority**: P0

**Files to create**:
- `src/protocol/rest/middleware/cors.rs`
- `src/protocol/rest/middleware/logging.rs`

**Requirements**:
1. Configure CORS for development (localhost:3000)
2. Add request logging middleware
3. Add error handling middleware

---

#### TASK-GUI-005: Static File Serving
**Effort**: Small | **Priority**: P0

**Files to modify**:
- `src/protocol/rest/static_files.rs`
- `src/config.rs`

**Requirements**:
1. Serve static files from configured directory
2. SPA fallback (serve index.html for unmatched routes)
3. Cache headers for assets

---

### Phase 2: Frontend Decision & Setup (P0)

#### TASK-GUI-006: Decide React vs Vue
**Effort**: Decision | **Priority**: P0

**Decision needed**: Keep React or convert to Vue?

**If keeping React**:
- Update API client in `datalog-store.ts` to call real endpoints
- Build Next.js for static export
- ~2-3 days work

**If converting to Vue**:
- Set up Vue 3 + Vite project
- Port components from React
- ~2-3 weeks work

---

#### TASK-GUI-007: Set Up VitePress
**Effort**: Medium | **Priority**: P1

**Files to create**:
- `docs/.vitepress/config.ts`
- `docs/.vitepress/theme/index.ts`
- `docs/index.md`

**Requirements**:
1. Initialize VitePress project
2. Configure navigation and sidebar
3. Set up custom theme (match admin UI colors)
4. Add Datalog syntax highlighting
5. Configure search

---

### Phase 3: Frontend Implementation (P1)

#### TASK-GUI-008: Implement API Client
**Effort**: Medium | **Priority**: P1

**Files to create/modify**:
- `gui/lib/api-client.ts` (or Vue equivalent)

**Requirements**:
1. Replace mock data with real API calls
2. Handle authentication if enabled
3. Error handling and retries
4. Type-safe request/response

---

#### TASK-GUI-009: Connect Query Editor to API
**Effort**: Small | **Priority**: P1

**Requirements**:
1. Call `POST /api/v1/query/execute`
2. Display real results
3. Handle errors from server
4. Store history via API

---

#### TASK-GUI-010: Connect Relations/Views to API
**Effort**: Medium | **Priority**: P1

**Requirements**:
1. Fetch relations list from API
2. Fetch relation data with pagination
3. Fetch views and their definitions
4. Real dependency graphs from server

---

#### TASK-GUI-011: Build Production Bundle
**Effort**: Small | **Priority**: P1

**Requirements**:
1. Configure production build
2. Optimize bundle size
3. Generate static files for server to serve

---

### Phase 4: Documentation Content (P1)

#### TASK-GUI-012: Migrate Existing Docs to VitePress
**Effort**: Medium | **Priority**: P1

**Requirements**:
1. Move all docs/*.md files to VitePress structure
2. Update internal links
3. Add frontmatter for navigation
4. Test all code examples

---

#### TASK-GUI-013: Write Datalog Fundamentals
**Effort**: Large | **Priority**: P1

**Docs to create**:
- `learn/datalog/what-is-datalog.md`
- `learn/datalog/comparison-to-sql.md`
- `learn/datalog/semantics.md`

---

#### TASK-GUI-014: Write RAR Documentation
**Effort**: Large | **Priority**: P1

**Docs to create**:
- `learn/rar/introduction.md`
- `learn/rar/knowledge-graphs.md`
- `learn/rar/policy-based-access.md`
- `learn/rar/vector-similarity.md`
- `learn/rar/multi-hop-reasoning.md`
- `learn/rar/building-rar-system.md`
- `tutorials/rar-chatbot.md`

---

#### TASK-GUI-015: Write API Reference Documentation
**Effort**: Medium | **Priority**: P1

**Docs to create**:
- `reference/api/overview.md`
- `reference/api/databases.md`
- `reference/api/queries.md`
- `reference/api/relations.md`
- `reference/api/views.md`

---

### Phase 5: Polish & Integration (P2)

#### TASK-GUI-016: Live Playground Component
**Effort**: Large | **Priority**: P2

**Requirements**:
1. Embeddable query runner in docs
2. Connect to demo server
3. Pre-populated examples
4. Syntax highlighting

---

#### TASK-GUI-017: OpenAPI Specification
**Effort**: Medium | **Priority**: P2

**Files to create**:
- `docs/api/openapi.yaml`

**Requirements**:
1. Document all REST endpoints
2. Generate from Rust code if possible
3. Use for API reference docs

---

#### TASK-GUI-018: End-to-End Testing
**Effort**: Medium | **Priority**: P2

**Requirements**:
1. Test GUI against running server
2. Test all API endpoints
3. Test static file serving

---

## Summary

| Phase | Tasks | Effort | Priority |
|-------|-------|--------|----------|
| 1: Server REST API | 5 | ~5-7 days | P0 |
| 2: Frontend Setup | 2 | ~1-2 days (React) or ~2-3 weeks (Vue) | P0 |
| 3: Frontend Connect | 4 | ~3-4 days | P1 |
| 4: Documentation | 4 | ~5-7 days | P1 |
| 5: Polish | 3 | ~3-4 days | P2 |
| **Total** | **18** | **~3-4 weeks** (React) or **~5-6 weeks** (Vue) |

---

## Open Questions

1. **Vue or React?** - Keep existing React GUI or convert to Vue for consistency with VitePress?

2. **Authentication?** - Should the REST API require authentication? Token-based? OAuth?

3. **Demo Server?** - Should docs have a live playground connected to a public demo server?

4. **Monorepo?** - Should GUI, docs, and server be in one repo or separate?

5. **Deployment?** - Where will the GUI be deployed? Embedded in server only, or also standalone?
