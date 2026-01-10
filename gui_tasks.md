# GUI Integration Tasks

Based on the comprehensive analysis, here are the tasks to integrate the Vue-based admin dashboard with VitePress documentation into the InputLayer server.

## Architecture Overview

```
InputLayer Server (:8080)
├── /api/v1/*           → REST API (JSON)
├── /auth/*             → Authentication endpoints
├── /app/*              → Vue Admin Dashboard
│   ├── /query          → Query Editor
│   ├── /relations      → Relations Explorer
│   ├── /views          → Views Explorer
│   ├── /metrics        → Metrics Dashboard
│   ├── /admin          → User/Permission Management
│   └── /settings       → Server Settings
└── /docs/*             → VitePress Documentation
    ├── /learn          → Datalog, InputLayer, RAR courses
    ├── /tutorials      → Step-by-step guides
    ├── /reference      → API & syntax reference
    └── [live examples] → Execute against this server
```

---

## Phase 1: Server REST API Foundation (P0)

### TASK-GUI-001: Add HTTP Framework (Axum)
**Priority**: P0 | **Effort**: Medium | **Depends**: None

**Files to modify**:
- `Cargo.toml`
- `src/bin/server.rs`
- `src/config.rs`

**Requirements**:
1. Add dependencies:
   ```toml
   axum = "0.7"
   tower = "0.4"
   tower-http = { version = "0.5", features = ["cors", "fs", "compression-gzip"] }
   serde_json = "1.0"
   jsonwebtoken = "9"
   argon2 = "0.5"  # Password hashing
   ```
2. Create HTTP server alongside RPC server
3. Add config options:
   ```toml
   [http]
   enabled = true
   port = 8080
   host = "0.0.0.0"
   ```

**Acceptance Criteria**:
- [ ] Server starts HTTP on configured port
- [ ] RPC still works on 5433
- [ ] Config file controls HTTP settings

---

### TASK-GUI-002: Implement REST Route Structure
**Priority**: P0 | **Effort**: Medium | **Depends**: TASK-GUI-001

**Files to create**:
```
src/protocol/rest/
├── mod.rs              # Module root, router setup
├── routes.rs           # All route definitions
├── error.rs            # HTTP error types
├── extractors.rs       # Custom Axum extractors
└── response.rs         # Standard response wrappers
```

**Requirements**:
1. Create Axum router with nested routes
2. Standard JSON response format:
   ```json
   { "success": true, "data": {...} }
   { "success": false, "error": { "code": "...", "message": "..." } }
   ```
3. Request ID tracking for debugging

**Acceptance Criteria**:
- [ ] Router structure in place
- [ ] Standard response format working
- [ ] Error handling returns proper JSON

---

### TASK-GUI-003: JSON Serialization for Wire Types
**Priority**: P0 | **Effort**: Small | **Depends**: TASK-GUI-002

**Files to modify**:
- `src/protocol/wire.rs`

**Files to create**:
- `src/protocol/rest/types.rs`

**Requirements**:
1. Add `#[derive(Serialize, Deserialize)]` to WireValue, WireTuple
2. Handle Vector serialization (f64[] → JSON array)
3. Create REST-specific DTOs:
   ```rust
   struct DatabaseResponse { id, name, relations_count, views_count }
   struct RelationResponse { id, name, arity, tuple_count, columns }
   struct QueryResponse { columns, data, execution_time, row_count }
   ```

**Acceptance Criteria**:
- [ ] All wire types serialize to JSON
- [ ] Vector types render as arrays
- [ ] Response DTOs defined

---

### TASK-GUI-004: Database Management Endpoints
**Priority**: P0 | **Effort**: Medium | **Depends**: TASK-GUI-003

**Files to create**:
- `src/protocol/rest/handlers/database.rs`

**Endpoints**:
```
GET    /api/v1/databases              → List all databases
POST   /api/v1/databases              → Create database
GET    /api/v1/databases/:name        → Get database info
DELETE /api/v1/databases/:name        → Drop database
POST   /api/v1/databases/:name/select → Set as current database
```

**Acceptance Criteria**:
- [ ] All CRUD operations work
- [ ] Returns relation/view counts
- [ ] Proper error for non-existent database

---

### TASK-GUI-005: Query Execution Endpoints
**Priority**: P0 | **Effort**: Medium | **Depends**: TASK-GUI-003

**Files to create**:
- `src/protocol/rest/handlers/query.rs`

**Endpoints**:
```
POST   /api/v1/query/execute          → Execute Datalog query
POST   /api/v1/query/explain          → Get execution plan
GET    /api/v1/query/history          → Query history (per session)
```

**Request/Response**:
```json
// POST /api/v1/query/execute
Request:  { "query": "?- edge(X, Y).", "database": "social", "timeout_ms": 5000 }
Response: { "columns": ["X", "Y"], "data": [[1, 2], [2, 3]], "execution_time_ms": 12, "row_count": 2 }
```

**Acceptance Criteria**:
- [ ] Queries execute and return results
- [ ] Errors return helpful messages with line/column
- [ ] Timeout is respected
- [ ] History stored per session

---

### TASK-GUI-006: Relations & Data Endpoints
**Priority**: P0 | **Effort**: Medium | **Depends**: TASK-GUI-003

**Files to create**:
- `src/protocol/rest/handlers/relations.rs`

**Endpoints**:
```
GET    /api/v1/databases/:db/relations              → List relations
GET    /api/v1/databases/:db/relations/:name        → Relation info + schema
GET    /api/v1/databases/:db/relations/:name/data   → Paginated data
POST   /api/v1/databases/:db/relations/:name/insert → Insert tuples
POST   /api/v1/databases/:db/relations/:name/delete → Delete tuples
```

**Query params for data**:
- `limit` (default: 100, max: 1000)
- `offset` (default: 0)
- `sort` (column name)
- `order` (asc/desc)

**Acceptance Criteria**:
- [ ] Pagination works correctly
- [ ] Sorting works on any column
- [ ] Insert/delete return affected count

---

### TASK-GUI-007: Views Endpoints
**Priority**: P0 | **Effort**: Medium | **Depends**: TASK-GUI-003

**Files to create**:
- `src/protocol/rest/handlers/views.rs`

**Endpoints**:
```
GET    /api/v1/databases/:db/views              → List views
GET    /api/v1/databases/:db/views/:name        → View definition + deps
GET    /api/v1/databases/:db/views/:name/data   → Computed view data
POST   /api/v1/databases/:db/views              → Create view (register rule)
DELETE /api/v1/databases/:db/views/:name        → Drop view
```

**View response includes**:
- Definition (Datalog rule text)
- Dependencies (list of relations used)
- Computation steps (for dependency graph)

**Acceptance Criteria**:
- [ ] Views list shows all registered rules
- [ ] View data computes on request
- [ ] Dependencies extracted correctly

---

### TASK-GUI-008: Admin & Stats Endpoints
**Priority**: P0 | **Effort**: Small | **Depends**: TASK-GUI-003

**Files to create**:
- `src/protocol/rest/handlers/admin.rs`

**Endpoints**:
```
GET    /api/v1/health                 → Health check
GET    /api/v1/stats                  → Server statistics
POST   /api/v1/backup                 → Create backup
POST   /api/v1/cache/clear            → Clear caches
```

**Stats response**:
```json
{
  "uptime_seconds": 3600,
  "databases_count": 3,
  "total_relations": 15,
  "total_views": 8,
  "queries_executed": 1234,
  "avg_query_time_ms": 45
}
```

**Acceptance Criteria**:
- [ ] Health returns version and status
- [ ] Stats aggregate across databases
- [ ] Backup creates timestamped file

---

## Phase 2: Authentication System (P0)

### TASK-GUI-009: User & Session Storage
**Priority**: P0 | **Effort**: Medium | **Depends**: TASK-GUI-001

**Files to create**:
- `src/auth/mod.rs`
- `src/auth/user.rs`
- `src/auth/session.rs`
- `src/auth/storage.rs`

**Requirements**:
1. User model:
   ```rust
   struct User {
       id: Uuid,
       username: String,
       password_hash: String,  // Argon2
       role: Role,             // Admin, User, ReadOnly
       created_at: DateTime,
   }
   ```
2. Session model:
   ```rust
   struct Session {
       token: String,          // JWT
       user_id: Uuid,
       expires_at: DateTime,
   }
   ```
3. Store users in dedicated `_auth` database

**Acceptance Criteria**:
- [ ] Users stored persistently
- [ ] Password hashing with Argon2
- [ ] Sessions have configurable expiry

---

### TASK-GUI-010: Authentication Endpoints
**Priority**: P0 | **Effort**: Medium | **Depends**: TASK-GUI-009

**Files to create**:
- `src/protocol/rest/handlers/auth.rs`

**Endpoints**:
```
POST   /api/v1/auth/register          → Create user (admin only)
POST   /api/v1/auth/login             → Login, get JWT
POST   /api/v1/auth/logout            → Invalidate session
GET    /api/v1/auth/me                → Current user info
POST   /api/v1/auth/refresh           → Refresh JWT
```

**JWT payload**:
```json
{
  "sub": "user-uuid",
  "username": "admin",
  "role": "admin",
  "exp": 1234567890
}
```

**Acceptance Criteria**:
- [ ] Login returns JWT
- [ ] JWT validated on protected routes
- [ ] Refresh extends session

---

### TASK-GUI-011: Authorization Middleware
**Priority**: P0 | **Effort**: Medium | **Depends**: TASK-GUI-010

**Files to create**:
- `src/protocol/rest/middleware/auth.rs`

**Requirements**:
1. Extract JWT from `Authorization: Bearer <token>` header
2. Validate token signature and expiry
3. Attach user to request context
4. Role-based access control:
   - `Admin`: All operations
   - `User`: Query, read data, manage own views
   - `ReadOnly`: Query and read only

**Protected routes**:
- All `/api/v1/*` except `/auth/login` and `/health`

**Acceptance Criteria**:
- [ ] Unauthenticated requests rejected
- [ ] Role checked for sensitive operations
- [ ] Clear error messages for auth failures

---

### TASK-GUI-012: User Management Endpoints (Admin)
**Priority**: P1 | **Effort**: Small | **Depends**: TASK-GUI-010

**Endpoints** (Admin only):
```
GET    /api/v1/admin/users            → List users
POST   /api/v1/admin/users            → Create user
GET    /api/v1/admin/users/:id        → Get user
PUT    /api/v1/admin/users/:id        → Update user
DELETE /api/v1/admin/users/:id        → Delete user
PUT    /api/v1/admin/users/:id/role   → Change role
```

**Acceptance Criteria**:
- [ ] Only admins can access
- [ ] Cannot delete self
- [ ] Password changes require confirmation

---

## Phase 3: Vue Frontend Setup (P0)

### TASK-GUI-013: Initialize Vue 3 + Vite Project
**Priority**: P0 | **Effort**: Small | **Depends**: None

**Files to create**:
```
frontend/
├── package.json
├── vite.config.ts
├── tsconfig.json
├── index.html
└── src/
    ├── main.ts
    ├── App.vue
    ├── router/index.ts
    └── stores/index.ts
```

**Tech stack**:
- Vue 3.4+ (Composition API)
- Vite 5
- TypeScript 5
- Pinia (state management)
- Vue Router 4
- VueUse (composables)

**Acceptance Criteria**:
- [ ] `pnpm dev` starts dev server
- [ ] TypeScript configured
- [ ] Router and Pinia set up

---

### TASK-GUI-014: Set Up UI Component Library
**Priority**: P0 | **Effort**: Medium | **Depends**: TASK-GUI-013

**Options** (choose one):
- **Radix Vue** (port of Radix UI) - closest to current
- **PrimeVue** - comprehensive, enterprise-ready
- **Naive UI** - modern, TypeScript-first
- **Custom with Tailwind** - most control

**Components needed**:
- Button, Input, Textarea, Select, Checkbox
- Dialog, Dropdown, Popover, Tooltip
- Table, Tabs, Card, Badge
- Toast notifications
- Theme toggle

**Acceptance Criteria**:
- [ ] Core components available
- [ ] Dark/light theme working
- [ ] Consistent with design system

---

### TASK-GUI-015: Port Authentication UI
**Priority**: P0 | **Effort**: Medium | **Depends**: TASK-GUI-014

**Components to create**:
- `LoginPage.vue` - Login form
- `useAuth.ts` - Auth composable (login, logout, refresh)
- `authStore.ts` - Pinia store for user state
- `AuthGuard.vue` - Route guard component

**Requirements**:
1. Login form with validation
2. Store JWT in httpOnly cookie or localStorage
3. Auto-refresh before expiry
4. Redirect to login on 401

**Acceptance Criteria**:
- [ ] Login flow works end-to-end
- [ ] Token persists across refresh
- [ ] Unauthorized routes redirect

---

### TASK-GUI-016: Port Query Editor
**Priority**: P0 | **Effort**: Large | **Depends**: TASK-GUI-015

**Components to port**:
- `QueryEditorPage.vue`
- `QueryEditorPanel.vue` (with line numbers)
- `QueryResultsPanel.vue`
- `QuerySidebar.vue` (history + snippets)

**Features**:
- Line numbers with scroll sync
- Keyboard shortcuts (Cmd+Enter)
- Real API calls to `/api/v1/query/execute`
- History from API
- Export results (CSV)

**Acceptance Criteria**:
- [ ] Query execution works
- [ ] Results display correctly
- [ ] History persists via API
- [ ] All keyboard shortcuts work

---

### TASK-GUI-017: Port Relations Explorer
**Priority**: P0 | **Effort**: Large | **Depends**: TASK-GUI-015

**Components to port**:
- `RelationsPage.vue`
- `RelationsExplorer.vue` (sidebar)
- `RelationDetailPanel.vue`
- `ViewDetailPanel.vue`
- `ViewGraphTab.vue` (dependency graph)
- `ViewPerformanceTab.vue`

**Features**:
- Real data from API
- Pagination for large relations
- Sorting and filtering
- Canvas-based dependency graph

**Acceptance Criteria**:
- [ ] Relations list from API
- [ ] Data pagination works
- [ ] View graph renders correctly

---

### TASK-GUI-018: Port Metrics Dashboard
**Priority**: P1 | **Effort**: Medium | **Depends**: TASK-GUI-015

**Components to port**:
- `DashboardPage.vue`
- `StatsCard.vue`
- `RecentQueries.vue`

**Features**:
- Stats from `/api/v1/stats`
- Recent queries with live updates
- Connection status

**Acceptance Criteria**:
- [ ] Stats refresh on interval
- [ ] Recent queries update

---

### TASK-GUI-019: Admin User Management UI
**Priority**: P1 | **Effort**: Medium | **Depends**: TASK-GUI-015

**Components to create**:
- `AdminPage.vue`
- `UsersTable.vue`
- `UserFormDialog.vue`
- `RoleSelect.vue`

**Features**:
- List all users
- Create/edit/delete users
- Change roles
- Admin-only access

**Acceptance Criteria**:
- [ ] CRUD for users works
- [ ] Role changes take effect
- [ ] Non-admins cannot access

---

## Phase 4: VitePress Integration (P1)

### TASK-GUI-020: Integrate VitePress into Vue App
**Priority**: P1 | **Effort**: Large | **Depends**: TASK-GUI-013

**Approach**: VitePress as sub-application within Vue dashboard

**Structure**:
```
frontend/
├── src/                    # Vue admin app
└── docs/                   # VitePress docs
    ├── .vitepress/
    │   ├── config.ts
    │   └── theme/
    │       └── index.ts    # Custom theme matching admin
    ├── index.md
    ├── learn/
    ├── tutorials/
    └── reference/
```

**Requirements**:
1. VitePress runs under `/docs` route
2. Shared theme/styles with admin
3. Auth carries over (if logged in)
4. Navigation between admin and docs

**Acceptance Criteria**:
- [ ] `/docs` serves VitePress
- [ ] Theme matches admin
- [ ] Navigation seamless

---

### TASK-GUI-021: Create Datalog Syntax Highlighting
**Priority**: P1 | **Effort**: Small | **Depends**: TASK-GUI-020

**Files to create**:
- `docs/.vitepress/theme/datalog.tmLanguage.json`

**Requirements**:
1. Highlight keywords: `:-`, `?-`, `+`, `-`, `not`, `!`
2. Highlight relations (lowercase)
3. Highlight variables (uppercase)
4. Highlight strings, numbers, vectors
5. Highlight comments (`//`, `/* */`)

**Acceptance Criteria**:
- [ ] Code blocks highlight correctly
- [ ] All syntax elements styled

---

### TASK-GUI-022: Create Live Playground Component
**Priority**: P1 | **Effort**: Large | **Depends**: TASK-GUI-020, TASK-GUI-005

**Files to create**:
- `docs/.vitepress/theme/components/Playground.vue`

**Features**:
1. Embedded query editor in docs
2. "Run" button executes against server
3. Results display inline
4. Pre-populated with example from page
5. Error display with helpful messages
6. Copy result/query buttons

**Usage in markdown**:
```markdown
<Playground>
+edge[(1, 2), (2, 3), (3, 4)].
?- edge(X, Y).
</Playground>
```

**Acceptance Criteria**:
- [ ] Playground renders in docs
- [ ] Queries execute on server
- [ ] Results display correctly
- [ ] Errors shown helpfully

---

### TASK-GUI-023: Migrate Existing Documentation
**Priority**: P1 | **Effort**: Medium | **Depends**: TASK-GUI-020

**Files to migrate**:
- All 17 files from `docs/` to `frontend/docs/`

**Requirements**:
1. Update file paths and links
2. Add VitePress frontmatter
3. Add to navigation config
4. Convert any incompatible syntax
5. Add Playground components to examples

**Acceptance Criteria**:
- [ ] All docs render in VitePress
- [ ] Navigation works
- [ ] Links don't break

---

### TASK-GUI-024: Write Datalog Fundamentals Course
**Priority**: P1 | **Effort**: Large | **Depends**: TASK-GUI-020

**Docs to create**:
```
docs/learn/datalog/
├── index.md              # What is Datalog?
├── history.md            # History and use cases
├── vs-sql.md             # Comparison to SQL
├── facts-rules-queries.md
├── variables-binding.md
├── recursion.md
├── negation.md
├── aggregations.md
└── safety-stratification.md
```

**Each doc includes**:
- Clear explanations
- Live Playground examples
- Practice exercises
- Links to related topics

**Acceptance Criteria**:
- [ ] Complete beginner-friendly course
- [ ] All examples runnable
- [ ] Progressive difficulty

---

### TASK-GUI-025: Write RAR (Retrieval Augmented Reasoning) Course
**Priority**: P1 | **Effort**: Large | **Depends**: TASK-GUI-020

**Docs to create**:
```
docs/learn/rar/
├── index.md              # What is RAR?
├── knowledge-graphs.md   # Building knowledge bases
├── policy-access.md      # Policy-based access control
├── vector-search.md      # Semantic similarity
├── multi-hop.md          # Multi-hop reasoning
├── fact-grounding.md     # Verification & attribution
├── building-system.md    # End-to-end RAR system
└── llm-integration.md    # Integrating with LLMs
```

**Tutorials**:
```
docs/tutorials/
├── rar-chatbot.md        # Build a RAR-powered chatbot
├── rag-vs-rar.md         # RAG to RAR evolution
└── policy-rag.md         # Policy-first retrieval
```

**Acceptance Criteria**:
- [ ] Comprehensive RAR coverage
- [ ] Real-world examples
- [ ] Live playground integration

---

### TASK-GUI-026: Write API Reference Documentation
**Priority**: P1 | **Effort**: Medium | **Depends**: TASK-GUI-020

**Docs to create**:
```
docs/reference/api/
├── index.md              # API overview
├── authentication.md     # Auth endpoints
├── databases.md          # Database management
├── queries.md            # Query execution
├── relations.md          # Relation operations
├── views.md              # View management
├── admin.md              # Admin endpoints
└── errors.md             # Error codes
```

**Each endpoint documented with**:
- HTTP method and path
- Request body (JSON schema)
- Response body (JSON schema)
- Example curl command
- Example response

**Acceptance Criteria**:
- [ ] All endpoints documented
- [ ] Examples are accurate
- [ ] Error codes explained

---

## Phase 5: Static File Serving (P1)

### TASK-GUI-027: Build Frontend for Production
**Priority**: P1 | **Effort**: Small | **Depends**: TASK-GUI-020

**Requirements**:
1. `pnpm build` produces optimized bundle
2. Output to `frontend/dist/`
3. VitePress docs included in build
4. Asset hashing for caching

**Acceptance Criteria**:
- [ ] Production build works
- [ ] Bundle size reasonable (<2MB)
- [ ] All routes work in static build

---

### TASK-GUI-028: Server Static File Middleware
**Priority**: P1 | **Effort**: Small | **Depends**: TASK-GUI-001

**Files to create**:
- `src/protocol/rest/static_files.rs`

**Requirements**:
1. Serve files from configured directory
2. SPA fallback (serve index.html for client routes)
3. Proper MIME types
4. Cache headers for assets (hash-based)
5. Gzip compression

**Config**:
```toml
[http.static]
enabled = true
path = "./frontend/dist"
```

**Acceptance Criteria**:
- [ ] Static files served correctly
- [ ] SPA routing works
- [ ] Compression enabled

---

### TASK-GUI-029: Integrated Build & Deployment
**Priority**: P1 | **Effort**: Medium | **Depends**: TASK-GUI-027, TASK-GUI-028

**Requirements**:
1. Build script compiles frontend
2. Optionally embeds in server binary (rust-embed)
3. Or copies to deployment directory
4. CI/CD pipeline for releases

**Options**:
- **Option A**: Include `dist/` in repo, serve from disk
- **Option B**: Use `rust-embed` to compile into binary
- **Option C**: Docker multi-stage build

**Acceptance Criteria**:
- [ ] Single deployment unit
- [ ] Easy to update frontend
- [ ] Works in Docker

---

## Phase 6: Polish & Testing (P2)

### TASK-GUI-030: CORS Configuration
**Priority**: P2 | **Effort**: Small | **Depends**: TASK-GUI-002

**Files to create**:
- `src/protocol/rest/middleware/cors.rs`

**Requirements**:
1. Configurable allowed origins
2. Support credentials (cookies)
3. Proper preflight handling
4. Development mode allows all

**Acceptance Criteria**:
- [ ] Frontend can call API cross-origin
- [ ] Credentials work

---

### TASK-GUI-031: Request Logging & Monitoring
**Priority**: P2 | **Effort**: Small | **Depends**: TASK-GUI-002

**Files to create**:
- `src/protocol/rest/middleware/logging.rs`

**Requirements**:
1. Log all requests (method, path, status, duration)
2. Request ID for tracing
3. Configurable log level
4. Optional access log file

**Acceptance Criteria**:
- [ ] Requests logged
- [ ] Duration tracked
- [ ] Can correlate errors

---

### TASK-GUI-032: API Rate Limiting
**Priority**: P2 | **Effort**: Medium | **Depends**: TASK-GUI-010

**Files to create**:
- `src/protocol/rest/middleware/rate_limit.rs`

**Requirements**:
1. Per-user rate limits
2. Configurable limits by endpoint
3. 429 response with retry-after
4. Exempt health endpoint

**Acceptance Criteria**:
- [ ] Rate limiting works
- [ ] Limits configurable
- [ ] Clear error response

---

### TASK-GUI-033: End-to-End Tests
**Priority**: P2 | **Effort**: Large | **Depends**: All P0/P1 tasks

**Test coverage**:
1. Auth flow (login, token refresh, logout)
2. Database CRUD
3. Query execution
4. Relation data access
5. View creation and computation
6. Admin user management
7. Static file serving
8. VitePress rendering

**Acceptance Criteria**:
- [ ] E2E tests pass
- [ ] Coverage >80%
- [ ] CI runs tests

---

### TASK-GUI-034: OpenAPI Specification
**Priority**: P2 | **Effort**: Medium | **Depends**: Phase 1

**Files to create**:
- `docs/reference/api/openapi.yaml`

**Requirements**:
1. Document all endpoints
2. Generate from Rust code (utoipa) if possible
3. Serve via Swagger UI at `/api/docs`

**Acceptance Criteria**:
- [ ] Complete OpenAPI spec
- [ ] Swagger UI accessible
- [ ] Examples work

---

## Summary

| Phase | Tasks | Priority | Effort | Dependencies |
|-------|-------|----------|--------|--------------|
| 1: REST API | 8 | P0 | ~6-8 days | None |
| 2: Auth System | 4 | P0 | ~4-5 days | Phase 1 |
| 3: Vue Frontend | 7 | P0/P1 | ~8-10 days | Phase 2 |
| 4: VitePress | 7 | P1 | ~7-10 days | Phase 3 |
| 5: Static Serving | 3 | P1 | ~2-3 days | Phase 4 |
| 6: Polish | 5 | P2 | ~4-5 days | All |
| **Total** | **34** | | **~5-6 weeks** | |

---

## Execution Roadmap

### Week 1-2: Server Foundation
- TASK-GUI-001 through TASK-GUI-011 (REST API + Auth)
- Deliverable: Working API with authentication

### Week 3-4: Vue Frontend
- TASK-GUI-013 through TASK-GUI-019 (Vue app)
- Deliverable: Functional admin dashboard

### Week 5: VitePress Integration
- TASK-GUI-020 through TASK-GUI-023 (VitePress + migration)
- Deliverable: Docs accessible at /docs

### Week 6: Content & Polish
- TASK-GUI-024 through TASK-GUI-026 (Courses + API docs)
- TASK-GUI-027 through TASK-GUI-029 (Production build)
- Deliverable: Complete learning platform

### Ongoing: Polish & Testing
- TASK-GUI-030 through TASK-GUI-034 (Polish)
- Deliverable: Production-ready system

---

## Dependencies Graph

```
TASK-GUI-001 (Axum)
    ├── TASK-GUI-002 (Routes)
    │   ├── TASK-GUI-003 (JSON)
    │   │   ├── TASK-GUI-004 (Database API)
    │   │   ├── TASK-GUI-005 (Query API)
    │   │   ├── TASK-GUI-006 (Relations API)
    │   │   ├── TASK-GUI-007 (Views API)
    │   │   └── TASK-GUI-008 (Admin API)
    │   └── TASK-GUI-030 (CORS)
    └── TASK-GUI-009 (Auth Storage)
        └── TASK-GUI-010 (Auth API)
            ├── TASK-GUI-011 (Auth Middleware)
            └── TASK-GUI-012 (User Mgmt)

TASK-GUI-013 (Vue Init)
    └── TASK-GUI-014 (UI Components)
        └── TASK-GUI-015 (Auth UI)
            ├── TASK-GUI-016 (Query Editor)
            ├── TASK-GUI-017 (Relations)
            ├── TASK-GUI-018 (Dashboard)
            └── TASK-GUI-019 (Admin UI)

TASK-GUI-020 (VitePress)
    ├── TASK-GUI-021 (Syntax)
    ├── TASK-GUI-022 (Playground) ← TASK-GUI-005
    ├── TASK-GUI-023 (Migration)
    ├── TASK-GUI-024 (Datalog Course)
    ├── TASK-GUI-025 (RAR Course)
    └── TASK-GUI-026 (API Docs)

TASK-GUI-027 (Build) + TASK-GUI-028 (Static) → TASK-GUI-029 (Deploy)
```
