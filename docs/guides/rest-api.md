# REST API Guide

Everything available in the REPL is also available over HTTP.

## Starting the Server

### Option 1: Dedicated Server

```bash
# Start the HTTP server
./target/release/inputlayer-server

# Or with custom port
./target/release/inputlayer-server --port 9090
```

### Option 2: Enable in Configuration

```toml
[http]
enabled = true
host = "127.0.0.1"
port = 8080
```

## Base URL

All API endpoints are prefixed with `/api/v1/`:

```
http://localhost:8080/api/v1/
```

---

## Knowledge Graph Endpoints

### List Knowledge Graphs

```http
GET /api/v1/knowledge-graphs
```

**Response:**
```json
{
  "success": true,
  "data": ["default", "analytics", "users"]
}
```

### Create Knowledge Graph

```http
POST /api/v1/knowledge-graphs
Content-Type: application/json

{
  "name": "my_database"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "name": "my_database",
    "created": true
  }
}
```

### Get Knowledge Graph Details

```http
GET /api/v1/knowledge-graphs/:name
```

**Response:**
```json
{
  "success": true,
  "data": {
    "name": "my_database",
    "relations": ["users", "orders"],
    "rules": ["active_users"],
    "created_at": "2024-01-15T10:30:00Z"
  }
}
```

### Delete Knowledge Graph

```http
DELETE /api/v1/knowledge-graphs/:name
```

**Response:**
```json
{
  "success": true,
  "data": {
    "deleted": "my_database"
  }
}
```

---

## Query Endpoints

### Execute Query

Execute a Datalog query or statement.

```http
POST /api/v1/query/execute
Content-Type: application/json

{
  "knowledge_graph": "default",
  "query": "?user(Id, Name, Age), Age > 25"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "columns": ["Id", "Name", "Age"],
    "rows": [
      [1, "Alice", 30],
      [2, "Bob", 28]
    ],
    "row_count": 2,
    "execution_time_ms": 5
  }
}
```

**Insert Data:**
```json
{
  "knowledge_graph": "default",
  "query": "+user(1, \"Alice\", 30)."
}
```

**Define Rules:**
```json
{
  "knowledge_graph": "default",
  "query": "+senior(Name) <- user(_, Name, Age), Age >= 65"
}
```

### Explain Query

Get the query execution plan without running it.

```http
POST /api/v1/query/explain
Content-Type: application/json

{
  "knowledge_graph": "default",
  "query": "?user(Id, Name, _), orders(Id, Product)"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "plan": "Join(Scan(user), Scan(orders), on: Id)",
    "estimated_cost": 150,
    "optimizations_applied": ["subplan_sharing"]
  }
}
```

---

## Relations Endpoints

### List Relations

```http
GET /api/v1/knowledge-graphs/:kg/relations
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"name": "users", "arity": 3, "row_count": 1000},
    {"name": "orders", "arity": 4, "row_count": 5000}
  ]
}
```

### Get Relation Schema

```http
GET /api/v1/knowledge-graphs/:kg/relations/:name
```

**Response:**
```json
{
  "success": true,
  "data": {
    "name": "users",
    "columns": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"},
      {"name": "age", "type": "int"}
    ],
    "row_count": 1000
  }
}
```

### Get Relation Data

```http
GET /api/v1/knowledge-graphs/:kg/relations/:name/data
GET /api/v1/knowledge-graphs/:kg/relations/:name/data?limit=100&offset=0
```

**Response:**
```json
{
  "success": true,
  "data": {
    "columns": ["id", "name", "age"],
    "rows": [
      [1, "Alice", 30],
      [2, "Bob", 25]
    ],
    "total": 1000,
    "limit": 100,
    "offset": 0
  }
}
```

### Insert Data

```http
POST /api/v1/knowledge-graphs/:kg/relations/:name/data
Content-Type: application/json

{
  "rows": [
    [3, "Charlie", 35],
    [4, "Diana", 28]
  ]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "inserted": 2
  }
}
```

### Delete Data

```http
DELETE /api/v1/knowledge-graphs/:kg/relations/:name/data
Content-Type: application/json

{
  "condition": "age < 18"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "deleted": 5
  }
}
```

---

## Rules Endpoints

### List Rules

```http
GET /api/v1/knowledge-graphs/:kg/rules
```

**Response:**
```json
{
  "success": true,
  "data": [
    {"name": "path", "clauses": 2},
    {"name": "active_users", "clauses": 1}
  ]
}
```

### Get Rule Definition

```http
GET /api/v1/knowledge-graphs/:kg/rules/:name
```

**Response:**
```json
{
  "success": true,
  "data": {
    "name": "path",
    "clauses": [
      "path(X, Y) <- edge(X, Y)",
      "path(X, Z) <- edge(X, Y), path(Y, Z)"
    ]
  }
}
```

### Delete Rule

```http
DELETE /api/v1/knowledge-graphs/:kg/rules/:name
```

### Delete Rule Clause

```http
DELETE /api/v1/knowledge-graphs/:kg/rules/:name/:index
```

Where `:index` is the 1-based clause number.

---

## Views Endpoints

### List Views

```http
GET /api/v1/knowledge-graphs/:kg/views
```

### Get View Schema

```http
GET /api/v1/knowledge-graphs/:kg/views/:name
```

### Get View Data

```http
GET /api/v1/knowledge-graphs/:kg/views/:name/data
```

### Create View

```http
POST /api/v1/knowledge-graphs/:kg/views
Content-Type: application/json

{
  "name": "active_orders",
  "query": "active_orders(UserId, Product) <- orders(UserId, Product, Status), Status = \"active\""
}
```

### Delete View

```http
DELETE /api/v1/knowledge-graphs/:kg/views/:name
```

---

## Admin Endpoints

### Health Check

```http
GET /api/v1/health
```

**Response:**
```json
{
  "status": "healthy",
  "version": "0.1.0"
}
```

### System Statistics

```http
GET /api/v1/stats
```

**Response:**
```json
{
  "success": true,
  "data": {
    "uptime_seconds": 3600,
    "total_queries": 15000,
    "total_inserts": 5000,
    "knowledge_graphs": 3,
    "memory_mb": 256
  }
}
```

---

## Error Responses

All errors return a consistent format:

```json
{
  "success": false,
  "error": {
    "code": "PARSE_ERROR",
    "message": "Invalid query syntax: unexpected token at line 1"
  }
}
```

**Common Error Codes:**

| Code | Description |
|------|-------------|
| `PARSE_ERROR` | Invalid Datalog syntax |
| `NOT_FOUND` | Knowledge graph, relation, or rule not found |
| `TYPE_ERROR` | Type mismatch in data |
| `VALIDATION_ERROR` | Schema validation failed |
| `INTERNAL_ERROR` | Server error |

---

## Client Examples

### cURL

```bash
# Create knowledge graph
curl -X POST http://localhost:8080/api/v1/knowledge-graphs \
  -H "Content-Type: application/json" \
  -d '{"name": "mydb"}'

# Insert data
curl -X POST http://localhost:8080/api/v1/query/execute \
  -H "Content-Type: application/json" \
  -d '{
    "knowledge_graph": "mydb",
    "query": "+user(1, \"Alice\", 30)."
  }'

# Query data
curl -X POST http://localhost:8080/api/v1/query/execute \
  -H "Content-Type: application/json" \
  -d '{
    "knowledge_graph": "mydb",
    "query": "?user(Id, Name, Age)"
  }'
```

### Python

```python
import requests

BASE_URL = "http://localhost:8080/api/v1"

# Execute query
response = requests.post(f"{BASE_URL}/query/execute", json={
    "knowledge_graph": "default",
    "query": "?user(Id, Name, Age), Age > 25"
})

result = response.json()
if result["success"]:
    for row in result["data"]["rows"]:
        print(row)
```

### JavaScript

```javascript
const BASE_URL = "http://localhost:8080/api/v1";

// Execute query
const response = await fetch(`${BASE_URL}/query/execute`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    knowledge_graph: "default",
    query: "?user(Id, Name, Age), Age > 25"
  })
});

const result = await response.json();
if (result.success) {
  console.log(result.data.rows);
}
```

---

## Authentication

When authentication is enabled, include the JWT token in the Authorization header:

```http
Authorization: Bearer <your-jwt-token>
```

See [Configuration Guide](configuration.md) for auth setup.
