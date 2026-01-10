# @inputlayer/api-client

TypeScript API client for InputLayer Datalog Database with runtime validation.

## Installation

```bash
npm install @inputlayer/api-client
```

## Usage

```typescript
import { InputLayerClient } from '@inputlayer/api-client';

// Create client instance
const client = new InputLayerClient({
  baseUrl: '/api/v1', // default
});

// Check server health
const health = await client.admin.health();
console.log(`Server version: ${health.version}`);

// List databases
const { databases } = await client.databases.list();

// Create a database
const db = await client.databases.create({
  name: 'mydb',
  description: 'My database',
});

// Execute a Datalog query
const result = await client.query.execute({
  query: 'person(X, Y)?',
  database: 'mydb',
});

console.log(`Found ${result.rowCount} results in ${result.executionTimeMs}ms`);
```

## API Reference

### `InputLayerClient`

Main client class with namespaced API methods:

- `client.databases` - Database management
- `client.query` - Query execution
- `client.relations` - Relation data access
- `client.views` - View management
- `client.rules` - Rule management
- `client.admin` - Server health and stats

### Database API

```typescript
// List all databases
const { databases, currentDatabase } = await client.databases.list();

// Get database details
const db = await client.databases.get('mydb');

// Create database
const newDb = await client.databases.create({ name: 'newdb' });

// Delete database
await client.databases.delete('mydb');
```

### Query API

```typescript
// Execute query
const result = await client.query.execute({
  query: 'ancestor(X, "alice")?',
  database: 'mydb',
  timeoutMs: 30000, // optional
});

// Explain query plan
const plan = await client.query.explain({
  query: 'ancestor(X, Y)?',
  database: 'mydb',
});
```

### Relations API

```typescript
// List relations
const { relations } = await client.relations.list('mydb');

// Get relation data with pagination
const data = await client.relations.getData('mydb', 'person', {
  offset: 0,
  limit: 100,
});

// Insert data
await client.relations.insertData('mydb', 'person', {
  rows: [
    ['alice', 30],
    ['bob', 25],
  ],
});

// Delete data
await client.relations.deleteData('mydb', 'person', {
  rows: [['alice', 30]],
});
```

### Error Handling

```typescript
import { InputLayerClient, ApiError } from '@inputlayer/api-client';

try {
  await client.databases.get('nonexistent');
} catch (error) {
  if (error instanceof ApiError) {
    console.error(`API Error [${error.code}]: ${error.message}`);
  }
}
```

## Features

- **Type-safe**: Full TypeScript support with detailed type definitions
- **Runtime validation**: Optional Zod schema validation for responses
- **Case transformation**: Automatic snake_case/camelCase conversion
- **Error handling**: Structured error responses with `ApiError` class
- **Zero dependencies**: Only `zod` as a peer dependency

## Development

```bash
# Install dependencies
npm install

# Generate client from OpenAPI spec (requires running server)
npm run generate

# Build
npm run build

# Watch mode
npm run dev
```

## License

Apache-2.0
