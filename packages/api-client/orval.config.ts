import { defineConfig } from 'orval';

export default defineConfig({
  inputlayer: {
    input: {
      target: process.env.OPENAPI_URL || 'http://localhost:8080/api/openapi.json',
    },
    output: {
      mode: 'tags-split',
      target: './src/generated/endpoints',
      schemas: './src/generated/schemas',
      client: 'fetch',
      baseUrl: '/api/v1',
      override: {
        mutator: {
          path: './src/utils/fetcher.ts',
          name: 'customFetch',
        },
      },
    },
    hooks: {
      afterAllFilesWrite: 'npx prettier --write ./src/generated',
    },
  },
  // Zod schemas generation
  inputlayerZod: {
    input: {
      target: process.env.OPENAPI_URL || 'http://localhost:8080/api/openapi.json',
    },
    output: {
      mode: 'single',
      target: './src/generated/zod-schemas.ts',
      client: 'zod',
      override: {
        zod: {
          strict: {
            response: true,
            body: true,
          },
          coerce: {
            param: true,
            query: true,
          },
        },
      },
    },
  },
});
