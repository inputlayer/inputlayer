FROM node:22-alpine AS builder

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src/ ./src/
RUN npx tsc

FROM node:22-alpine

RUN apk add --no-cache wget

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts
COPY --from=builder /app/dist ./dist
COPY seeds/ ./seeds/

EXPOSE 3000
CMD ["node", "dist/index.js"]
