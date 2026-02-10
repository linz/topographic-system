FROM ghcr.io/linz/action-kart:v0.2.1

WORKDIR /app

COPY package.json package-lock.json ./
COPY packages/shared ./packages/shared
COPY packages/data ./packages/data

RUN npm install

WORKDIR /app/packages/data

RUN uv sync --frozen --no-dev

ENTRYPOINT [ "node", "src/index.ts" ]