FROM ghcr.io/linz/action-kart:v0.1.2

WORKDIR /app

COPY package.json package-lock.json ./
COPY packages/shared ./packages/shared
COPY packages/kart ./packages/kart

WORKDIR /app/packages/kart

