FROM ghcr.io/linz/action-kart:v0.2.0

WORKDIR /scripts

# Copy all the kart package files
COPY packages/kartts/ ./
COPY packages/shared/ ../shared/

# Install dependencies and build the project
RUN npm install

WORKDIR /tmp/kart

ENTRYPOINT ["node", "/scripts/src/index.ts"]
