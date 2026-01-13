FROM archlinux:base-20251019.0.436919

RUN pacman-key --init && \
    pacman-key --populate archlinux && \
    pacman -Sy --noconfirm archlinux-keyring && \
    pacman -Syu --noconfirm arrow qgis nodejs npm

WORKDIR /app

COPY package.json package-lock.json ./
COPY packages/shared ./packages/shared
COPY packages/map ./packages/map
# Copy the qgis python code into map cli
COPY packages/qgis ./packages/map/qgis

RUN npm install

WORKDIR /app/packages/map

ENTRYPOINT [ "node", "src/index.ts" ]