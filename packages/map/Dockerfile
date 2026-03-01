FROM archlinux:base-20251019.0.436919

RUN pacman-key --init && \
    pacman-key --populate archlinux && \
    pacman -Sy --noconfirm archlinux-keyring && \
    pacman -Syu --noconfirm arrow qgis nodejs npm

WORKDIR /app

COPY packages/map/dist/ /app/

# Copy the qgis python code into map cli
COPY packages/qgis ./packages/map/qgis

WORKDIR /app/packages/map

ENTRYPOINT [ "node", "/app/index.cjs" ]