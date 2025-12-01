FROM archlinux:base-20251019.0.436919

RUN pacman -Syu --noconfirm arrow qgis nodejs npm

WORKDIR /app

COPY packages/qgis/ /app/qgis/
COPY packages/map/ /app/

RUN npm install

ENTRYPOINT [ "node", "src/index.ts" ]