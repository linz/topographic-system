FROM archlinux:latest

RUN pacman -Syu --noconfirm arrow qgis nodejs npm

WORKDIR /app

COPY . /app

ENTRYPOINT [ "node", "src/index.ts" ]