FROM archlinux:latest

RUN pacman -Syu --noconfirm arrow qgis nodejs npm

WORKDIR /app

COPY . /app

RUN npm install

ENTRYPOINT [ "node", "src/index.ts" ]