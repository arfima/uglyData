# Deployment

## Preview mode

Vite has a preview mode that allows you to build the app and serve it locally. To do so, go to `uglyData/frontend` and run:

```bash
npm run preview
```

This will build the app and serve it on [http://localhost:4173](http://localhost:4173).

## Build

To build the app, go to `uglyData/frontend` and run:

```bash
npm run build
```

This will build the app and generate the static files in the `dist` folder.

## Docker

To deploy the app to production, we use Docker alongside Docker Compose. The docker container install the dependencies, build the app and serve it using Nginx.

The Nginx configuration is located in `uglyData/frontend/nginx/nginx.conf`.

Docker compose file - `uglyData/frontend/docker-compose.yml`:

```yaml
version: "3.9"

services:
  mimir:
    build:
      context: ./
      dockerfile: ./Dockerfile
    working_dir: /app
    ports:
      - 80:80

    restart: always
```

Docker file - `uglyData/frontend/Dockerfile`:

```dockerfile
FROM node:18-alpine AS builder

WORKDIR /app

COPY . .


RUN npm install

RUN npm run build

FROM nginx:1.16.0-alpine

COPY --from=builder /app/dist /usr/share/nginx/html

RUN rm /etc/nginx/conf.d/default.conf

COPY ./nginx/nginx.conf /etc/nginx/conf.d

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
```

To deploy, go to `uglyData/frontend` where `docker-compose.yml` is located and run:

```bash
docker-compose up -d
```

This will build the Docker image and start the container.
