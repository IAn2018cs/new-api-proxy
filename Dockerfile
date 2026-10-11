FROM node:20-alpine

WORKDIR /app

COPY package.json ./
COPY index.js access-log.js access-log-worker.js ./

CMD ["node", "index.js"]
