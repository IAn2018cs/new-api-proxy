FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY index.js upstream-proxy.js access-log.js access-log-worker.js ./

CMD ["node", "index.js"]
