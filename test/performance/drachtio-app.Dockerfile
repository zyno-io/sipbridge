FROM node:26-alpine
WORKDIR /app
COPY test/performance/package.json test/performance/package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund
COPY test/performance/drachtio-app.cjs ./app.cjs
USER node
ENTRYPOINT ["node", "/app/app.cjs"]
