FROM node:20-slim
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --production
COPY server.js ./
# No public/ copy: the game client is served from Vercel; this server is
# the WebSocket matchmaking + settlement API, not the web host.
EXPOSE 8080
CMD ["node", "server.js"]
