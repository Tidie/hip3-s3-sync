FROM node:18-alpine

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 8001

CMD ["node", "--max-old-space-size=7168", "s3-sync-bot.js"]
