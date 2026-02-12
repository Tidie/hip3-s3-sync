FROM node:18

WORKDIR /app

COPY package.json ./

RUN npm install

COPY s3-sync-bot.js .

EXPOSE 8001

CMD ["node", "--max-old-space-size=7168", "s3-sync-bot.js"]
