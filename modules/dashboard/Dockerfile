FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package.json .
RUN npm install --production

# Copy application
COPY backend/ backend/
COPY public/ public/

ENV PORT=3000
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV HDFS_NAMENODE=http://namenode:9870

EXPOSE 3000

CMD ["node", "backend/server.js"]
