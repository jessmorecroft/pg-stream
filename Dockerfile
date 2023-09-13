FROM node:20-alpine

RUN apk update && apk add python3 make g++

WORKDIR /app
COPY package*.json .
RUN npm install
