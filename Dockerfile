FROM node:20.18.1-alpine AS development

WORKDIR /usr/src/app
COPY package.json ./
COPY yarn.lock ./

ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache g++ make curl python3 py-pip && ln -sf python3 /usr/bin/python


RUN npm update -g yarn npm
RUN yarn global add @nestjs/cli node-gyp node-pre-gyp
RUN yarn install --pure-lockfile \
  && yarn cache clean
COPY . .
RUN yarn run build

FROM node:20.18.1-alpine AS production

ARG NODE_ENV=production
ENV NODE_ENV=${NODE_ENV}

WORKDIR /usr/src/app
COPY package.json ./
COPY yarn.lock ./

ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache g++ make curl python3 py-pip && ln -sf python3 /usr/bin/python

RUN npm update -g yarn npm
RUN yarn global add @nestjs/cli node-gyp node-pre-gyp
RUN yarn install --production --pure-lockfile \
  && yarn cache clean

COPY . .
COPY --from=development /usr/src/app/dist ./dist

CMD ["node", "dist/src/main"]
