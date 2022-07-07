# KAFKAJS EXAMPLE

This project will show how to use kafkajs and schema-registry

## Run Kafka

You can run kafka on local environment with this command

```sh
docker-compose up -d
```

after you run command docker will start all about kafka container in this picture

![alt text](picture/container.png "Title")

## Start KafkaJs

```sh
npm install
```

Can run producer with

```sh
node producer.js
```

you can see all messages in krafdrop

Can run consume with

```sh
node consumer.js
```

**Note**

Kafka will delete messages after commit 168 H

https://stackoverflow.com/questions/28586008/delete-message-after-consuming-it-in-kafka