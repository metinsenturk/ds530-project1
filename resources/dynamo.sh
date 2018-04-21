#!/usr/bin/env bash
function dynamo(){
  cd resources/dynamodb_local_latest
  java -Djava.library.path=./DynamoDBLocal_lib/ -jar DynamoDBLocal.jar -port 8000
}
dynamo