#!/bin/bash

mongosh "mongodb://127.0.0.1:27017/preen" -u "root" -p "thisisnotarealpassword" --authenticationDatabase "admin" /scripts/create-mongo-user.js
mongosh "mongodb://127.0.0.1:27017/preen" -u "preen" -p "thisisnotarealpassword" --authenticationDatabase "preen" /scripts/insert-document.js