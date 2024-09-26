#!/bin/bash

mongosh "mongodb://127.0.0.1:27017/hypha" -u "root" -p "thisisnotarealpassword" --authenticationDatabase "admin" /scripts/create-mongo-user.js
mongosh "mongodb://127.0.0.1:27017/hypha" -u "hypha" -p "thisisnotarealpassword" --authenticationDatabase "hypha" /scripts/insert-document.js