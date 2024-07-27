# Mongo

To query mongo locally after docker-compose up, go through all the bullshit to install mongo locally then run `mongosh
mongodb://root:thisisnotarealpassword@localhost:27017/mongo_db_1` to enter the REPL.

`db.users.find()` to print data.

# convert-csv-to-json
A csv to json tool written by and for the mentally disturbed. Find an easier way to do this. Or use this script, but
execute it from this directory (.docker/db/scripts)