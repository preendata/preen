var fs = require('fs');

db.createUser({
    user: 'root',
    pwd: 'thisisnotarealpassword',
    roles: [{
        role: 'readWrite',
        db: 'mongo_db_1'
    }]
});

print("Initialization script is running");

db.createCollection('users');
db.createCollection('transactions');

const users = JSON.parse(fs.readFileSync('/home/data/mock-user-data-1.json', 'utf8'));
const transactions = JSON.parse(fs.readFileSync('/home/data/mock-transaction-data-1.json', 'utf8'));

db.users.insertMany(users);
db.transactions.insertMany(transactions);