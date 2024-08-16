var fs = require('fs');

db.createUser({
    user: "root",
    pwd: "thisisnotarealpassword",
    roles: [
        { role: "readWrite", db: "hyphadb" }
    ]
});

db.grantRolesToUser(
     "root",
     [
       { role: "read", db: "admin" }
     ]
);

db.createCollection('users');
db.createCollection('transactions');
db.createCollection('tenants');

const users = JSON.parse(fs.readFileSync('/home/data/mock-user-data.json', 'utf8'));
const transactions = JSON.parse(fs.readFileSync('/home/data/mock-transaction-data.json', 'utf8'));
const tenants = JSON.parse(fs.readFileSync('/home/data/mock-tenants-data.json', 'utf8'));

db.users.insertMany(users);
db.transactions.insertMany(transactions);
db.tenants.insertMany(tenants);