db.createUser({
  user: "preen",
  pwd: "thisisnotarealpassword",
  roles: [
      { role: "readWrite", db: "preen" }
  ]
});

db.grantRolesToUser(
   "preen",
   [
     { role: "read", db: "admin" }
   ]
);