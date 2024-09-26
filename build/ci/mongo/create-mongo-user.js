db.createUser({
  user: "hypha",
  pwd: "thisisnotarealpassword",
  roles: [
      { role: "readWrite", db: "hypha" }
  ]
});

db.grantRolesToUser(
   "hypha",
   [
     { role: "read", db: "admin" }
   ]
);