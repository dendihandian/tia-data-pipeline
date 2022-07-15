db = new Mongo().getDB("tia");
db.createCollection('posts', { capped: false });
db.createCollection('comments', { capped: false });