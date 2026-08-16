package mongodb

// Package mongodb types placeholder.
//
// Unlike the elasticsearch/opensearch modules, mongodb has no server-side
// error envelope to decode (no ErrorResponse analogue) and its CRUD methods
// return the mongo-driver's own result types directly, so this file holds no
// shared types today. It exists to mirror the layout of the sibling client
// modules and as a landing point should shared types be introduced later.
