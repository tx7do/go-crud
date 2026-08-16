package doris

// Package doris types placeholder.
//
// Unlike the elasticsearch/opensearch modules, doris has no server-side error
// envelope to decode (no ErrorResponse analogue) and its CRUD methods delegate
// directly to sqlx, returning sql/database result types. This file holds no
// shared types today; it exists to mirror the layout of the sibling client
// modules and as a landing point should shared types be introduced later.
