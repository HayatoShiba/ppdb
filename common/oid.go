package common

// oid is object id
// in ppdb, this is expected to be used as table identifier
// see https://github.com/postgres/postgres/blob/2f47715cc8649f854b1df28dfc338af9801db217/src/include/postgres_ext.h#L28-L31
type oid uint32

// Relation is table oid
// table information is stored in system catalog (pg_class table)
// the oid is uniquely allocated to each table when created
// the logic to access table is described below
// - get the table oid from pg_class table (the table is specified in sql)
// - identify the file path with table oid
type Relation oid
