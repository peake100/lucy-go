package lucysql

import _ "embed" // Import embed here so we can embed our .sql files.

//go:embed schema.sql
var sqlSchema string
