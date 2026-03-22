// INSERT
{"operation": "insert", "data": {"username": "alice", "email": "alice@test.com", "age": 28}}

// READ
{"operation": "read", "filter": {"username": "alice"}}

// UPDATE
{"operation": "update", "filter": {"username": "alice"}, "data": {"age": 29}}

// DELETE
{"operation": "delete", "filter": {"username": "alice"}}