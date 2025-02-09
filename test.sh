#!/usr/bin/expect

spawn ./build/bin/bustub-shell

expect "bustub>"
send "EXPLAIN SELECT * FROM __mock_table_1 ORDER BY colA LIMIT 10;\r"
expect "bustub>"
send "\x03"
expect eof

exec rm test.db test.log
