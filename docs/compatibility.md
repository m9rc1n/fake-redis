# Command compatibility matrix

Legend: ✅ full · 🟡 partial (common paths) · 🔌 stub (returns valid but canned reply) · ❌ missing

| Group | Status |
|---|---|
| Strings (GET/SET/INCR/APPEND/GETRANGE/…) | ✅ |
| Keys & TTL (EXPIRE/PERSIST/TTL/PTTL/EXPIREAT/PEXPIREAT + NX/XX/GT/LT) | ✅ |
| Lists (LPUSH/RPUSH/LRANGE/LPOP/RPOP/LMOVE/LPOS/…) | ✅ |
| Hashes (HSET/HGET/HGETALL/HSCAN/HINCRBY/…) | ✅ |
| Sets (SADD/SINTER/SUNION/SDIFF/SMOVE/…) | ✅ |
| Sorted sets (ZADD/ZRANGE BYSCORE/BYLEX/ZRANGESTORE/ZUNION/…) | ✅ |
| Pub/Sub (SUBSCRIBE/PSUBSCRIBE/PUBLISH/PUBSUB) | ✅ |
| Transactions (MULTI/EXEC/DISCARD) | ✅ |
| SCAN / HSCAN / SSCAN / ZSCAN | 🟡 (single-cursor reply) |
| Bit ops (GETBIT/SETBIT/BITCOUNT/BITOP) | 🟡 |
| Geo | 🔌 |
| Streams (XADD/XLEN/XRANGE/XDEL) | 🟡 |
| Scripting (EVAL) | 🔌 |
| Cluster | 🔌 (single-node replies) |
| HyperLogLog | 🟡 (approximated via set) |
| INFO/CLIENT/CONFIG/DEBUG | 🟡 |

Contribute via [command request](../.github/ISSUE_TEMPLATE/command_request.yml).
