# pgcache

A transparent caching proxy for PostgreSQL that caches query results and maintains cache using PostgreSQL logical replication.

## Overview

pgcache acts as a transparent middleware layer between PostgreSQL clients and the origin database. Applications connect to pgcache as if it were a regular PostgreSQL server, and pgcache automatically:

- **Analyzes incoming queries** to determine cacheability
- **Serves cached results** for cacheable queries
- **Passes through non-cacheable queries** directly to the origin database  
- **Maintains cache consistency** using PostgreSQL's logical replication CDC (Change Data Capture)

The proxy is completely transparent to client applications - no code changes required.

## Status

🚧 **Under Active Development**

