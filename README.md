# sturdy.sqlite

A robust, highly concurrent, and production-ready SQLite toolkit for Clojure apps.

SQLite is incredibly fast, but its single-writer concurrency model can cause `SQLITE_BUSY` errors in multi-threaded web applications. 
`sturdy.sqlite` solves this by combining HikariCP for concurrent reads with a dedicated `core.async` background queue for writes, pushing SQLite throughput to 50k+ writes per second without lock contention.

## Features
* **Single-Writer Queue:** Batches inserts to eliminate write-lock contention.
* **Optimized Profiles:** Pre-configured PRAGMAs (WAL mode, memory mapping) for `:high-performance`, `:low-resource`, and `:in-memory` workloads.
* **Deadlock Prevention:** Safe multi-statement transactions using `BEGIN IMMEDIATE`.
* **Zero-Downtime Backups:** Online point-in-time snapshots using `VACUUM INTO`.
* **Custom Types:** Seamless conversion for UUIDs (stored as BLOBs), Enums, Paths, and JSON.
* **Testing Harness:** A drop-in macro for isolated, ephemeral test databases.

## Initialization & The System Map

To start the database, use `make-datasource`. 
This initializes the Hikari pool, sets all connection PRAGMAs, and spins up the background batch writer thread.

```clojure
(require '[sturdy.sqlite.core :as db]
         '[sturdy.sqlite.types :as types])

;; 1. Define your custom type parsers
(def b-opts (types/make-builder-opts {:uuid-col? #{"id" "user-id"}
                                      :json-col? #{"metadata"}
                                      :json-parse-fn my-json-parser}))

;; 2. Note the nesting! The factory expects the options wrapped under :builder-opts
(def db-opts {:batch-size 500
              :batch-wait-ms 10
              :builder-opts b-opts})

;; 3. Start the system
(def sys (db/make-datasource "my-app" "./data" :high-performance db-opts))
```

The factory returns a system map containing everything you need to interact with the database:
`{:datasource, :write-fn, :write-async-fn, :migrate-fn, :backup-fn, :close-fn}`

**Crucial shutdown note:** Always call `(:close-fn sys)` when shutting down your application. 
This gracefully flushes the background queue before closing the physical connections.

## How to Interact with the Database

Because SQLite only supports a single concurrent writer, `sturdy.sqlite` provides specific functions for different types of operations. 
Choosing the right one is critical for performance and safety.

### 1. Reads (Use standard `next.jdbc`)
For all `SELECT` queries, bypass the write queue and use standard `next.jdbc` functions directly against the `:datasource`. 
This utilizes the full Hikari connection pool for highly concurrent reads.

**Important:** When reading, pass the unwrapped `b-opts` to `next.jdbc` so it uses your custom column parsers and returns unqualified kebab-case maps

```clojure
(require '[next.jdbc :as jdbc])

;; Pass the unwrapped `b-opts` directly
(jdbc/execute! (:datasource sys) ["SELECT * FROM users"] b-opts)
```

### 2. Homogeneous Writes (Use the Batch Queue)
If you are doing standard single-statement inserts, updates, or deletes, use the batch writer. 
It routes requests through a `core.async` channel to a single worker thread, executing up to 500 operations in a single physical transaction. 
This entirely eliminates `SQLITE_BUSY` contention.

* **:write-fn (Synchronous / Blocking):** Use this when you need the generated ID back, or you need to know if the query failed (e.g., catching a unique constraint violation). 
It blocks the calling thread until the background queue processes the batch.

```clojure
(let [res ((:write-fn sys) ["INSERT INTO users (name) VALUES (?)" "Alice"]
           {:return-keys true})]
  (:last-insert-rowid() (first res)))
```

* **:write-async-fn (Fire-and-Forget):** Use this for high-throughput, non-critical data like logging, metric counters, or rate-limiting. 
It returns immediately and flushes in the background. 
Errors are silently swallowed.
```clojure
((:write-async-fn sys) ["UPDATE metrics SET count = count + 1 WHERE id = ?" 1])
```

### 3. Complex Business Logic (Use `with-immediate-transaction`)
The batch queue only accepts single SQL vectors. 
If you need to perform a **multi-statement ACID transaction** (e.g., Read -> Modify -> Write, or inserting into multiple tables atomically), you must check out a dedicated connection and use `with-immediate-transaction` from `sturdy.sqlite.ops`.

This automatically issues a `BEGIN IMMEDIATE` command. 
Standard `next.jdbc/with-transaction` defaults to deferred locks, which will cause SQLite deadlocks under load.

```clojure
(require '[sturdy.sqlite.ops :as ops])

(ops/with-immediate-transaction [tx (:datasource sys)]
  (let [user (jdbc/execute-one! tx ["SELECT balance FROM accounts WHERE id = ?" 1])]
    (when (> (:balance user) 50)
      (jdbc/execute! tx ["UPDATE accounts SET balance = balance - 50 WHERE id = ?" 1])
      (jdbc/execute! tx ["INSERT INTO ledger (msg) VALUES (?)" "Withdrawal"]))))
```
*Note: Use this sparingly, as it blocks the single SQLite write lock for the duration of the transaction.*

## Backups & Migrations

**Migrations:**
The library uses `ragtime`. 
Put your `.sql` migrations in `resources/migrations` and run them at startup:
```clojure
((:migrate-fn sys) "migrations")
```

**Backups:**
Never copy a live SQLite `.db` file using standard OS tools, as it may result in a corrupt snapshot. 
Use the built-in backup function, which safely streams the database using `VACUUM INTO` without blocking writers. 
It also automatically prunes backups older than 30 days.
```clojure
;; Creates a snapshot in the provided directory (e.g., ./backups/my-app-2026-04-20_12-00-00.db)
((:backup-fn sys) "./backups" {:keep-days 14})
```

## Testing

Use the `with-test-db` macro to spin up a fully isolated, in-memory SQLite database for unit tests. 
It automatically handles the "anchor" connection to prevent the DB from garbage collecting itself, and cleans up the batch worker when the test finishes.

```clojure
(ns my-app.db-test
  (:require [clojure.test :refer [deftest is]]
            [sturdy.sqlite.test :refer [with-test-db]]
            [next.jdbc :as jdbc]))

(def b-opts (types/make-builder-opts {}))
(def db-opts {:builder-opts b-opts
              :classpath-prefix "test-migrations"}) ; Auto-runs migrations!

(deftest account-creation-test
  ;; Destructure the system map right in the binding
  (with-test-db [{:keys [datasource write-fn]} "my-test-db" db-opts]
    
    (write-fn ["INSERT INTO users (name) VALUES (?)" "Bob"])
    
    (let [users (jdbc/execute! datasource ["SELECT * FROM users"] b-opts)]
      (is (= 1 (count users))))))
```


<!-- Local Variables: -->
<!-- fill-column: 10000 -->
<!-- End: -->
