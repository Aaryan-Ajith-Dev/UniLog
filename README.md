# UniLog: Data Synchronization Across Heterogeneous Database Systems

## Overview

**UniLog** implements a mathematically grounded, operation-log-based framework for synchronizing distributed data across **MongoDB**, **PostgreSQL**, and **Hive**. It ensures eventual consistency by supporting autonomous operations and log-based merging, while preserving key properties like **commutativity**, **associativity**, and **idempotency**.

---

## Features

* **Autonomous Operation Support:** Independent `GET` and `SET` operations on each system.
* **Logical Timestamping:** Establishes a versioned timeline of operations.
* **Operation Log Format:** Unified schema for storing operation metadata across systems.
* **Conflict Resolution:** Implements **Last-Writer-Wins (LWW)** strategy.
* **Mathematical Soundness:** Merge operations are commutative, associative, and idempotent.
* **Eventual Consistency:** Guarantees convergence across systems under repeated merges.

---

## Architecture

The project is modular, with three independent services handling:

```
UniLog/
├── hive/         # Hive service, timestamp cache, sync interface
├── mongo/        # MongoService class with log tracking
├── postgresql/   # PostgreSQL handlers and log manager
├── dataset/      # Sample data (optional)
├── testcase.in   # Sample SET, GET, MERGE sequences
├── main.py       # Parses test cases and runs operations
└── README.md
```

Each module handles:

* Database connection
* Timestamp tracking
* Operation logging
* Merge conflict resolution
* Table schema and data ops

This modularity ensures minimal changes are needed when integrating a new database system. Typically, only the database-specific handler needs to implement the required core functions using the shared interface, keeping the integration process clean and maintainable.

---

## Core Operations

### `GET`

Reads a value by composite key and logs the operation.

```text
[Timestamp], [System].GET(student_id, course_id)
```

### `SET`

Writes a value and logs the operation with a timestamp.

```text
[Timestamp], [System].SET((student_id, course_id), grade)
```

### `MERGE`

Synchronizes state with another system via its operation log.

```text
[System1].MERGE(System2)
```

---

## Operation Log Format

```json
{
  "timestamp": 1,
  "operation": "SET",
  "table": "student_course_grades",
  "keys": {
    "student_id": "SID101",
    "course_id": "CSE026"
  },
  "item": {
    "grade": "A"
  }
}
```

---

## Mathematical Properties of `MERGE`

* **Commutativity:** `A.merge(B) == B.merge(A)`
* **Associativity:** `(A.merge(B)).merge(C) == A.merge(B.merge(C))`
* **Idempotency:** `A.merge(B) == A.merge(B).merge(B)`
* **Eventual Consistency:** All systems converge under repeated merges

---

## Test Cases

### Test Case 1: Basic GET and SET

```text
1, HIVE.SET((SID103, CSE016), A)
2, HIVE.GET(SID103, CSE016)
```

### Test Case 2: Merge with Conflict

```text
1, HIVE.SET((SID103, CSE016), A)
3, SQL.SET((SID103, CSE016), B)
HIVE.MERGE(SQL)
```

### Test Case 3: Concurrent Writes

```text
1, HIVE.SET((SID101, CSE026), C)
2, MONGO.SET((SID101, CSE026), B)
3, SQL.SET((SID101, CSE026), A)
HIVE.MERGE(MONGO)
HIVE.MERGE(SQL)
```

### Test Case 4: Circular Merges

```text
1, HIVE.SET((SID103, CSE016), A)
3, SQL.SET((SID103, CSE016), B)
5, MONGO.SET((SID103, CSE016), C)
HIVE.MERGE(SQL)
SQL.MERGE(MONGO)
MONGO.MERGE(HIVE)
```

---

## Setup and Execution

### Prerequisites

* Python 3.7+
* MongoDB
* Apache Hive
* PostgreSQL
* Python packages: `pymongo`, `pyhive`, `psycopg2`, `pandas`

### Run

```bash
git clone https://github.com/Aaryan-Ajith-Dev/UniLog.git
cd UniLog
python main.py
```

This will execute the commands from `testcase.in` across all systems.

---

## Contribution
Aaryan Ajith Dev — Implemented the MongoDB integration module and contributed the design for a unified operation log schema, ensuring consistency and simplifying the addition of new database systems through a shared interface.

Sai Venkata Sohith Gutta — Developed the Hive module and implemented the timestamp caching mechanism to maintain operation ordering.

Shreyas S — Implemented the PostgreSQL integration layer, handling synchronization and log-based merge functionality.
