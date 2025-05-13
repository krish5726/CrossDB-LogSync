
# CrossDB-LogSync

**CrossDB-LogSync** is a Java-based synchronization system that maintains data consistency across heterogeneous databases — PostgreSQL, MongoDB, and Hive — by using a shared logging mechanism. Each data modification is logged with a timestamp, and consistency is achieved by replaying these logs during merge operations, ensuring the latest update prevails.

## Features

- Supports **set**, **get**, and **merge** operations across:
  - PostgreSQL
  - MongoDB
  - Hive (simulated)
- Tracks all `set` and `get` operations in local log files
- Performs conflict resolution using **timestamp-based synchronization**
- Command-line interface or batch execution mode
- Automatically cleans up log files on exit

## Project Structure

```
com.nosql/
│
├── Main.java              # CLI entry point and orchestrator
├── server.java            # Common interface for all databases
├── logger.java            # Manages log file read/write
├── logobj.java            # Represents a log entry
├── Pair.java              # Utility class to return multiple values
│
├── PostgreSQL.java        # PostgreSQL integration and logic
├── MongoDB.java           # MongoDB integration and logic
├── Hive.java              # Hive simulation (works like a local DB)
```

## How It Works

1. **Write Operation (`set`)**:
   - Applies the update in the specified DB.
   - Logs the operation with a timestamp in a system-specific log file.

2. **Read Operation (`get`)**:
   - Retrieves and displays data from the specified DB.
   - Logs the access (for audit/tracking).

3. **Merge Operation**:
   - Reads another DB’s log file.
   - Compares timestamps.
   - Applies changes if the log entry has a more recent timestamp than the local record.

4. **Exit**:
   - Automatically performs all pairwise merges.
   - Deletes all log files to ensure a clean shutdown.

## Setup

### Prerequisites

- Java 11+
- PostgreSQL running on `localhost:5432` with:
  - Database: `testdb`
  - User: `postgres`
  - Password: `1234`
  - Table: `student_course_grades (studentid, courseid, grade, rollno, emailid, last_modified)`
- MongoDB running on `localhost:27017` with:
  - Database: `testdb`
  - Collection: `student_course_grades`
- Hive: Simulated locally; no external setup required

### Build & Run

1. **Compile**:
   ```bash
   javac -cp ".:mongo-java-driver.jar:postgresql.jar" com/nosql/*.java
   ```

2. **Run CLI Mode**:
   ```bash
   java -cp ".:mongo-java-driver.jar:postgresql.jar" com.nosql.Main
   ```

3. **Run with Batch File**:
   ```bash
   java -cp ".:mongo-java-driver.jar:postgresql.jar" com.nosql.Main commands.txt
   ```

## Sample Commands

```
mongo.set(101, CSE101, A)
postgresql.set(101, CSE101, B)
hive.set(101, CSE101, A+)
mongo.merge(postgresql)
postgresql.merge(hive)
mongo.get(101, CSE101)
exit
```

## Logs

Each DB maintains its own append-only log:
- `mongo.log`
- `postgresql.log`
- `hive.log`

Log entries format:
```
set, student_id, course_id, grade, timestamp
get, student_id, course_id, timestamp
```

Logs are cleaned up on exit.


