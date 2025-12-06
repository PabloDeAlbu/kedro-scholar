## Requirements

* Docker and Docker Compose
* GNU Make
* A valid `.env` file containing at least:

```bash
PROJECT_NAME=dw
```

## Usage

### Start the stack

```bash
make up
```

### Stop the stack

```bash
make stop
```

### Access Postgres container

```bash
make bash
```

### Create a database dump

```bash
make dump
```

Exports the `dw` database with `pg_dump` into the `data/dumps/` directory.
The file is named with the project name and the current date, e.g.:

```
data/dumps/dw-20250926.sql
```

## Notes

* All commands rely on the `COMPOSE_PROJECT_NAME` variable defined in `.env`.
* Dumps are created with the `dw_user` Postgres user. Adjust if your credentials differ.
