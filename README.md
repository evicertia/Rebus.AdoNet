# Rebus.AdoNet
Experimental attempt at a Rebus persistence generic provider using Ado.Net 

# Running Unit Tests
In order to run unit-tests locally, a postgres instance is required.
It can be launched using docker, with same parameters as it is launched on appveyor.

* docker run -it --rm -e 'POSTGRES_PASSWORD=Password12!' -e 'POSTGRES_DB=test' -p 5432:5432 postgres:10

# Running Unit Tests (against YugaByteDB)
In order to run unit-tests locally using YugaByteDB, a 2.15+ version is required for full
compatibility (as this version supports FOR UPDATE NOWAIT/SKIP LOCKED), and it should
have been started as:

docker run -it --rm  --name yugabyte -p1070:7000  -p9000:9000 -p5432:5433 -p9042:9042 yugabytedb/yugabyte:2.15.0.0-b11 bin/yugabyted start --daemon=false --tserver_flags="yb_enable_read_committed_isolation=true"

Also a test database has to be pre-created using:

docker exec -it yugabyte bin/ysqlsh -C "CREATE DATABASE test;"

