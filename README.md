# Rebus.AdoNet
Experimental attempt at a Rebus persistence generic provider using Ado.Net 

# Running Unit Tests
In order to run unit-tests locally, a postgres instance is required.
It can be launched using docker, with same parameters as it is launched on appveyor.

* docker run -it --rm -e 'POSTGRES_PASSWORD=Password12!' -e 'POSTGRES_DB=test' -p 5432:5432 postgres:10
