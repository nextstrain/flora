# flora
DB management, APIs, web portals etc. This project is still experimental.

# prerequisites

* install rethinkDB: https://www.rethinkdb.com/docs/install (on OS-X: `brew update && brew install rethinkdb`)

* you also need `npm`, `node`, `python2.7`, `git`. Homebrew helps!

* clone the submodules: `git submodule update --init --recursive`

* install npm libraries (here & in chateau) `cd chateau && npm i && cd .. && npm i`

* install python libraries: `pip install -r requirements.txt`

# how to run

## locally running rethinkDB server

* start rethinkdb locally: `rethinkdb` (admin data at `http://localhost:8080/`)

* start chateau: `npm run chateau` (accessed via `http://localhost:3000`)


## common commands (to be expanded into a tutorial / documentation)

**these commands are currently only for testing - flora is not ready for production**

Everything is run via `python scripts/run [global arguments] CMD [command arguments]`.
Global arguments include `--debug` to increase logging verbosity, and `--database <str>` to define the database (pathogen) name (_currently defaults to "test", will be required at some point_).
To see command arguments, run `python scripts/run CMD -h`.

* `python scripts/run.py --db mumps --debug createDatabase`
* `python scripts/run.py --db mumps --debug createTables`
* `python scripts/run.py --db mumps --debug clearTables`
* `python scripts/run.py --db mumps --debug upload -f ../sacra/output/piglets.json`
* `python scripts/run.py --db mumps --debug download --dbdump --filename downloaded_data/db.json`
* `python scripts/run.py --db mumps --debug download --filename downloaded_data/data.json`
