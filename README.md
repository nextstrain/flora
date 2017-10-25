# flora
DB management, APIs, web portals etc. This project is still experimental.

# installation

* install rethinkDB: https://www.rethinkdb.com/docs/install (on OS-X: `brew update && brew install rethinkdb`)

* you also need `npm`, `node`, `python`, `git`. Homebrew helps!

* clone the submodules: `git submodule update --init --recursive`

* install npm libraries (here & in chateau) `cd chateau && npm i && cd .. && npm i`

* install python libraries: `pip install -r requirements.txt`

# how to run

* start rethinkdb locally: `rethinkdb` (admin data at `http://localhost:8080/`)
* start chateau: `npm run chateau` (accessed via `http://localhost:3000`)
