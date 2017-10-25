import rethinkdb as r

# https://www.rethinkdb.com/docs/guide/python/

r.connect("localhost", 28015).repl()

# we assume that the "test" database is present (it is by default)
# in reality, this will be the pathogen name, e.g. zika / cholera
dbname = "test"

# create the tables
for table_name, primary_key in {
    "pathogens": "strain",
    "sequences": "accession",
    "geo": "name",
    "access": "entity",
    "authors": "author"
    }.iteritems():
    r.db(dbname).table_create(table_name, primary_key=primary_key).run()


## note, if you want to remove tables:
# r.db(dbname).table_drop("pathogens").run()
# r.db(dbname).table_drop("sequences").run()
# r.db(dbname).table_drop("geo").run()
# r.db(dbname).table_drop("access").run()
# r.db(dbname).table_drop("authors").run()
