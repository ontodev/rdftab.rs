# rdftab.rs: RDF Tables in Rust

This is an early prototype that reads an RDF file and inserts the triples into an SQLite database.

1. download the standalone binary for your platform from the "Assets" section
   of the [Releases](https://github.com/ontodev/rdftab.rs/releases) page.
2. ensure that the binary is executable
3. run `rdftab` with the database you want to use, and the RDFXML input as STDIN
4. query your database with SQLite

```
$ curl -L -o rdftab https://github.com/ontodev/rdftab.rs/releases/download/v0.1.0/rdftab-x86_64-apple-darwin
$ chmod +x rdftab
$ ./rdftab example.db < example.rdf
$ sqlite3 example.db
> select * from statements limit 3;
```

## Motivation

RDF data consists of subject-predicate-object triples that form a graph.
With SPARQL we can perform complex queries over that graph.
With OWLAPI we can interpret that graph as a rich set of logical axioms.
But loading a large RDF graph into OWLAPI or a triplestore for SPARQL
can be slow and require a lot of memory.

In many cases the queries we want to run are actually quite simple.
We often just want all the triples associated with a set of terms,
or all the subjects that match a given predicate and object.
In these cases, SQLite is actually very fast, efficient, and effective.

## Design

TODO

## Build

TODO
