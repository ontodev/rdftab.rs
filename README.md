# rdftab.rs: RDF Tables with Rust

`rdftab` reads RDFXML and generates a `statements` table like this:

stanza | subject | predicate          | object                   | value | datatype | language
-------|---------|--------------------|--------------------------|-------|----------|----------
ex:foo | ex:foo  | rdfs:label         |                          | Foo   |          |
ex:foo | ex:foo  | rdfs:label         |                          | Fou   |          | fr
ex:foo | ex:foo  | ex:size            |                          | 123   | xsd:int  |
ex:foo | ex:foo  | ex:link            | <http://example.com/foo> |       |          |
ex:foo | ex:foo  | rdf:type           | owl:Class                |       |          |
ex:foo | ex:foo  | rdfs:subClassOf    | _:b1                     |       |          |
ex:foo | _:b1    | rdf:type           | owl:Restriction          |       |          |
ex:foo | _:b1    | owl:onProperty     | ex:part-of               |       |          |
ex:foo | _:b1    | owl:someValuesFrom | ex:bar                   |       |          |

This is an early prototype that only works with RDFXML input and SQLite databases.
We use the Rust programming language to read and insert as quickly as possible,
using as little memory as possible.

## Usage

1. download the binary for your platform
   from the "Assets" section of the latest release on the
   [Releases](https://github.com/ontodev/rdftab.rs/releases) page.
2. make sure that the binary is executable
3. create a SQLite database file with a [`prefix`](src/prefix.sql) table
4. run `rdftab` with the database you want to use, and the RDFXML input as STDIN
5. query your database with SQLite

```
$ curl -L -o rdftab https://github.com/ontodev/rdftab.rs/releases/download/v0.1.1/rdftab-x86_64-apple-darwin
$ chmod +x rdftab
$ sqlite3 example.db < test/prefix.sql
$ ./rdftab example.db < test/example.owl
$ sqlite3 example.db
> select * from statements limit 3;
```

## Build

If we haven't provided a binary for your platform,
or you want to modify the `rdftab` code,
you can build the code as you would any Rust project:

1. install Rust tools: [`rustup`](https://rustup.rs)
2. clone this repository: `git clone https://github.com/ontodev/rdftab.rs && cd rdftab.rs`
3. run [`cargo build`](https://doc.rust-lang.org/cargo/guide/working-on-an-existing-project.html)

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
Better yet, you can use SQLite from the command line
or pretty much any programming language.

## Examples

<table>
  <tr>
    <th>Task</th>
    <th>SQL</th>
    <th>SPARQL</th>
  </tr>

  <tr>
    <td>Get subjects with labels</td>
    <td>
      <pre lang="sql">SELECT subject, value AS label
FROM statements
WHERE predicate = "rdfs:label";</pre>
    </td>
    <td>
      <pre lang="sparql">SELECT ?subject, ?label
WHERE {
  ?subject rdfs:label ?label .
}</pre>
    </td>
  </tr>

  <tr>
    <td>Get OWL classes with labels</td>
    <td>
      <pre lang="sql">SELECT s1.subject, s2.value AS label
FROM statements s1
JOIN statements s2 ON s2.subject = s1.subject
WHERE s1.predicate = "rdf:type"
  AND s1.object = "owl:Class"
  AND s2.predicate = "rdfs:label";</pre>
    </td>
    <td>
      <pre lang="sparql">SELECT ?subject, ?label
WHERE {
  ?subject
    rdf:type owl:Class ;
    rdfs:label ?label .
}</pre>
    </td>
  </tr>

  <tr>
    <td>Get all triples for a subject, including nested anonymous structures such as OWL class expressions and OWL annotation axioms</td>
    <td>
      <pre lang="sql">SELECT *
FROM statements
WHERE stanza = "ex:foo";</pre>
    </td>
    <td>
    Annoying...
    </td>
  </tr>
</table>


## Design

If you've worked with RDF before,
all of these columns in the example above should be familiar,
except for `stanza`.
We'll discuss stanzas in a moment.

In each of these columns, values are encoded pretty much as you would in Turtle syntax:

- IRIs (URLs) are wrapped in angle brackets: `<http://example.com/foo>`
- prefixed names use a prefix from the `prefix` table: `ex:foo`
- blank nodes start with `_:`: `_:b1234`

Some differences from Turtle syntax:

- literals are multiline strings, without enclosing quotations marks or escaping
- language tags do not include an `@`

This means it's quite simple to convert this table to Turtle format.
As a first pass:

```sql
SELECT
  "@prefix " || prefix || ": <" || base || "> ."
FROM prefix
UNION ALL
SELECT 
   subject
|| " "
|| predicate
|| " "
|| coalesce(
     object,
     """" || value || """^^" || datatype,
     """" || value || """@" || language,
     """" || value || """"
   )
|| " ."
FROM statements;
```

The [`src/turtle.sql`](src/turtle.sql) file is a more complete example,
with better escaping of special characters.

### Objects

We use four columns to encode RDF objects, which fall into four types:

1. IRI: use the `object` column; `value`, `datatype`, and `language` are NULL
2. Plain literal: use the `value` column; `object`, `datatype`, and `language` are NULL
3. Typed literal: use the `value` and `datatype` columns; `object` and `language` are NULL
4. Langage tagged literal: use the `value` and `language` columns; `object` and `datatype` are NULL

### Prefixes

While any IRI can be wrapped in angle brackets,
it's much easier for people to read prefixed names.
When reading RDFXML `rdftab` uses a `prefix` table from your SQLite database,
and tries to convert each IRI it encounters into a prefixes name.
[`src/prefix.sql`](src/prefix.sql) provides an example.

Some warnings:

- Since SQL simply compares strings, not expanded IRIs,
  it's your job to ensure that your prefixes are consistent across your data.
- Turtle prefixed names are a superset of XML QNames and a subset of CURIEs.
  `rdftab`'s prefix handling is currently very primitive.
  Depending on your choices of prefixes and the IRIs in your RDF,
  `rdftab` may generate prefixed names that are not valid in Turtle.

### Stanzas

The RDF graph structure is exceedingly simple.
To encode data with more structure than a simple triple,
we usually construct some sort of tree using blank nodes as subjects.
To encode an OWL class expression "rdfs:subClassOf (ex:part-of some ex:bar)"
we use a little tree like this:

```ttl
ex:foo rdfs:subClassOf _:b1 .
_:b1 rdf:type owl:Restriction .
_:b1 owl:onProperty ex:part-of .
_:b1 owl:someValuesFrom ex:bar .
```

When we want to query for all the information about `ex:foo`,
we can't simply ask for all the subjects matching `ex:foo`.
We also have to query for `_:b1`.
In general, we have to recurse through these trees of blank nodes.

Turtle provides some "syntactic sugar" for nested anonymous structures,
and Turtle processors also group together all the triples about a given subject:

```ttl
ex:foo
  rdfs:label "Foo", "Fou"@fr ;
  ex:size "123"^^xsd:int ;
  ex:link <http://example.com/foo> ;
  rdf:type owl:Class ;
  rdfs:subClassOf [
    rdf:type owl:Restriction ;
    owl:onProperty ex:part-of ;
    owl:someValuesFrom ex:bar
  ] .
```

When we ask for all the information about `ex:foo` this is what we want!
In the [Turtle grammar](https://www.w3.org/TR/turtle/#sec-grammar-grammar)
this is just called `triples`,
but we call it a "stanza".
RDFXML has a similar stanza structure,
where each child element of the root element is a tree
specifying a particular subject,
and various nested anonymous structures are encoded in the XML tree structure.

```xml
<owl:Class rdf:about="http://example.com/foo">
  <rdfs:label>Foo</rdfs:label>
  <rdfs:label xml:lang="fr">Fou</rdfs:label>
  <ex:size rdf:datatype="http://www.w3.org/2001/XMLSchema#int">123</ex:size>
  <ex:link rdf:resource="http://example.com/foo"/>
  <rdfs:subClassOf>
    <owl:Restriction>
      <owl:onProperty rdf:resource="http://example.com/part-of"/>
      <owl:someValuesFrom rdf:resource="http://example.com/bar"/>
    </owl:Restriction>
  </rdfs:subClassOf>
</owl:Class>
```

(See [example.owl](test/examle.owl).)

To encode stanza information in the `statements` table,
`rdftab` uses a [slightly modified version](https://github.com/ontodev/rio)
of [`rio`](https://github.com/oxigraph/rio)
that emits a special triple when a child element of the RDFXML root element is closed.
This information is used to associate the "top-level subject"
with all the triples that came out of that element.
We put that top-level subject in the `stanza` column.

Looking back to our main example,
you can see that the subjects `ex:foo` and `_:b1` both the same stanza `ex:foo`.
Now when we query SQLite for `stanza = "ex:foo"`
we will get all the triples for the subject `ex:foo`
**and** all of the nested anonymous structures.

Note that the `stanza` column is usually a named subject,
but there are also cases where the top-level subject is a blank node.

### OWL Annotation Axioms

OWL Annotation Axioms provide a way to make statements about other statements in the RDF graph.
For example, we can add a comment on a label:

```ttl
ex:foo rdfs:label "Foo" .
[ rdf:type owl:Axiom ;
  owl:annotatedSource ex:foo ;
  owl:annotatedProperty ex:label ;
  owl:annotatedTarget "Foo" ;
  rdfs:comment "A silly label"
] .
```

The top-level subject for the OWL Annotation Axiom is a blank node.
However when we query for `ex:foo` we want to get this information as well.
So `rdftab` looks for the `owl:annotatedSource` predicate,
and uses the object of that triple as the stanza.

stanza | subject | predicate             | object                   | value         | datatype | language
-------|---------|-----------------------|--------------------------|---------------|----------|----------
ex:foo | ex:foo  | rdfs:label            |                          | Foo           |          |
ex:foo | _:b1    | rdf:type              | owl:Axiom                |               |          |
ex:foo | _:b1    | owl:annotatedSubject  | ex:foo                   |               |          |
ex:foo | _:b1    | owl:annotatedProperty | rdfs:label               |               |          |
ex:foo | _:b1    | owl:annotatedTarget   |                          | Foo           |          |
ex:foo | _:b1    | rdfs:comment          |                          | A silly label |          |

