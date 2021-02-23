CREATE TABLE prefix (
  prefix TEXT PRIMARY KEY,
  base TEXT NOT NULL
);
INSERT INTO prefix VALUES('rdf','http://www.w3.org/1999/02/22-rdf-syntax-ns#');
INSERT INTO prefix VALUES('rdfs','http://www.w3.org/2000/01/rdf-schema#');
INSERT INTO prefix VALUES('xsd','http://www.w3.org/2001/XMLSchema#');
INSERT INTO prefix VALUES('owl','http://www.w3.org/2002/07/owl#');
INSERT INTO prefix VALUES('oio','http://www.geneontology.org/formats/oboInOwl#');
INSERT INTO prefix VALUES('dce','http://purl.org/dc/elements/1.1/');
INSERT INTO prefix VALUES('dct','http://purl.org/dc/terms/');
INSERT INTO prefix VALUES('foaf','http://xmlns.com/foaf/0.1/');
INSERT INTO prefix VALUES('protege','http://protege.stanford.edu/plugins/owl/protege#');
INSERT INTO prefix VALUES('ex','http://example.com/');

CREATE TABLE statements (
      stanza TEXT,
      subject TEXT,
      predicate TEXT,
      object TEXT,
      value TEXT,
      datatype TEXT,
      language TEXT
    );
INSERT INTO statements VALUES('ex:foo','ex:foo','rdfs:subClassOf','_:b1',NULL,NULL,NULL);
INSERT INTO statements VALUES('ex:foo','_:b1','owl:someValuesFrom','ex:bar',NULL,NULL,NULL);
INSERT INTO statements VALUES('ex:foo','_:b1','owl:onProperty','ex:part-of',NULL,NULL,NULL);
INSERT INTO statements VALUES('ex:foo','_:b1','rdf:type','owl:Restriction',NULL,NULL,NULL);
INSERT INTO statements VALUES('ex:foo','ex:foo','<ex:link>','<http://exaple.com/foo>',NULL,NULL,NULL);
INSERT INTO statements VALUES('ex:foo','ex:foo','<ex:size>',NULL,'123','xsd:int',NULL);
INSERT INTO statements VALUES('ex:foo','ex:foo','rdfs:label',NULL,'Fou',NULL,'fr');
INSERT INTO statements VALUES('ex:foo','ex:foo','rdfs:label',NULL,'Foo',NULL,NULL);
INSERT INTO statements VALUES('ex:foo','ex:foo','rdf:type','owl:Class',NULL,NULL,NULL);
