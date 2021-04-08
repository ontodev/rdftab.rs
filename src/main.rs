// Based on https://docs.rs/csv/1.1.3/csv/tutorial/index.html
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::fmt;
use std::io;
use std::process;

use serde::Serialize;
use serde_json::{to_string, to_string_pretty};

use rio_api::model::*;
use rio_api::parser::TriplesParser;
use rio_xml::{RdfXmlError, RdfXmlParser};

use rusqlite::{params, Connection, Result};

#[derive(Debug)]
struct Prefix {
    prefix: String,
    base: String,
}

#[derive(Clone, Serialize, Debug, Eq)]
enum RDFObject {
    Nested(Vec<BTreeMap<String, RDFObject>>),
    Flat(String),
}

impl fmt::Display for RDFObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RDFObject::Nested(v) => {
                let mut rdfobj_vec = String::from("[");
                for (i, bt_map) in v.iter().enumerate() {
                    rdfobj_vec.push_str(&String::from("{"));
                    for (j, (key, val)) in bt_map.iter().enumerate() {
                        rdfobj_vec.push_str(&format!("\"{}\"", key));
                        rdfobj_vec.push_str(&String::from(": "));
                        rdfobj_vec.push_str(&format!("{}", val));
                        if j < (bt_map.keys().len() - 1) {
                            rdfobj_vec.push_str(",");
                        }
                    }
                    rdfobj_vec.push_str(&String::from("}"));
                    if i < (v.len() - 1) {
                        rdfobj_vec.push_str(&String::from(","));
                    }
                }
                rdfobj_vec.push_str(&String::from("]"));
                write!(f, "{}", rdfobj_vec)
            }
            RDFObject::Flat(s) => write!(f, "\"{}\"", s),
        }
    }
}

impl Ord for RDFObject {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = to_string(self).unwrap_or(String::from(""));
        let b = to_string(other).unwrap_or(String::from(""));
        a.cmp(&b)
    }
}

impl PartialOrd for RDFObject {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RDFObject {
    fn eq(&self, other: &Self) -> bool {
        let a = to_string(self).unwrap_or(String::from(""));
        let b = to_string(other).unwrap_or(String::from(""));
        a == b
    }
}

impl RDFObject {
    fn is_blank(&self) -> bool {
        match self {
            RDFObject::Nested(a) => false,
            RDFObject::Flat(a) => a.starts_with("_:"),
        }
    }
}

fn get_prefixes(conn: &mut Connection) -> Result<Vec<Prefix>> {
    let mut stmt = conn.prepare("SELECT prefix, base FROM prefix ORDER BY length(base) DESC")?;
    let mut rows = stmt.query(params![])?;
    let mut prefixes = Vec::new();
    while let Some(row) = rows.next()? {
        prefixes.push(Prefix {
            prefix: row.get(0)?,
            base: row.get(1)?,
        });
    }
    Ok(prefixes)
}

fn shorten(prefixes: &Vec<Prefix>, iri: &str) -> String {
    for prefix in prefixes {
        if iri.starts_with(&prefix.base) {
            return iri.replace(&prefix.base, format!("{}:", prefix.prefix).as_str());
        }
    }
    return format!("<{}>", iri);
}

fn thinify(
    stanza_stack: &mut Vec<Vec<Option<String>>>, stanza_name: &mut String,
) -> Vec<Vec<Option<String>>> {
    let mut rows = vec![];
    for s in stanza_stack.iter() {
        if stanza_name == "" {
            if let Some(ref sb) = s[1] {
                *stanza_name = sb.clone();
                //println!("Changing stanza name to {}", stanza_name);
            }
        }
        let mut v = vec![Some(stanza_name.to_string())];
        v.extend_from_slice(&s);
        rows.push(v);
    }
    return rows;
}

fn get_column_contents(c: Option<&String>) -> String {
    match c {
        Some(s) => s.to_string(),
        None => String::from(""),
    }
}

fn row2object_map(row: Vec<Option<String>>) -> BTreeMap<String, RDFObject> {
    let object = get_column_contents(row[3].as_ref());
    let value = get_column_contents(row[4].as_ref());
    let datatype = get_column_contents(row[5].as_ref());
    let language = get_column_contents(row[6].as_ref());

    let mut object_map = BTreeMap::new();
    if object != "" {
        object_map.insert(String::from("object"), RDFObject::Flat(object));
    }
    else if value != "" {
        object_map.insert(String::from("value"), RDFObject::Flat(value));
        if datatype != "" {
            object_map.insert(String::from("datatype"), RDFObject::Flat(datatype));
        }
        else if language != "" {
            object_map.insert(String::from("language"), RDFObject::Flat(language));
        }
    }
    else {
        // TODO: The python code throws an exception here. Should we do something similar?
        println!("ERROR: Invalid RDF row");
    }

    return object_map;
}

fn thin2subjects(
    thin_rows: &Vec<Vec<Option<String>>>,
) -> BTreeMap<String, BTreeMap<String, Vec<BTreeMap<String, RDFObject>>>> {
    let mut subjects = BTreeMap::new();
    let mut dependencies: BTreeMap<String, BTreeSet<_>> = BTreeMap::new();
    let mut subject_ids: BTreeSet<String> = vec![].into_iter().collect();
    for row in thin_rows.iter() {
        subject_ids.insert(row[1].clone().unwrap_or(String::from("")));
    }

    for subject_id in subject_ids.iter() {
        let mut predicates = BTreeMap::new();
        for row in thin_rows.iter() {
            if subject_id.to_string() != get_column_contents(row[1].as_ref()) {
                continue;
            }

            let add_objects_and_sort = |v: &mut Vec<_>| {
                v.push(row2object_map(row.to_vec()));
                v.sort_by(|a, b| {
                    let a = to_string(&a).unwrap_or(String::from(""));
                    let b = to_string(&b).unwrap_or(String::from(""));
                    a.cmp(&b)
                });
            };

            let predicate = get_column_contents(row[2].as_ref());
            if let Some(v) = predicates.get_mut(&predicate) {
                add_objects_and_sort(v);
            }
            else if predicate != "" {
                let mut v = vec![];
                add_objects_and_sort(&mut v);
                predicates.insert(predicate, v);
            }
            else {
                println!("WARNING row {:?} has empty predicate", row);
            }

            let object = get_column_contents(row[3].as_ref());
            if object != "" && object.starts_with("_:") {
                if let Some(v) = dependencies.get_mut(subject_id) {
                    v.insert(object);
                }
                else {
                    let mut v = BTreeSet::new();
                    v.insert(object);
                    dependencies.insert(subject_id.to_string(), v);
                }
            }
        }
        subjects.insert(subject_id.to_string(), predicates);
    }

    // Work from leaves to root, nesting the blank structures:
    while !dependencies.is_empty() {
        let mut leaves: BTreeSet<_> = vec![].into_iter().collect();
        for leaf in subjects.keys() {
            if !dependencies.keys().collect::<Vec<_>>().contains(&leaf) {
                leaves.insert(leaf.clone());
            }
        }

        dependencies.clear();
        let mut handled = BTreeSet::new();
        for subject_id in subjects.keys().map(|s| s.to_string()).collect::<Vec<_>>() {
            let mut predicates = subjects
                .get(&subject_id)
                .unwrap_or(&BTreeMap::new())
                .clone();
            for predicate in predicates.keys().map(|s| s.to_string()).collect::<Vec<_>>() {
                let mut objects = vec![];
                for obj in predicates.get(&predicate).unwrap_or(&vec![]) {
                    let mut obj = obj.clone();
                    let empty_obj = RDFObject::Flat(String::from(""));
                    let o = obj.get(&String::from("object")).unwrap_or(&empty_obj);
                    let o = o.clone();
                    match o {
                        RDFObject::Nested(o) => {}
                        RDFObject::Flat(o) => {
                            if format!("{}", o).starts_with("_:") {
                                if leaves.contains(&o) {
                                    let object_val = {
                                        if let Some(v) = subjects.get(&o) {
                                            let mut w = BTreeMap::new();
                                            for (key, val) in v.iter() {
                                                let val = RDFObject::Nested(val.to_vec());
                                                w.insert(key.to_string(), val);
                                            }
                                            RDFObject::Nested(vec![w])
                                        }
                                        else {
                                            RDFObject::Nested(vec![])
                                        }
                                    };
                                    obj.clear();
                                    obj.insert(String::from("object"), object_val);
                                    handled.insert(o);
                                }
                                else {
                                    if let Some(v) = dependencies.get_mut(&subject_id) {
                                        // We expect o to be a RDFObject::Flat
                                        v.insert(format!("{}", o));
                                    }
                                    else {
                                        let mut v = BTreeSet::new();
                                        // We expect o to be a RDFObject::Flat
                                        v.insert(format!("{}", o));
                                        dependencies.insert(subject_id.to_string(), v);
                                    }
                                }
                            }
                        }
                    }
                    objects.push(obj);
                }
                objects.sort_by(|a, b| {
                    let a = to_string(&a).unwrap_or(String::from(""));
                    let b = to_string(&b).unwrap_or(String::from(""));
                    a.cmp(&b)
                });
                predicates.insert(predicate.to_string(), objects);
                subjects.insert(subject_id.to_string(), predicates.clone());
            }
        }
        for subject_id in &handled {
            subjects.remove(subject_id);
        }
    }

    // TODO: Handle OWL annotations and RDF reification
    //...

    return subjects;
}

fn jsonify(subjects: BTreeMap<String, BTreeMap<String, Vec<BTreeMap<String, RDFObject>>>>) {
    print!("{{");
    for (i, (k1, v1)) in subjects.iter().enumerate() {
        print!("\"{}\":{{", k1);
        for (j, (k2, v2)) in v1.iter().enumerate() {
            print!("\"{}\": ", k2);
            let v2 = RDFObject::Nested(v2.to_vec());
            print!("{}{}", v2, {
                if j < (v1.keys().len() - 1) {
                    ","
                }
                else {
                    ""
                }
            });
        }
        print!("}}{}", {
            if i < (subjects.keys().len() - 1) {
                ","
            }
            else {
                ""
            }
        });
    }
    print!("}}");
}

fn thickify(subjects: BTreeMap<String, BTreeMap<String, Vec<BTreeMap<String, RDFObject>>>>) {
    jsonify(subjects)
}

fn insert(db: &String) -> Result<(), Box<dyn Error>> {
    let stanza_end = NamedOrBlankNode::from(NamedNode {
        iri: "http://example.com/stanza-end",
    })
    .into();

    let annotated_source = NamedNode {
        iri: "http://www.w3.org/2002/07/owl#annotatedSource",
    };

    let rdf_subject = NamedNode {
        iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#subject",
    };

    let stdin = io::stdin();
    let mut stack: Vec<Vec<Option<String>>> = Vec::new();
    let mut stanza = String::from("");
    let mut conn = Connection::open(db)?;
    let prefixes = get_prefixes(&mut conn).expect("Get prefixes");

    let tx = conn.transaction()?;
    tx.execute(
        "CREATE TABLE IF NOT EXISTS statements (
      stanza TEXT,
      subject TEXT,
      predicate TEXT,
      object TEXT,
      value TEXT,
      datatype TEXT,
      language TEXT
    )",
        params![],
    )?;
    let filename = format!("file:{}", db);
    let mut thin_rows: Vec<_> = vec![];
    RdfXmlParser::new(stdin.lock(), filename.as_str())
        // TODO: Check with James if it would be better to replace the call to unwrap() with a
        // more robust error handling mechanism.
        .unwrap()
        .parse_all(&mut |t| {
            if t.subject == stanza_end {
                for mut row in thinify(&mut stack, &mut stanza) {
                    if row.len() != 7 {
                        row.resize_with(7, Default::default);
                    }
                    thin_rows.push(row);
                }
                // In the current implementation, thinify() will clear the stack as a
                // side effect, so we make sure to clear it here to get ready for the next stanza:
                stanza = String::from("");
                stack.clear()
            }
            else {
                let subject = match t.subject {
                    NamedOrBlankNode::NamedNode(node) => Some(shorten(&prefixes, node.iri)),
                    NamedOrBlankNode::BlankNode(node) => Some(format!("_:{}", node.id)),
                };
                let predicate = Some(shorten(&prefixes, t.predicate.iri));
                let (object, value, datatype, language) = match t.object {
                    Term::NamedNode(node) => (Some(shorten(&prefixes, node.iri)), None, None, None),
                    Term::BlankNode(node) => (Some(format!("_:{}", node.id)), None, None, None),
                    Term::Literal(node) => match node {
                        Literal::Simple { value } => (None, Some(value.to_string()), None, None),
                        Literal::Typed { value, datatype } => (
                            None,
                            Some(value.to_string()),
                            Some(shorten(&prefixes, datatype.iri)),
                            None,
                        ),
                        Literal::LanguageTaggedString { value, language } => (
                            None,
                            Some(value.to_string()),
                            None,
                            Some(language.to_string()),
                        ),
                    },
                };
                stack.push(vec![subject, predicate, object, value, datatype, language]);

                match t.subject {
                    NamedOrBlankNode::NamedNode(node) => {
                        stanza = shorten(&prefixes, node.iri);
                    }
                    _ => {}
                }
                if stanza == "" && (t.predicate == annotated_source || t.predicate == rdf_subject) {
                    match t.object {
                        Term::NamedNode(node) => {
                            stanza = shorten(&prefixes, node.iri);
                        }
                        _ => {}
                    }
                }
            }
            Ok(()) as Result<(), RdfXmlError>
        })
        // TODO: Check with James if it would be better to replace the call to unwrap() with a
        // more robust error handling mechanism.
        .unwrap();

    let subjects = thin2subjects(&thin_rows);
    jsonify(subjects);

    // TODO
    //thickify(subjects);

    for row in thin_rows {
        let mut stmt = tx
            .prepare_cached("INSERT INTO statements values (?1, ?2, ?3, ?4, ?5, ?6, ?7)")
            .expect("Statement ok");
        stmt.execute(row).expect("Insert row");
    }

    tx.commit()?;
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let usage = "Usage: rdftab [-h|--help] TARGET.db";
    match args.get(1) {
        None => {
            println!("You must specify a target database file.");
            println!("{}", usage);
            process::exit(1);
        }
        Some(i) => {
            if i.eq("--help") || i.eq("-h") {
                println!("{}", usage);
                process::exit(0);
            }
            else if i.starts_with("-") {
                println!("Unknown option: {}", i);
                println!("{}", usage);
                process::exit(1);
            }

            let db = &args[1];
            if let Err(err) = insert(db) {
                println!("{}", err);
                process::exit(1);
            }
        }
    }
}
