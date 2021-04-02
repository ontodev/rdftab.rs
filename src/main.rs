// Based on https://docs.rs/csv/1.1.3/csv/tutorial/index.html
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
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

fn row2object_map(row: Vec<Option<String>>) -> BTreeMap<String, String> {
    let object = get_column_contents(row[3].as_ref());
    let value = get_column_contents(row[4].as_ref());
    let datatype = get_column_contents(row[5].as_ref());
    let language = get_column_contents(row[6].as_ref());

    let mut object_map = BTreeMap::new();
    if object != "" {
        object_map.insert(String::from("object"), object);
    }
    else if value != "" {
        object_map.insert(String::from("value"), value);
        if datatype != "" {
            object_map.insert(String::from("datatype"), datatype);
        }
        else if language != "" {
            object_map.insert(String::from("language"), language);
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
) -> BTreeMap<String, BTreeMap<String, Vec<BTreeMap<String, String>>>> {
    let mut subjects = BTreeMap::new();
    let mut dependencies: BTreeMap<String, BTreeSet<_>> = BTreeMap::new();
    let mut subject_ids: BTreeSet<String> = vec![].into_iter().collect();
    for row in thin_rows.iter() {
        subject_ids.insert(row[1].clone().unwrap_or(String::from("")));
    }

    for subject_id in subject_ids.iter() {
        let mut predicates: BTreeMap<String, Vec<_>> = BTreeMap::new();
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
    println!("SUBJECTS ARE:\n {}", to_string_pretty(&subjects).unwrap());

    // Work from leaves to root, nesting the blank structures:
    while !dependencies.is_empty() {
        let mut leaves: BTreeSet<String> = vec![].into_iter().collect();
        for leaf in subjects.keys() {
            if !dependencies.keys().collect::<Vec<_>>().contains(&leaf) {
                leaves.insert(leaf.clone());
            }
        }

        dependencies.clear();
        let mut handled = BTreeSet::new();
        for subject_id in subjects.clone().keys() {
            let mut predicates = subjects.get(subject_id).unwrap_or(&BTreeMap::new()).clone();
            for predicate in predicates.clone().keys() {
                let mut objects = vec![];
                for obj in predicates.get(predicate).unwrap_or(&vec![]) {
                    let mut obj = obj.clone();
                    let o = obj.get(&String::from("object"));
                    match o {
                        None => {}
                        Some(o) => {
                            let o = o.clone();
                            if o.starts_with("_:") {
                                if leaves.contains(&o) {
                                    // TODO: Instead of converting to a String here, we should
                                    // create a complex object. This will require us to redefine
                                    // predicates so that it is a map from Strings to possibly
                                    // nested objects.
                                    let val = subjects.get(&o).unwrap_or(&BTreeMap::new()).clone();
                                    let val = to_string(&val).unwrap_or(String::from(""));
                                    obj.insert(String::from("object"), val);
                                    handled.insert(o);
                                }
                                else {
                                    if let Some(v) = dependencies.get_mut(subject_id) {
                                        v.insert(o);
                                    }
                                    else {
                                        let mut v = BTreeSet::new();
                                        v.insert(o);
                                        dependencies.insert(subject_id.clone(), v);
                                    }
                                }
                            }
                        }
                    }
                    objects.push(obj.clone());
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

    println!(
        "SUBJECTS ARE NOW:\n {}",
        to_string_pretty(&subjects).unwrap()
    );

    // TODO: Handle OWL annotations and RDF reification
    //...

    //return subjects;
    return BTreeMap::new();
}

fn render_subjects(subjects: BTreeMap<String, BTreeMap<String, Vec<BTreeMap<String, String>>>>) {
    let mut subject_ids: Vec<_> = subjects.keys().collect();
    subject_ids.sort();
    for subject_id in subject_ids {
        println!("{}", subject_id);
        let predicates = subjects.get(subject_id);
        let mut pkeys: Vec<_> = match predicates {
            Some(p) => p.keys().collect(),
            None => vec![],
        };
        pkeys.sort();
        for pkey in pkeys {
            println!(" {}", pkey);
            let objs = match predicates {
                Some(p) => p.get(pkey).unwrap().clone(),
                None => vec![],
            };
            for obj in objs {
                println!("   {:?}", obj);
            }
        }
    }
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

    //println!("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
    //println!("Received {} rows:\n{}", thin_rows.len(),
    //         to_string_pretty(&thin_rows).unwrap());
    //println!("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

    let subjects = thin2subjects(&thin_rows);
    //println!("#############################################");
    //println!("{}", to_string_pretty(&subjects).unwrap());
    //println!("#############################################");
    // render_subjects(subjects);

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
