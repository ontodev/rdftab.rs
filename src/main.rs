// Based on https://docs.rs/csv/1.1.3/csv/tutorial/index.html
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io;
use std::process;

use serde::Serialize;
use serde_json::to_string;

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
                //eprintln!("Changing stanza name to {}", stanza_name);
            }
        }
        let mut v = vec![Some(stanza_name.to_string())];
        v.extend_from_slice(&s);
        rows.push(v);
    }
    return rows;
}

fn row2object_map(row: Vec<Option<String>>) -> HashMap<String, String> {
    let object = {
        if let Some(object) = row.get(3).and_then(|r| r.clone()) {
            object
        }
        else {
            String::from("")
        }
    };
    let value = {
        if let Some(value) = row.get(4).and_then(|r| r.clone()) {
            value
        }
        else {
            String::from("")
        }
    };
    let datatype = {
        if let Some(datatype) = row.get(5).and_then(|r| r.clone()) {
            datatype
        }
        else {
            String::from("")
        }
    };
    let language = {
        if let Some(language) = row.get(6).and_then(|r| r.clone()) {
            language
        }
        else {
            String::from("")
        }
    };

    let mut object_map = HashMap::new();
    if object != "" {
        object_map.insert(String::from("object"), object);
        return object_map;
    }
    else if value != "" {
        object_map.insert(String::from("value"), value);
        if datatype != "" {
            object_map.insert(String::from("datatype"), datatype);
        }
        else if language != "" {
            object_map.insert(String::from("language"), language);
        }
        return object_map;
    }
    else {
        // TODO: The python code throws an exception here. Should we do something similar?
        eprintln!("ERROR: Invalid RDF row");
        return HashMap::new().clone();
    }
}

fn thin2subjects(
    thin_rows: &Vec<Vec<Option<String>>>,
) -> HashMap<String, HashMap<String, Vec<HashMap<String, String>>>> {
    let mut subject_ids = vec![];
    for row in thin_rows.iter() {
        let subject_id = row[1].clone().unwrap_or(String::from(""));
        if subject_id != "" && !subject_ids.contains(&subject_id) {
            subject_ids.push(subject_id);
        }
    }

    let mut subjects = HashMap::new();
    let mut dependencies: HashMap<String, Vec<_>> = HashMap::new();
    for subject_id in subject_ids.iter() {
        let mut predicates: HashMap<String, Vec<_>> = HashMap::new();
        for row in thin_rows.iter() {
            if subject_id.to_string() != row[1].clone().unwrap_or(String::from("")) {
                continue;
            }

            let predicate = row[2].clone().unwrap_or(String::from(""));
            if let Some(v) = predicates.get_mut(&predicate) {
                v.push(row2object_map(row.to_vec()));
                v.sort_by(|a, b| {
                    let a = to_string(&a).unwrap_or(String::from(""));
                    let b = to_string(&b).unwrap_or(String::from(""));
                    a.cmp(&b)
                });
            }
            else if predicate != "" {
                predicates.insert(predicate, vec![]);
            }
            else {
                eprintln!("WARNING row {:?} has empty predicate", row);
            }

            let object = row[3].clone().unwrap_or(String::from(""));
            if object != "" && object.starts_with("_:") {
                if let Some(v) = dependencies.get_mut(subject_id) {
                    // add uniquely to v
                    if !v.contains(&object) {
                        v.push(object);
                    }
                }
                else {
                    dependencies.insert(subject_id.to_string(), vec![object]);
                }
            }
        }
        //eprintln!("Predicates of {:?}: {:?}", subject_id, predicates);
        subjects.insert(subject_id.to_string(), predicates);
    }

    // Work from leaves to root, nesting the blank structures:
    while !dependencies.is_empty() {
        let mut leaves = vec![];
        for leaf in subjects.keys() {
            if !dependencies.keys().collect::<Vec<_>>().contains(&leaf) {
                leaves.push(leaf.clone());
            }
        }

        dependencies.clear();
        let mut handled = vec![];
        let mut subjects_tmp = HashMap::new();
        for (subject_id, predicates) in subjects.iter() {
            for predicate in predicates.keys() {
                let mut objects = vec![];
                for obj in predicates.get(predicate).unwrap_or(&vec![]) {
                    let o = obj.get(&String::from("object"));
                    match o {
                        Some(o) => {
                            if o.starts_with("_:") {
                                if leaves.contains(&o) {
                                    let val = subjects.get(o).unwrap_or(&HashMap::new()).clone();
                                    let mut complex_obj = HashMap::new();
                                    complex_obj.insert(String::from("object"), val);
                                    if !handled.contains(o) {
                                        handled.push(o.clone());
                                    }
                                }
                                else {
                                    if let Some(v) = dependencies.get_mut(subject_id) {
                                        if !v.contains(o) {
                                            v.push(o.clone());
                                        }
                                    }
                                    else {
                                        dependencies.insert(subject_id.clone(), vec![o.clone()]);
                                    }
                                }
                            }
                        }
                        None => {}
                    }
                    objects.push(obj.clone());
                }
                objects.sort_by(|a, b| {
                    let a = to_string(&a).unwrap_or(String::from(""));
                    let b = to_string(&b).unwrap_or(String::from(""));
                    a.cmp(&b)
                });
                let mut predicates_tmp = predicates.clone();
                predicates_tmp.insert(predicate.clone(), objects);
                subjects_tmp.insert(subject_id.clone(), predicates_tmp);
            }
        }
        subjects = subjects_tmp.clone();
        for subject_id in handled {
            subjects.remove(&subject_id);
        }
    }

    // TODO: Handle OWL annotations and RDF reification
    //...

    return subjects;
}

fn render_subjects(subjects: HashMap<String, HashMap<String, Vec<HashMap<String, String>>>>) {
    let mut subject_ids: Vec<_> = subjects.keys().collect();
    subject_ids.sort();
    for subject_id in subject_ids {
        eprintln!("{}", subject_id);
        let predicates = subjects.get(subject_id);
        let mut pkeys: Vec<_> = match predicates {
            Some(p) => p.keys().collect(),
            None => vec![],
        };
        pkeys.sort();
        for pkey in pkeys {
            eprintln!(" {}", pkey);
            let objs = match predicates {
                Some(p) => p.get(pkey).unwrap().clone(),
                None => vec![],
            };
            for obj in objs {
                eprintln!("   {:?}", obj);
            }
        }
    }
}

fn get_rows_to_insert(
    stanza_stack: &mut Vec<Vec<Option<String>>>, stanza_name: &mut String,
) -> Vec<Vec<Option<String>>> {
    //eprintln!("Processing stanza: {} ...", stanza_name);

    let thin_rows = thinify(stanza_stack, stanza_name);
    let subjects = thin2subjects(&thin_rows);
    eprintln!("#############################################");
    eprintln!("{:?}", subjects);
    eprintln!("#############################################");
    render_subjects(subjects);

    // TODO: replace this later with thickified rows:
    return thin_rows;
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
    RdfXmlParser::new(stdin.lock(), filename.as_str())
        // TODO: Check with James if it would be better to replace the call to unwrap() with a
        // more robust error handling mechanism.
        .unwrap()
        .parse_all(&mut |t| {
            if t.subject == stanza_end {
                for mut row in get_rows_to_insert(&mut stack, &mut stanza) {
                    if row.len() != 7 {
                        row.resize_with(7, Default::default);
                    }
                    let mut stmt = tx
                        .prepare_cached(
                            "INSERT INTO statements values (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        )
                        .expect("Statement ok");
                    stmt.execute(row).expect("Insert row");
                }
                // In the current implementation, get_rows_to_insert() will clear the stack as a
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
