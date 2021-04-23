// Based on https://docs.rs/csv/1.1.3/csv/tutorial/index.html
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::fmt;
use std::io;
use std::process;

use rio_api::model::*;
use rio_api::parser::TriplesParser;
use rio_xml::{RdfXmlError, RdfXmlParser};

use rusqlite::{params, Connection, Result};

use serde::Serialize;
use serde_json::to_string;

/// Represents a URI prefix
#[derive(Debug)]
struct Prefix {
    prefix: String,
    base: String,
}

/// A custom datatype that is useful for passing information about a given stanza between functions:
#[derive(Clone, Serialize, Debug, Eq)]
enum HandyEnum {
    VecOf(Vec<HandyEnum>),
    MapTo(BTreeMap<String, HandyEnum>),
    Flat(String),
}

impl Ord for HandyEnum {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = to_string(self).unwrap_or(String::from(""));
        let b = to_string(other).unwrap_or(String::from(""));
        a.cmp(&b)
    }
}

impl PartialOrd for HandyEnum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HandyEnum {
    fn eq(&self, other: &Self) -> bool {
        let a = to_string(self).unwrap_or(String::from(""));
        let b = to_string(other).unwrap_or(String::from(""));
        a == b
    }
}

impl HandyEnum {
    /// Renders the given HandyEnum object as a String
    fn render(&self) -> String {
        let mut string_to_return = String::from("");
        match self {
            HandyEnum::VecOf(v) => {
                string_to_return.push_str("[");
                for (i, bt_map) in v.iter().enumerate() {
                    let nested_obj = bt_map.render();
                    string_to_return.push_str(nested_obj.as_str());
                    if i < (v.len() - 1) {
                        string_to_return.push_str(",");
                    }
                }
                string_to_return.push_str("]");
            }
            HandyEnum::MapTo(bt_map) => {
                string_to_return.push_str("{");
                for (j, (key, val)) in bt_map.iter().enumerate() {
                    string_to_return.push_str(&format!("\"{}\"", key));
                    string_to_return.push_str(":");
                    string_to_return.push_str(&format!("{}", val));
                    if j < (bt_map.keys().len() - 1) {
                        string_to_return.push_str(",");
                    }
                }
                string_to_return.push_str("}");
            }
            HandyEnum::Flat(s) => {
                string_to_return.push_str("\"");
                string_to_return.push_str(s);
                string_to_return.push_str("\"");
            }
        };
        string_to_return
    }
}

impl fmt::Display for HandyEnum {
    /// The default formatter for HandyEnum objects.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.render())
    }
}

/// Converts an object in the form of a BTreeMap mapping strings to vectors of BTreeMaps mapping
/// strings to HandyEnum objects, into BTreeMap mapping strings to HandyEnums.
fn nest_handy_enum(
    exposed: &BTreeMap<String, Vec<BTreeMap<String, HandyEnum>>>,
) -> BTreeMap<String, HandyEnum> {
    let mut nest = BTreeMap::new();
    for (key, val) in exposed.iter() {
        let val = {
            let mut tmp = vec![];
            for bt_map in val.iter() {
                tmp.push(HandyEnum::MapTo(bt_map.clone()));
            }
            HandyEnum::VecOf(tmp)
        };
        nest.insert(key.to_string(), val);
    }
    nest
}

/// Converts an object in the form of a BTreeMap mapping strings to BTreeMaps mapping strings to
/// vectors of BTreeMaps mapping strings to HandyEnum objects, into a BTreeMap mapping strings to
/// HandyEnums.
fn double_nest_handy_enum(
    double_exposed: &BTreeMap<String, BTreeMap<String, Vec<BTreeMap<String, HandyEnum>>>>,
) -> BTreeMap<String, HandyEnum> {
    let mut double_nest = BTreeMap::new();
    for (k1, v1) in double_exposed.iter() {
        let mut tmp = BTreeMap::new();
        for (k2, v2) in v1.iter() {
            let val = {
                let mut handy_vec = vec![];
                for bt_map in v2.iter() {
                    handy_vec.push(HandyEnum::MapTo(bt_map.clone()));
                }
                HandyEnum::VecOf(handy_vec)
            };

            tmp.insert(k2.to_string(), val);
        }
        double_nest.insert(k1.to_string(), HandyEnum::MapTo(tmp));
    }
    double_nest
}

/// Fetch all prefixes via the given connection to the database.
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

/// If the given IRI begins with a known prefix, shorten the IRI by replacing the long form of the
/// prefix with its short form.
fn shorten(prefixes: &Vec<Prefix>, iri: &str) -> String {
    for prefix in prefixes {
        if iri.starts_with(&prefix.base) {
            return iri.replace(&prefix.base, format!("{}:", prefix.prefix).as_str());
        }
    }
    return format!("<{}>", iri);
}

/// Given a stack of rows representing a stanza, add a new column with the given stanza name to each
/// row and return the modified rows.
fn thinify(
    stanza_stack: &mut Vec<Vec<Option<String>>>,
    stanza_name: &mut String,
) -> Vec<Vec<Option<String>>> {
    let mut rows = vec![];
    for s in stanza_stack.iter() {
        if stanza_name == "" {
            if let Some(ref sb) = s[1] {
                *stanza_name = sb.clone();
            }
        }
        let mut v = vec![Some(stanza_name.to_string())];
        v.extend_from_slice(&s);
        rows.push(v);
    }
    return rows;
}

/// Given an Option representing a cell from a given column of a given row, return its contents
/// or an empty string if the cell has None.
fn get_cell_contents(c: Option<&String>) -> String {
    match c {
        Some(s) => s.to_string(),
        None => String::from(""),
    }
}

/// Convert the given row to a map from strings to HandyEnums.
fn row2object_map(row: &Vec<Option<String>>) -> BTreeMap<String, HandyEnum> {
    let object = get_cell_contents(row[3].as_ref());
    let value = get_cell_contents(row[4].as_ref());
    let datatype = get_cell_contents(row[5].as_ref());
    let language = get_cell_contents(row[6].as_ref());

    let mut object_map = BTreeMap::new();
    if object != "" {
        object_map.insert(String::from("object"), HandyEnum::Flat(object));
    } else {
        object_map.insert(String::from("value"), HandyEnum::Flat(value));
        if datatype != "" {
            object_map.insert(String::from("datatype"), HandyEnum::Flat(datatype));
        } else if language != "" {
            object_map.insert(String::from("language"), HandyEnum::Flat(language));
        }
    }

    return object_map;
}

/// Given a map (representing predicates) from strings to vectors of BTreeMaps mapping strings to
/// HandyEnums, and a specific predicate represented by a string slice, return a HandyEnum
/// representing the first object contained in the predicates map.
fn first_object(
    predicates: &BTreeMap<String, Vec<BTreeMap<String, HandyEnum>>>,
    predicate: &str,
) -> HandyEnum {
    let objs = predicates.get(predicate);
    match objs {
        None => (),
        Some(objs) => {
            for obj in objs.iter() {
                match obj.get("object") {
                    None => (),
                    Some(o) => return o.clone(),
                };
            }
        }
    };
    return HandyEnum::Flat(String::from(""));
}

/// Given a subject id, a map of subjects to read from, a compressed map of subjects to be written
/// to, a set of subject ids to be marked for removal, and the subject, predicate, and object types
/// to be compressed, write a compressed version of subjects to compressed_subjects, and add the
/// eliminated subject ids to the list of subject ids to be removed.
fn compress(
    subject_id: &String,
    subjects: &BTreeMap<String, BTreeMap<String, Vec<BTreeMap<String, HandyEnum>>>>,
    compressed_subjects: &mut BTreeMap<String, BTreeMap<String, Vec<BTreeMap<String, HandyEnum>>>>,
    remove: &mut BTreeSet<String>,
    subject_type: &str,
    predicate_type: &str,
    object_type: &str,
) {
    let preds = match subjects.get(subject_id) {
        Some(p) => p.clone(),
        None => BTreeMap::new(),
    };
    let subject = format!("{}", first_object(&preds, subject_type))
        .trim_start_matches("\"")
        .trim_end_matches("\"")
        .to_string();
    let predicate = format!("{}", first_object(&preds, predicate_type))
        .trim_start_matches("\"")
        .trim_end_matches("\"")
        .to_string();
    let obj = match preds.get(object_type).and_then(|x| x.first()) {
        Some(obj) => obj.clone(),
        None => BTreeMap::new(),
    };
    println!("<S, P, O> = <{}, {}, {:?}>", subject, predicate, obj);
    compressed_subjects
        .get_mut(subject_id)
        .and_then(|x| x.remove(subject_type));
    compressed_subjects
        .get_mut(subject_id)
        .and_then(|x| x.remove(predicate_type));
    compressed_subjects
        .get_mut(subject_id)
        .and_then(|x| x.remove(object_type));
    compressed_subjects
        .get_mut(subject_id)
        .and_then(|x| x.remove("rdf:type"));
    if let Some(objs) = subjects
        .get(&subject)
        .and_then(|preds| preds.get(&predicate))
    {
        let mut objs_copy = vec![];
        for o in objs {
            let mut o = o.clone();
            if o == obj {
                let new_preds = match compressed_subjects.get(subject_id) {
                    Some(p) => nest_handy_enum(&p),
                    None => BTreeMap::new(),
                };
                o.insert(String::from("annotations"), HandyEnum::MapTo(new_preds));
                remove.insert(subject_id.to_string());
            }
            objs_copy.push(o);
        }
        *compressed_subjects
            .get_mut(&subject)
            .and_then(|x| x.get_mut(&predicate))
            .unwrap_or(&mut vec![]) = objs_copy;
    }
}

/// Given a vector of thin rows, return a map from Strings to HandyEnums
fn thin_rows_to_subjects(thin_rows: &Vec<Vec<Option<String>>>) -> BTreeMap<String, HandyEnum> {
    let mut subjects = BTreeMap::new();
    let mut dependencies: BTreeMap<String, BTreeSet<_>> = BTreeMap::new();
    let mut subject_ids: BTreeSet<String> = vec![].into_iter().collect();
    for row in thin_rows.iter() {
        subject_ids.insert(row[1].clone().unwrap_or(String::from("")));
    }

    // Convert the given thin rows to a BTreeMap of subjects:
    println!("Converting subject ids to subjects map ...");
    let somewhat = subject_ids.len();
    for (i, subject_id) in subject_ids.iter().enumerate() {
        let mut predicates = BTreeMap::new();
        for row in thin_rows.iter() {
            if subject_id.to_string() != get_cell_contents(row[1].as_ref()) {
                continue;
            }

            // Useful closure for adding nested HandyEnums to a vector in sorted order:
            let add_objects_and_sort = |v: &mut Vec<_>| {
                v.push(row2object_map(&row));
                v.sort_by(|a, b| HandyEnum::MapTo(a.clone()).cmp(&HandyEnum::MapTo(b.clone())));
            };

            let predicate = get_cell_contents(row[2].as_ref());
            if let Some(v) = predicates.get_mut(&predicate) {
                add_objects_and_sort(v);
            } else if predicate != "" {
                let mut v = vec![];
                add_objects_and_sort(&mut v);
                predicates.insert(predicate, v);
            } else {
                println!("WARNING row {:?} has empty predicate", row);
            }

            let object = get_cell_contents(row[3].as_ref());
            if object != "" && object.starts_with("_:") {
                if let Some(v) = dependencies.get_mut(subject_id) {
                    v.insert(object);
                } else {
                    let mut v = BTreeSet::new();
                    v.insert(object);
                    dependencies.insert(subject_id.to_string(), v);
                }
            }
        }
        subjects.insert(subject_id.to_string(), predicates);
        if i != 0 && (i % 500) == 0 {
            println!("Converted {} subject ids ({} total) ...", i + 1, somewhat);
        }
    }

    // Work from leaves to root, nesting the blank structures:
    println!("Working through dependencies ...");
    while !dependencies.is_empty() {
        let mut leaves: BTreeSet<_> = vec![].into_iter().collect();
        for leaf in subjects.keys() {
            if !dependencies.keys().collect::<Vec<_>>().contains(&leaf) {
                leaves.insert(leaf.clone());
            }
        }

        dependencies.clear();
        let mut handled = BTreeSet::new();
        let somewhat = subjects.keys().len();
        for (i, subject_id) in subjects
            .keys()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .iter()
            .enumerate()
        {
            let mut predicates = subjects.get(subject_id).unwrap_or(&BTreeMap::new()).clone();
            let funwhat = predicates.keys().len();
            for (j, predicate) in predicates
                .keys()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .iter()
                .enumerate()
            {
                let mut objects = vec![];
                let gunwhat = predicates.get(predicate).unwrap_or(&vec![]).len();
                for (k, obj) in predicates
                    .get(predicate)
                    .unwrap_or(&vec![])
                    .iter()
                    .enumerate()
                {
                    let mut obj = obj.clone();
                    let empty_obj = HandyEnum::Flat(String::from(""));
                    let o = obj.get(&String::from("object")).unwrap_or(&empty_obj);
                    let o = o.clone();
                    match o {
                        HandyEnum::VecOf(_) => {}
                        HandyEnum::MapTo(_) => {}
                        HandyEnum::Flat(o) => {
                            if o.starts_with("_:") {
                                if leaves.contains(&o) {
                                    let object_val = {
                                        if let Some(o) = subjects.get(&o) {
                                            HandyEnum::MapTo(nest_handy_enum(&o))
                                        } else {
                                            HandyEnum::MapTo(BTreeMap::new())
                                        }
                                    };
                                    obj.clear();
                                    obj.insert(String::from("object"), object_val);
                                    handled.insert(o);
                                } else {
                                    if let Some(v) = dependencies.get_mut(subject_id) {
                                        // We expect o to be a HandyEnum::Flat
                                        v.insert(format!("{}", o));
                                    } else {
                                        let mut v = BTreeSet::new();
                                        // We expect o to be a HandyEnum::Flat
                                        v.insert(format!("{}", o));
                                        dependencies.insert(subject_id.to_string(), v);
                                    }
                                }
                            }
                        }
                    }
                    objects.push(obj);
                    if k != 0 && (k % 100) == 0 {
                        println!("Converted {} objects ({} total) ...", k + 1, gunwhat);
                    }
                }
                objects
                    .sort_by(|a, b| HandyEnum::MapTo(a.clone()).cmp(&HandyEnum::MapTo(b.clone())));
                predicates.insert(predicate.to_string(), objects);
                subjects.insert(subject_id.to_string(), predicates.clone());
                if j != 0 && (j % 100) == 0 {
                    println!("Converted {} predicates ({} total) ...", j + 1, funwhat);
                }
            }
            if i != 0 && (i % 100) == 0 {
                println!("Converted {} subject ids ({} total) ...", i + 1, somewhat);
            }
        }
        for subject_id in &handled {
            subjects.remove(subject_id);
        }
    }

    // OWL annotation and HandyEnum reification:
    println!("Doing OWL annotation and HandyEnum reification ...");
    let mut remove: BTreeSet<String> = vec![].into_iter().collect();
    let mut compressed_subjects = subjects.clone();
    for subject_id in subjects.keys() {
        let subject_id = subject_id.to_string();
        let preds = match subjects.get(&subject_id) {
            Some(p) => p.clone(),
            None => BTreeMap::new(),
        };
        if preds.contains_key("owl:annotatedSource") {
            println!("OWL annotation {}", subject_id);
            compress(
                &subject_id,
                &subjects,
                &mut compressed_subjects,
                &mut remove,
                "owl:annotatedSource",
                "owl:annotatedProperty",
                "owl:annotatedTarget",
            );
        }

        if preds.contains_key("rdf:subject") {
            println!("RDF Reification {}", subject_id);
            compress(
                &subject_id,
                &subjects,
                &mut compressed_subjects,
                &mut remove,
                "rdf:subject",
                "rdf:predicate",
                "rdf:object",
            );
        }
    }

    // Remove the subject ids from compressed_subjects that we earlier identified for removal:
    for r in remove.iter() {
        compressed_subjects.remove(r);
    }

    return double_nest_handy_enum(&compressed_subjects);
}

/// Convert a BTreeMap, `subjects`, from Strings to HandyEnums, into a vector of BTreeMaps that map
/// Strings to HandyEnums.
fn subjects_to_thick_rows(
    subjects: &BTreeMap<String, HandyEnum>,
) -> Vec<BTreeMap<String, HandyEnum>> {
    let mut rows = vec![];
    for subject_id in subjects.keys() {
        let predicates = match subjects.get(subject_id) {
            Some(p) => match p {
                HandyEnum::MapTo(p) => p.clone(),
                _ => BTreeMap::new(),
            },
            None => BTreeMap::new(),
        };
        for predicate in predicates.keys() {
            let objs = match predicates.get(predicate) {
                Some(o) => match o {
                    HandyEnum::VecOf(o) => o.clone(),
                    _ => vec![],
                },
                None => vec![],
            };
            for obj in objs {
                let mut result = match obj {
                    HandyEnum::MapTo(o) => o,
                    _ => BTreeMap::new(),
                };
                result.insert(String::from("subject"), HandyEnum::Flat(subject_id.clone()));
                result.insert(
                    String::from("predicate"),
                    HandyEnum::Flat(predicate.clone()),
                );
                match result.get(&String::from("object")) {
                    Some(s) => match s {
                        HandyEnum::Flat(_) => (),
                        _ => {
                            let s = format!("{}", s).replace("\"", "\\\"");
                            result.insert(String::from("object"), HandyEnum::Flat(s.to_string()));
                        }
                    },
                    None => (),
                };
                rows.push(result);
            }
        }
    }
    return rows;
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
    println!("Parsing thin rows ...");
    RdfXmlParser::new(stdin.lock(), filename.as_str())
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
            } else {
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
        .unwrap();

    println!("Converting thin rows to subjects ...");
    let subjects = thin_rows_to_subjects(&thin_rows);
    println!("Converting subjects to thick rows ...");
    let thick_rows = subjects_to_thick_rows(&subjects);
    let rows_to_insert = {
        let mut rows = vec![];
        for t in &thick_rows {
            let mut row = vec![];
            for column in vec![
                "subject",
                "predicate",
                "object",
                "value",
                "datatype",
                "language",
            ] {
                match t.get(column) {
                    Some(HandyEnum::Flat(s)) => row.push(Some(s)),
                    None => row.push(None),
                    _ => (),
                };
            }
            rows.push(row);
        }
        rows
    };

    println!("Inserting thick rows to db ...");
    for row in rows_to_insert {
        let mut stmt = tx
            .prepare_cached("INSERT INTO statements values (?1, ?2, ?3, ?4, ?5, ?6)")
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
            } else if i.starts_with("-") {
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
