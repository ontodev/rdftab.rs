// Based on https://docs.rs/csv/1.1.3/csv/tutorial/index.html
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::io;
use std::process;

use oxiri::Iri;
use phf::phf_map;
use rio_api::model::{Literal, NamedNode, NamedOrBlankNode, Term};
use rio_api::parser::TriplesParser;
use rio_xml::{RdfXmlError, RdfXmlParser};
use rusqlite::{params, Connection, Result};
use serde_json::{
    // SerdeMap by default backed by BTreeMap (see https://docs.serde.rs/serde_json/map/index.html)
    Map as SerdeMap,
    Value as SerdeValue,
};

/// Represents a URI prefix
#[derive(Debug)]
struct Prefix {
    prefix: String,
    base: String,
}

/// Fetch all prefixes from the database via the given database connection
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
    stanza_stack: &Vec<Vec<Option<String>>>,
    stanza_name: &String,
) -> Vec<Vec<Option<String>>> {
    let mut rows = vec![];
    let mut stanza_name = stanza_name.to_string();
    for s in stanza_stack.iter() {
        if stanza_name == "" {
            if let Some(ref sb) = s[1] {
                stanza_name = sb.clone();
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

/// Convert the given row to a SerdeValue::Object
fn row2object_map(row: &Vec<Option<String>>) -> SerdeValue {
    let object = get_cell_contents(row[3].as_ref());
    let value = get_cell_contents(row[4].as_ref());
    let datatype = get_cell_contents(row[5].as_ref());
    let language = get_cell_contents(row[6].as_ref());

    let mut object_map = SerdeMap::new();
    if object != "" {
        object_map.insert(String::from("object"), SerdeValue::String(object));
    } else {
        object_map.insert(String::from("value"), SerdeValue::String(value));
        if datatype != "" {
            object_map.insert(String::from("datatype"), SerdeValue::String(datatype));
        } else if language != "" {
            object_map.insert(String::from("language"), SerdeValue::String(language));
        }
    }

    return SerdeValue::Object(object_map);
}

/// Given a SerdeMap mapping strings to SerdeValues, and a specific predicate represented by a
/// string slice, return a SerdeValue representing the first object contained in the predicates map.
fn first_object(predicates: &SerdeMap<String, SerdeValue>, predicate: &str) -> SerdeValue {
    match predicates.get(predicate) {
        None => (),
        Some(objs) => match objs {
            SerdeValue::Array(v) => {
                for obj in v.iter() {
                    if let Some(o) = obj.get("object") {
                        return o.clone();
                    } else if let Some(o) = obj.get("value") {
                        return o.clone();
                    }
                }
            }
            _ => (),
        },
    };
    eprintln!("WARNING No object found");
    return SerdeValue::String(String::from(""));
}

/// Given a subject id, a map representing subjects, a map that compressed versions of the subjects
/// map will be copied to, a set of subject ids to be marked for removal, and the subject,
/// predicate, and object types to be compressed, write a compressed version of subjects to
/// compressed_subjects, and add the eliminated subject ids to the list of those marked for removal.
fn compress(
    kind: &str,
    subject_id: &String,
    subjects: &SerdeMap<String, SerdeValue>,
    compressed_subjects: &mut SerdeMap<String, SerdeValue>,
    remove: &mut BTreeSet<String>,
    preds: &SerdeMap<String, SerdeValue>,
    subject_type: &str,
    predicate_type: &str,
    object_type: &str,
) {
    let subject = format!("{}", first_object(&preds, subject_type))
        .trim_start_matches("\"")
        .trim_end_matches("\"")
        .to_string();
    let predicate = format!("{}", first_object(&preds, predicate_type))
        .trim_start_matches("\"")
        .trim_end_matches("\"")
        .to_string();
    let obj = format!("{}", first_object(&preds, object_type))
        .trim_start_matches("\"")
        .trim_end_matches("\"")
        .to_string();

    if let Some(SerdeValue::Object(m)) = compressed_subjects.get_mut(subject_id) {
        m.remove(subject_type);
        m.remove(predicate_type);
        m.remove(object_type);
        m.remove("rdf:type");
    }

    let alt_preds: SerdeMap<String, SerdeValue>;
    match subjects.get(&subject) {
        Some(SerdeValue::Object(m)) => alt_preds = m.clone(),
        _ => alt_preds = SerdeMap::new(),
    };
    if let None = compressed_subjects.get(&subject) {
        compressed_subjects.insert(subject.to_string(), SerdeValue::Object(alt_preds.clone()));
    }
    // We are assured compressed_preds will not be None because of the code immediately above, so
    // we simply call unwrap() here:
    let compressed_preds = compressed_subjects.get_mut(&subject).unwrap();
    if let None = compressed_preds.get(&predicate) {
        let compressed_objs: SerdeValue;
        match alt_preds.get(&predicate) {
            Some(SerdeValue::Object(p)) => compressed_objs = SerdeValue::Object(p.clone()),
            _ => compressed_objs = SerdeValue::Object(SerdeMap::new()),
        };
        if let SerdeValue::Object(m) = compressed_preds {
            m.insert(predicate.to_string(), compressed_objs);
        }
    }

    if let Some(SerdeValue::Array(objs)) = compressed_subjects
        .get(&subject)
        .and_then(|preds| preds.get(&predicate))
    {
        let mut objs_copy = vec![];
        for o in objs {
            let mut o = o.clone();
            let o_obj: String;
            let o_val: String;
            let trim = |s: String| {
                format!("{}", s)
                    .trim_start_matches("\"")
                    .trim_end_matches("\"")
                    .to_string()
            };
            match o.get("object") {
                Some(s) => o_obj = trim(format!("{}", s)),
                None => o_obj = String::from(""),
            };
            match o.get("value") {
                Some(s) => o_val = trim(format!("{}", s)),
                None => o_val = String::from(""),
            };

            if o_obj == obj || o_val == obj {
                if let Some(SerdeValue::Object(items)) = compressed_subjects.get(subject_id) {
                    let mut annotations;
                    match o.get(kind) {
                        Some(SerdeValue::Object(m)) => annotations = m.clone(),
                        _ => annotations = SerdeMap::new(),
                    };
                    for (key, val) in items.iter() {
                        let mut annotations_for_key;
                        match annotations.get(key) {
                            Some(SerdeValue::Array(v)) => annotations_for_key = v.clone(),
                            _ => annotations_for_key = vec![],
                        };
                        if let SerdeValue::Array(v) = val {
                            for w in v {
                                annotations_for_key.push(w.clone());
                            }
                        }
                        annotations.insert(key.to_string(), SerdeValue::Array(annotations_for_key));
                    }
                    if let SerdeValue::Object(mut m) = o.clone() {
                        m.insert(kind.to_string(), SerdeValue::Object(annotations));
                        o = SerdeValue::Object(m);
                        remove.insert(subject_id.to_string());
                    } else {
                        eprintln!("WARNING: {} is not a map.", o);
                    }
                }
            }
            objs_copy.push(o);
        }

        if let Some(SerdeValue::Object(m)) = compressed_subjects.get_mut(&subject) {
            if let Some(SerdeValue::Array(v)) = m.get_mut(&predicate) {
                *v = objs_copy;
            }
        }
    }
}

/// Given a vector of thin rows, return a map from Strings to SerdeValues
fn thin_rows_to_subjects(thin_rows: &Vec<Vec<Option<String>>>) -> SerdeMap<String, SerdeValue> {
    let mut subjects = SerdeMap::new();
    let mut dependencies: BTreeMap<String, BTreeSet<_>> = BTreeMap::new();
    let mut subject_ids: BTreeSet<String> = vec![].into_iter().collect();
    for row in thin_rows.iter() {
        subject_ids.insert(get_cell_contents(row[1].as_ref()));
    }

    for subject_id in &subject_ids {
        let mut predicates = SerdeMap::new();
        for row in thin_rows.iter() {
            if subject_id.ne(&get_cell_contents(row[1].as_ref())) {
                continue;
            }

            let object_map = row2object_map(&row);
            // Useful closure for adding SerdeValues to a list in sorted order:
            let add_objects_and_sort = |v: &mut SerdeValue| {
                if let SerdeValue::Array(v) = v {
                    v.push(object_map);
                    v.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                }
            };

            let predicate = get_cell_contents(row[2].as_ref());
            // If the given predicate is already associated with a list in the predicates map,
            // then add the objects represented by `row` to the list in sorted order, otherwise
            // add an empty list corresponding to the predicate in the map.
            if let Some(v) = predicates.get_mut(&predicate) {
                add_objects_and_sort(v);
            } else if predicate != "" {
                let mut v = SerdeValue::Array(vec![]);
                add_objects_and_sort(&mut v);
                predicates.insert(predicate, v);
            } else {
                eprintln!("WARNING row {:?} has empty predicate", row);
            }

            let object = get_cell_contents(row[3].as_ref());
            // If the object is a blank node, then if a set corresponding to `subject_id` already
            // exists in the dependencies map, add the object to it; otherwise add an empty list
            // corresponding to the subject in the map.
            if object != "" && object.starts_with("_:") {
                if let Some(v) = dependencies.get_mut(subject_id) {
                    v.insert(object);
                } else {
                    let mut v = BTreeSet::new();
                    v.insert(object);
                    dependencies.insert(subject_id.to_owned(), v);
                }
            }
        }

        // Add an entry mapping `subject_id` to the predicates map in the subjects map:
        subjects.insert(subject_id.to_owned(), SerdeValue::Object(predicates));
    }

    work_through_dependencies(&mut dependencies, &mut subjects);
    subjects
}

fn work_through_dependencies(
    dependencies: &mut BTreeMap<String, BTreeSet<String>>,
    subjects: &mut SerdeMap<String, SerdeValue>,
) {
    // Work through dependencies from leaves to root, nesting the blank structures:
    while !dependencies.is_empty() {
        let mut leaves: BTreeSet<_> = vec![].into_iter().collect();
        for leaf in subjects.keys() {
            if !dependencies.keys().collect::<Vec<_>>().contains(&leaf) {
                leaves.insert(leaf.to_owned());
            }
        }

        dependencies.clear();
        let mut handled = BTreeSet::new();
        for subject_id in &subjects.keys().map(|s| s.to_owned()).collect::<Vec<_>>() {
            let mut predicates: SerdeMap<String, SerdeValue>;
            match subjects.get(subject_id) {
                Some(SerdeValue::Object(m)) => predicates = m.clone(),
                _ => predicates = SerdeMap::new(),
            };

            for predicate in &predicates.keys().map(|s| s.to_owned()).collect::<Vec<_>>() {
                let pred_objs: Vec<SerdeValue>;
                match predicates.get(predicate) {
                    Some(SerdeValue::Array(v)) => pred_objs = v.clone(),
                    _ => pred_objs = vec![],
                };

                let mut objects = vec![];
                for obj in &pred_objs {
                    let mut obj = obj.to_owned();
                    let o: SerdeValue;
                    if let Some(val) = obj.get(&String::from("object")) {
                        o = val.to_owned();
                    } else {
                        o = SerdeValue::Object(SerdeMap::new());
                    }

                    match o {
                        SerdeValue::String(o) => {
                            if o.starts_with("_:") {
                                if leaves.contains(&o) {
                                    let val: SerdeValue;
                                    if let Some(v) = subjects.get(&o) {
                                        val = v.to_owned();
                                    } else {
                                        val = SerdeValue::Object(SerdeMap::new());
                                    }

                                    if let SerdeValue::Object(ref mut m) = obj {
                                        m.clear();
                                        m.insert(String::from("object"), val);
                                        handled.insert(o);
                                    }
                                } else {
                                    if let Some(v) = dependencies.get_mut(subject_id) {
                                        v.insert(o);
                                    } else {
                                        let mut v = BTreeSet::new();
                                        v.insert(o);
                                        dependencies.insert(subject_id.to_owned(), v);
                                    }
                                }
                            }
                        }
                        _ => (),
                    }
                    objects.push(obj);
                }
                objects.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                predicates.insert(predicate.to_owned(), SerdeValue::Array(objects));
                subjects.insert(
                    subject_id.to_owned(),
                    SerdeValue::Object(predicates.to_owned()),
                );
            }
        }
        for subject_id in &handled {
            subjects.remove(subject_id);
        }
    }
}

fn annotate_reify(subjects: SerdeMap<String, SerdeValue>) -> SerdeMap<String, SerdeValue> {
    // OWL annotation and RDF reification:
    let mut remove: BTreeSet<String> = vec![].into_iter().collect();
    let mut compressed_subjects = SerdeMap::new();
    for subject_id in subjects.keys() {
        let subject_id = subject_id.to_owned();
        let preds: SerdeMap<String, SerdeValue>;
        match subjects.get(&subject_id) {
            Some(SerdeValue::Object(m)) => preds = m.clone(),
            _ => preds = SerdeMap::new(),
        };

        if let None = compressed_subjects.get(&subject_id) {
            compressed_subjects.insert(subject_id.to_owned(), SerdeValue::Object(preds.clone()));
        };

        if preds.contains_key("owl:annotatedSource") {
            compress(
                "annotations",
                &subject_id,
                &subjects,
                &mut compressed_subjects,
                &mut remove,
                &preds,
                "owl:annotatedSource",
                "owl:annotatedProperty",
                "owl:annotatedTarget",
            );
        }

        if preds.contains_key("rdf:subject") {
            compress(
                "metadata",
                &subject_id,
                &subjects,
                &mut compressed_subjects,
                &mut remove,
                &preds,
                "rdf:subject",
                "rdf:predicate",
                "rdf:object",
            );
        }
    }

    // Remove the subject ids from compressed_subjects that we earlier identified for removal:
    for r in &remove {
        compressed_subjects.remove(r);
    }

    compressed_subjects
}

/// Convert the given SerdeMap, mapping Strings to SerdeValues, into a vector of SerdeMaps that map
/// Strings to SerdeValues.
fn subjects_to_thick_rows(
    subjects: &SerdeMap<String, SerdeValue>,
) -> Vec<SerdeMap<String, SerdeValue>> {
    let mut rows = vec![];
    for subject_id in subjects.keys() {
        let predicates: SerdeMap<String, SerdeValue>;
        match subjects.get(subject_id) {
            Some(SerdeValue::Object(p)) => predicates = p.clone(),
            _ => predicates = SerdeMap::new(),
        };

        for predicate in predicates.keys() {
            let objs: Vec<SerdeValue>;
            match predicates.get(predicate) {
                Some(SerdeValue::Array(v)) => objs = v.clone(),
                _ => objs = vec![],
            };

            for obj in objs {
                let mut result: SerdeMap<String, SerdeValue>;
                match obj {
                    SerdeValue::Object(m) => result = m.clone(),
                    _ => result = SerdeMap::new(),
                };
                result.insert(
                    String::from("subject"),
                    SerdeValue::String(subject_id.clone()),
                );
                result.insert(
                    String::from("predicate"),
                    SerdeValue::String(predicate.clone()),
                );
                if let Some(s) = result.get("object") {
                    match s {
                        SerdeValue::String(_) => (),
                        _ => {
                            let s = s.to_string();
                            result.insert(String::from("object"), SerdeValue::String(s));
                        }
                    };
                }
                rows.push(result);
            }
        }
    }
    rows
}

// TODO: using mutable global variables in this way requires the use of `unsafe` code blocks.
// We should find an alternative.
/// Given a predicates map, return a list of triples
static mut B_ID: usize = 0;
fn thick2triples(
    prefixes: &Vec<Prefix>,
    subject: &String,
    predicate: &String,
    thick_row: &SerdeMap<String, SerdeValue>,
) -> Vec<SerdeValue> {
    fn deprefix(prefixes: &Vec<Prefix>, content: &String) -> String {
        let v: Vec<&str> = content.split(':').collect();
        if v.len() == 2 {
            let prefix = v[0];
            let name = v[1];
            for p in prefixes {
                if p.prefix == prefix {
                    return format!("<{}{}>", p.base, name);
                }
            }
        }
        if content.contains("^^") || content.contains("@") {
            return content.clone();
        } else {
            return format!("\"\"\"{}\"\"\"", content.clone());
        }
    }

    fn create_node(prefixes: &Vec<Prefix>, content: &SerdeValue) -> SerdeValue {
        fn quote(token: &String) -> String {
            if token.contains("\n") {
                let token = {
                    if let Some(t) = token.strip_prefix("\"").and_then(|s| s.strip_suffix("\"")) {
                        t
                    } else {
                        token
                    }
                };
                return format!("\"\"\"{}\"\"\"", token);
            } else {
                return token.to_string();
            }
        }
        if let SerdeValue::String(s) = content {
            if s.starts_with("_:") {
                return content.clone();
            } else if s.starts_with("<") {
                return content.clone();
            } else if s.starts_with("http") {
                return SerdeValue::String(format!("\"\"\"{}\"\"\"", s));
            } else {
                return SerdeValue::String(deprefix(prefixes, s));
            }
        } else if let SerdeValue::Object(m) = content {
            if let (Some(SerdeValue::String(value)), Some(SerdeValue::String(language))) =
                (m.get("value"), m.get("language"))
            {
                return SerdeValue::String(format!("{}@{}", quote(value), language));
            } else if let (Some(SerdeValue::String(value)), Some(SerdeValue::String(datatype))) =
                (m.get("value"), m.get("datatype"))
            {
                return SerdeValue::String(format!("{}^^{}", quote(value), datatype));
            } else if let Some(SerdeValue::String(value)) = m.get("value") {
                return SerdeValue::String(format!("{}", quote(value)));
            } else {
                eprintln!("WARNING: could not interpret content map.");
                return SerdeValue::String(format!("{}", content));
            }
        }

        eprintln!("WARNING: could not interpret content.");
        return SerdeValue::String(format!("{}", content));
    }

    fn decompress(
        prefixes: &Vec<Prefix>,
        thick_row: &SerdeMap<String, SerdeValue>,
        target: &SerdeValue,
        target_type: &str,
        decomp_type: &str,
    ) -> SerdeMap<String, SerdeValue> {
        static ANNOTATIONS: phf::Map<&'static str, &'static str> = phf_map! {
            "subject" => "owl:annotatedSource",
            "predicate" => "owl:annotatedProperty",
            "object" => "owl:annotatedTarget",
        };
        static METADATA: phf::Map<&'static str, &'static str> = phf_map! {
            "subject" => "rdf:subject",
            "predicate" => "rdf:predicate",
            "object" => "rdf:object",
        };
        static SPO: phf::Map<&'static str, &'static phf::Map<&'static str, &'static str>> = phf_map! {
            "annotations" => &ANNOTATIONS,
            "metadata" => &METADATA,
        };

        let annodata_subj = SPO[decomp_type]["subject"];
        let annodata_pred = SPO[decomp_type]["predicate"];
        let annodata_obj = SPO[decomp_type]["object"];

        let mut target_map = SerdeMap::new();
        match target {
            SerdeValue::Object(m) => {
                if !m.contains_key("value") {
                    target_map.insert(
                        String::from(target_type),
                        SerdeValue::Array(predicate_map_to_triples(prefixes, m)),
                    );
                } else {
                    target_map.insert(String::from(target_type), target.clone());
                }
            }
            SerdeValue::String(_) => {
                target_map.insert(String::from(target_type), target.clone());
            }
            _ => {
                eprintln!("WARNING: unknown target");
            }
        }

        let mut subject_map = SerdeMap::new();
        if let Some(SerdeValue::String(s)) = thick_row.get("subject") {
            subject_map.insert(String::from("object"), SerdeValue::String(s.to_string()));
        } else {
            eprintln!("WARNING: unknown subject");
        }

        let mut predicate_map = SerdeMap::new();
        if let Some(SerdeValue::String(s)) = thick_row.get("predicate") {
            predicate_map.insert(String::from("object"), SerdeValue::String(s.to_string()));
        } else {
            eprintln!("WARNING: unknown predicate");
        }

        let mut object_type_map = SerdeMap::new();
        object_type_map.insert(String::from("object"), {
            if decomp_type == "annotations" {
                SerdeValue::String(String::from("owl:Axiom"))
            } else {
                SerdeValue::String(String::from("rdf:Statement"))
            }
        });

        let mut annodata = SerdeMap::new();
        annodata.insert(
            String::from(annodata_subj),
            SerdeValue::Array(vec![SerdeValue::Object(subject_map)]),
        );
        annodata.insert(
            String::from(annodata_pred),
            SerdeValue::Array(vec![SerdeValue::Object(predicate_map)]),
        );
        annodata.insert(
            String::from(annodata_obj),
            SerdeValue::Array(vec![SerdeValue::Object(target_map)]),
        );
        annodata.insert(
            String::from("rdf:type"),
            SerdeValue::Array(vec![SerdeValue::Object(object_type_map)]),
        );
        if let Some(SerdeValue::Object(m)) = thick_row.get(decomp_type) {
            for (key, val) in m.iter() {
                annodata.insert(key.to_string(), val.clone());
            }
        }
        return annodata;
    }

    fn predicate_map_to_triples(
        prefixes: &Vec<Prefix>,
        pred_map: &SerdeMap<String, SerdeValue>,
    ) -> Vec<SerdeValue> {
        let mut triples = vec![];
        let bnode = unsafe {
            B_ID += 1;
            format!("_:myb{}", B_ID)
        };
        for (predicate, objects) in pred_map.iter() {
            if let SerdeValue::Array(v) = objects {
                for obj in v {
                    if let SerdeValue::Object(m) = obj {
                        triples.append(&mut thick2triples(&prefixes, &bnode, &predicate, &m));
                    } else {
                        eprintln!("WARNING: This shouldn't have happened.");
                    }
                }
            }
        }
        triples
    }

    fn obj2triples(
        prefixes: &Vec<Prefix>,
        subject: &String,
        predicate: &String,
        thick_row: &SerdeMap<String, SerdeValue>,
    ) -> Vec<SerdeValue> {
        let mut triples = vec![];
        let target = thick_row.get("object");
        match target {
            Some(SerdeValue::Array(target)) => {
                for t in target {
                    if let SerdeValue::Object(t) = t {
                        let t_subject;
                        match t.get("subject") {
                            Some(SerdeValue::String(s)) => t_subject = s.clone(),
                            _ => t_subject = String::from(""),
                        };
                        let t_predicate;
                        match t.get("predicate") {
                            Some(SerdeValue::String(s)) => t_predicate = s.clone(),
                            _ => t_predicate = String::from(""),
                        };
                        triples.append(&mut thick2triples(prefixes, &t_subject, &t_predicate, &t));
                    }
                }
                let object = unsafe { format!("_:myb{}", B_ID - 1) };
                let mut triple = SerdeMap::new();
                triple.insert(
                    String::from("subject"),
                    create_node(&prefixes, &SerdeValue::String(subject.clone())),
                );
                triple.insert(
                    String::from("predicate"),
                    create_node(&prefixes, &SerdeValue::String(predicate.clone())),
                );
                triple.insert(
                    String::from("object"),
                    create_node(&prefixes, &SerdeValue::String(object)),
                );
                triples.push(SerdeValue::Object(triple));
            }
            Some(SerdeValue::Object(target)) => {
                let object = unsafe { format!("_:myb{}", B_ID + 1) };
                triples.append(&mut predicate_map_to_triples(prefixes, &target));
                let mut triple = SerdeMap::new();
                triple.insert(
                    String::from("subject"),
                    create_node(&prefixes, &SerdeValue::String(subject.clone())),
                );
                triple.insert(
                    String::from("predicate"),
                    create_node(&prefixes, &SerdeValue::String(predicate.clone())),
                );
                triple.insert(
                    String::from("object"),
                    create_node(&prefixes, &SerdeValue::String(object)),
                );
                triples.push(SerdeValue::Object(triple));
            }
            Some(SerdeValue::String(target)) => {
                let mut triple = SerdeMap::new();
                triple.insert(
                    String::from("subject"),
                    create_node(&prefixes, &SerdeValue::String(subject.clone())),
                );
                triple.insert(
                    String::from("predicate"),
                    create_node(&prefixes, &SerdeValue::String(predicate.clone())),
                );
                triple.insert(
                    String::from("object"),
                    create_node(&prefixes, &SerdeValue::String(target.clone())),
                );
                triples.push(SerdeValue::Object(triple));
            }
            _ => (),
        };

        if let Some(_) = thick_row.get("annotations") {
            if let Some(target) = target {
                triples.append(&mut predicate_map_to_triples(
                    prefixes,
                    &decompress(prefixes, thick_row, target, "object", "annotations"),
                ));
            }
        }

        if let Some(_) = thick_row.get("metadata") {
            if let Some(target) = target {
                triples.append(&mut predicate_map_to_triples(
                    prefixes,
                    &decompress(prefixes, thick_row, target, "object", "metadata"),
                ));
            }
        }

        triples
    }

    fn val2triples(
        prefixes: &Vec<Prefix>,
        subject: &String,
        predicate: &String,
        thick_row: &SerdeMap<String, SerdeValue>,
    ) -> Vec<SerdeValue> {
        let mut triples = vec![];
        let target;
        if let Some(value) = thick_row.get("value") {
            if let Some(SerdeValue::String(datatype)) = thick_row.get("datatype") {
                let mut target_map = SerdeMap::new();
                target_map.insert("value".to_string(), SerdeValue::String(value.to_string()));
                target_map.insert(
                    "datatype".to_string(),
                    SerdeValue::String(datatype.to_string()),
                );
                target = SerdeValue::Object(target_map);
            } else if let Some(SerdeValue::String(language)) = thick_row.get("language") {
                let mut target_map = SerdeMap::new();
                target_map.insert("value".to_string(), SerdeValue::String(value.to_string()));
                target_map.insert(
                    "language".to_string(),
                    SerdeValue::String(language.to_string()),
                );
                target = SerdeValue::Object(target_map);
            } else {
                target = value.clone();
            }

            let mut triple = SerdeMap::new();
            triple.insert(
                String::from("subject"),
                create_node(&prefixes, &SerdeValue::String(subject.clone())),
            );
            triple.insert(
                String::from("predicate"),
                create_node(&prefixes, &SerdeValue::String(predicate.clone())),
            );
            triple.insert(
                String::from("object"),
                create_node(&prefixes, &target.clone()),
            );
            triples.push(SerdeValue::Object(triple));

            if let Some(_) = thick_row.get("annotations") {
                triples.append(&mut predicate_map_to_triples(
                    prefixes,
                    &decompress(prefixes, thick_row, &target, "value", "annotations"),
                ));
            }

            if let Some(_) = thick_row.get("metadata") {
                triples.append(&mut predicate_map_to_triples(
                    prefixes,
                    &decompress(prefixes, thick_row, &target, "value", "metadata"),
                ));
            }

            return triples;
        } else {
            eprintln!("ERROR Unable to retrieve value from thick row");
            return triples;
        }
    }

    if let Some(_) = thick_row.get("object") {
        return obj2triples(prefixes, subject, predicate, thick_row);
    } else if let Some(_) = thick_row.get("value") {
        return val2triples(prefixes, subject, predicate, thick_row);
    } else {
        eprintln!("ERROR could not find either an object or a value in thick_row");
        return vec![];
    }
}

fn thicks2triples(
    prefixes: &Vec<Prefix>,
    thick_rows: &Vec<SerdeMap<String, SerdeValue>>,
) -> Vec<SerdeValue> {
    let mut triples = vec![];
    for row in thick_rows {
        let mut row = row.clone();
        if let Some(SerdeValue::String(s)) = row.get("object") {
            if s.starts_with("{") {
                if let Ok(val) = serde_json::from_str(s) {
                    row.insert(String::from("object"), val);
                }
            }
        }
        let subject;
        match row.get("subject") {
            Some(SerdeValue::String(s)) => subject = s.clone(),
            _ => subject = String::from(""),
        };
        let predicate;
        match row.get("predicate") {
            Some(SerdeValue::String(s)) => predicate = s.clone(),
            _ => predicate = String::from(""),
        };
        triples.append(&mut thick2triples(&prefixes, &subject, &predicate, &row));
    }
    triples
}

fn insert(db: &String, round_trip: bool) -> Result<(), Box<dyn Error>> {
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
    let mut thin_rows_by_stanza: BTreeMap<String, Vec<_>> = BTreeMap::new();
    eprintln!("Parsing thin rows ...");
    RdfXmlParser::new(stdin.lock(), Some(Iri::parse(filename.to_owned()).unwrap()))
        .parse_all(&mut |t| {
            if t.subject == stanza_end {
                let mut stanza_rows: Vec<_> = vec![];
                for mut row in thinify(&mut stack, &mut stanza) {
                    if row.len() != 7 {
                        row.resize_with(7, Default::default);
                    }
                    stanza_rows.push(row);
                }
                if let Some(v) = thin_rows_by_stanza.get_mut(&stanza) {
                    v.append(&mut stanza_rows);
                } else {
                    thin_rows_by_stanza.insert(stanza.to_owned(), stanza_rows);
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

    eprintln!("Converting thin rows to thick ...");
    let mut thick_rows: Vec<_> = vec![];
    for (_, thin_rows) in thin_rows_by_stanza.iter() {
        let subjects = annotate_reify(thin_rows_to_subjects(&thin_rows));
        thick_rows.append(&mut subjects_to_thick_rows(&subjects));
    }

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
                    Some(SerdeValue::String(s)) => row.push(Some(s)),
                    None => row.push(None),
                    _ => (),
                };
            }
            rows.push(row);
        }
        rows
    };

    eprintln!("Inserting thick rows to db ...");
    for row in rows_to_insert {
        let mut stmt = tx
            .prepare_cached("INSERT INTO statements values (?1, ?2, ?3, ?4, ?5, ?6)")
            .expect("Statement ok");
        stmt.execute(row).expect("Insert row");
    }

    tx.commit()?;

    if round_trip {
        eprintln!("Generating triples for round-trip comparison ...");
        let triples = thicks2triples(&prefixes, &thick_rows);
        for prefix in prefixes {
            println!("@prefix {}: <{}> .", prefix.prefix, prefix.base)
        }
        for triple in triples {
            match triple.get("subject") {
                Some(SerdeValue::String(s)) => print!("{} ", s),
                _ => print!(r#""" "#),
            };
            match triple.get("predicate") {
                Some(SerdeValue::String(p)) => print!("{} ", p),
                _ => print!(r#""" "#),
            };
            match triple.get("object") {
                Some(SerdeValue::String(o)) => println!("{} .", o),
                _ => println!(r#""""#),
            };
        }
    }

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let usage = "Usage: rdftab [-h|--help] [-r|--round-trip] TARGET.db";
    match args.get(1) {
        None => {
            eprintln!("You must specify a target database file.");
            eprintln!("{}", usage);
            process::exit(1);
        }
        Some(i) => {
            if i.eq("--help") || i.eq("-h") {
                eprintln!("{}", usage);
                process::exit(0);
            }

            let round_trip;
            let db;
            if i.eq("--round-trip") || i.eq("-r") {
                round_trip = true;
                match args.get(2) {
                    Some(_) => {
                        db = &args[2];
                    }
                    None => {
                        eprintln!("You must specify a target database file.");
                        eprintln!("{}", usage);
                        process::exit(1);
                    }
                };
            } else {
                round_trip = false;
                db = &args[1];
            }

            if let Err(err) = insert(db, round_trip) {
                eprintln!("{}", err);
                process::exit(1);
            }
        }
    }
}
