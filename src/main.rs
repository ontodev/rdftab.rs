// Based on https://docs.rs/csv/1.1.3/csv/tutorial/index.html
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::io;
use std::process;

use phf::phf_map;
//use regex::Regex;
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
    let objs = predicates.get(predicate);
    match objs {
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
    eprintln!("No object found");
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
    eprintln!("<S, P, O> = <{}, {}, {:?}>", subject, predicate, obj);

    if let Some(SerdeValue::Object(m)) = compressed_subjects.get_mut(subject_id) {
        m.remove(subject_type);
        m.remove(predicate_type);
        m.remove(object_type);
        m.remove("rdf:type");
    }

    if let None = compressed_subjects.get(&subject) {
        let preds: SerdeMap<String, SerdeValue>;
        match subjects.get(&subject) {
            Some(SerdeValue::Object(m)) => preds = m.clone(),
            _ => preds = SerdeMap::new(),
        };
        compressed_subjects.insert(subject.to_string(), SerdeValue::Object(preds));
    }
    // We are assured compressed_preds will not be None because of the code immediately above, so
    // we simply call unwrap() here:
    let compressed_preds = compressed_subjects.get_mut(&subject).unwrap();
    if let None = compressed_preds.get(&predicate) {
        let compressed_objs: SerdeValue;
        match preds.get(&predicate) {
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
                let new_preds = match compressed_subjects.get(subject_id) {
                    Some(p) => p.clone(),
                    None => SerdeValue::Object(SerdeMap::new()),
                };
                let mut m = match o {
                    SerdeValue::Object(m) => m.clone(),
                    _ => SerdeMap::new(),
                };
                m.insert(kind.to_string(), new_preds);
                o = SerdeValue::Object(m);
                remove.insert(subject_id.to_string());
            }
            objs_copy.push(o);
        }

        // TODO: Make this code less ugly:
        let mut empty_array = SerdeValue::Array(vec![]);
        let preds_tmp = compressed_subjects.get_mut(&subject);
        let objs_tmp = match preds_tmp {
            Some(SerdeValue::Object(m)) => m.get_mut(&predicate),
            _ => Some(&mut empty_array),
        };
        *objs_tmp.unwrap() = SerdeValue::Array(objs_copy);
    }
}

/// Given a vector of thin rows, return a map from Strings to SerdeValues
fn thin_rows_to_subjects(thin_rows: &Vec<Vec<Option<String>>>) -> SerdeMap<String, SerdeValue> {
    let mut subjects = SerdeMap::new();
    let mut dependencies: BTreeMap<String, BTreeSet<_>> = BTreeMap::new();
    let mut subject_ids: BTreeSet<String> = vec![].into_iter().collect();
    for row in thin_rows.iter() {
        subject_ids.insert(row[1].clone().unwrap_or(String::from("")));
    }

    eprintln!("Converting subject ids to subjects map ...");
    let num_subjs = subject_ids.len();
    for (i, subject_id) in subject_ids.iter().enumerate() {
        let mut predicates = SerdeMap::new();
        for row in thin_rows.iter() {
            if subject_id.to_string() != get_cell_contents(row[1].as_ref()) {
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
                    dependencies.insert(subject_id.to_string(), v);
                }
            }
        }

        // Add an entry mapping `subject_id` to the predicates map in the subjects map:
        subjects.insert(subject_id.to_string(), SerdeValue::Object(predicates));
        if i != 0 && (i % 500) == 0 {
            eprintln!("Converted {} subject ids out of {} ...", i + 1, num_subjs);
        }
    }

    // Work through dependencies from leaves to root, nesting the blank structures:
    eprintln!("Working through dependencies ...");
    while !dependencies.is_empty() {
        let mut leaves: BTreeSet<_> = vec![].into_iter().collect();
        for leaf in subjects.keys() {
            if !dependencies.keys().collect::<Vec<_>>().contains(&leaf) {
                leaves.insert(leaf.clone());
            }
        }

        dependencies.clear();
        let mut handled = BTreeSet::new();
        let num_subjs = subjects.keys().len();
        for (i, subject_id) in subjects
            .keys()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .iter()
            .enumerate()
        {
            let mut predicates: SerdeMap<String, SerdeValue>;
            match subjects.get(subject_id) {
                Some(SerdeValue::Object(m)) => predicates = m.clone(),
                _ => predicates = SerdeMap::new(),
            };

            let num_preds = predicates.keys().len();
            for (j, predicate) in predicates
                .keys()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .iter()
                .enumerate()
            {
                let pred_objs: Vec<SerdeValue>;
                match predicates.get(predicate) {
                    Some(SerdeValue::Array(v)) => pred_objs = v.clone(),
                    _ => pred_objs = vec![],
                };

                let num_pred_objs = pred_objs.len();
                let mut objects = vec![];
                for (k, obj) in pred_objs.iter().enumerate() {
                    let mut obj = obj.clone();
                    let o: SerdeValue;
                    if let Some(val) = obj.get(&String::from("object")) {
                        o = val.clone();
                    } else {
                        o = SerdeValue::Object(SerdeMap::new());
                    }

                    match o {
                        SerdeValue::String(o) => {
                            if o.starts_with("_:") {
                                if leaves.contains(&o) {
                                    let val: SerdeValue;
                                    if let Some(v) = subjects.get(&o) {
                                        val = v.clone();
                                    } else {
                                        val = SerdeValue::Object(SerdeMap::new());
                                    }

                                    if let SerdeValue::Object(ref mut m) = obj {
                                        m.clear();
                                        m.insert(String::from("object"), val.clone());
                                        handled.insert(o);
                                    }
                                } else {
                                    if let Some(v) = dependencies.get_mut(subject_id) {
                                        v.insert(o);
                                    } else {
                                        let mut v = BTreeSet::new();
                                        v.insert(o);
                                        dependencies.insert(subject_id.to_string(), v);
                                    }
                                }
                            }
                        }
                        _ => (),
                    }
                    objects.push(obj);
                    if k != 0 && (k % 100) == 0 {
                        eprintln!("Converted {} objects ({} total) ...", k + 1, num_pred_objs);
                    }
                }
                objects.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                predicates.insert(predicate.to_string(), SerdeValue::Array(objects));
                subjects.insert(
                    subject_id.to_string(),
                    SerdeValue::Object(predicates.clone()),
                );
                if j != 0 && (j % 100) == 0 {
                    eprintln!("Converted {} predicates ({} total) ...", j + 1, num_preds);
                }
            }
            if i != 0 && (i % 100) == 0 {
                eprintln!("Converted {} subject ids ({} total) ...", i + 1, num_subjs);
            }
        }
        for subject_id in &handled {
            subjects.remove(subject_id);
        }
    }

    // OWL annotation and RDF reification:
    eprintln!("Doing OWL annotation and RDF reification ...");
    let mut remove: BTreeSet<String> = vec![].into_iter().collect();
    let mut compressed_subjects = SerdeMap::new();
    for subject_id in subjects.keys() {
        let subject_id = subject_id.to_string();
        let preds: SerdeMap<String, SerdeValue>;
        match subjects.get(&subject_id) {
            Some(SerdeValue::Object(m)) => preds = m.clone(),
            _ => preds = SerdeMap::new(),
        };

        if let None = compressed_subjects.get(&subject_id) {
            compressed_subjects.insert(subject_id.to_string(), SerdeValue::Object(preds.clone()));
        };

        if preds.contains_key("owl:annotatedSource") {
            eprintln!("OWL annotation {}", subject_id);
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
            eprintln!("RDF Reification {}", subject_id);
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
    for r in remove.iter() {
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
    eprintln!(
        "In thick2obj. Received thick row: {}",
        SerdeValue::Object(thick_row.clone())
    );

    fn create_node(prefixes: &Vec<Prefix>, content: &SerdeValue) -> SerdeValue {
        if let SerdeValue::String(s) = content {
            return SerdeValue::String(format!("{}", s));
        } else if let SerdeValue::Object(m) = content {
            if let (Some(SerdeValue::String(value)), Some(SerdeValue::String(language))) =
                (m.get("value"), m.get("language"))
            {
                return SerdeValue::String(format!(r#""{}"@{}"#, value, language));
            } else if let (Some(SerdeValue::String(value)), Some(SerdeValue::String(datatype))) =
                (m.get("value"), m.get("datatype"))
            {
                return SerdeValue::String(format!(r#""{}"^^{}"#, value, datatype));
            } else if let Some(SerdeValue::String(value)) = m.get("value") {
                return SerdeValue::String(format!("{}", value));
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
                triples.append(&mut predicate_map_to_triples(prefixes, &target));
                let object = unsafe { format!("_:myb{}", B_ID + 1) };
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
        if let Some(SerdeValue::String(value)) = thick_row.get("value") {
            let mut target = SerdeMap::new();
            target.insert("value".to_string(), SerdeValue::String(value.to_string()));
            if let Some(SerdeValue::String(datatype)) = thick_row.get("datatype") {
                target.insert(
                    "datatype".to_string(),
                    SerdeValue::String(datatype.to_string()),
                );
            } else if let Some(SerdeValue::String(language)) = thick_row.get("language") {
                target.insert(
                    "language".to_string(),
                    SerdeValue::String(language.to_string()),
                );
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
                create_node(&prefixes, &SerdeValue::Object(target.clone())),
            );
            triples.push(SerdeValue::Object(triple));

            let target = SerdeValue::Object(target);
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
        }
        triples
    }

    if let Some(_) = thick_row.get("object") {
        return obj2triples(prefixes, subject, predicate, thick_row);
    } else if let Some(_) = thick_row.get("value") {
        return val2triples(prefixes, subject, predicate, thick_row);
    } else {
        eprintln!("ERROR!! {:?}", thick_row);
        return vec![];
    }
}

fn thicks2triples(
    prefixes: &Vec<Prefix>,
    thick_rows: &Vec<SerdeMap<String, SerdeValue>>,
) -> Vec<SerdeValue> {
    eprintln!("In thicks2triples. Received thick_rows: {:?}", thick_rows);
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
    eprintln!("Parsing thin rows ...");
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

    eprintln!("Converting thin rows to subjects ...");
    let subjects = thin_rows_to_subjects(&thin_rows);

    eprintln!("Converting subjects to thick rows ...");
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

    let triples = thicks2triples(&prefixes, &thick_rows);
    let mut triples_output = String::from("");
    for triple in triples {
        match triple.get("subject") {
            Some(SerdeValue::String(s)) => triples_output.push_str(&format!("{} ", s)),
            _ => triples_output.push_str(r#""""#),
        };
        match triple.get("predicate") {
            Some(SerdeValue::String(p)) => triples_output.push_str(&format!("{} ", p)),
            _ => triples_output.push_str(r#""""#),
        };
        match triple.get("object") {
            Some(SerdeValue::String(o)) => triples_output.push_str(&format!("{}\n", o)),
            _ => triples_output.push_str(r#""""#),
        };
    }

    eprintln!("---------- Triples (after round-trip) ----------");
    println!("{}", triples_output);

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let usage = "Usage: rdftab [-h|--help] TARGET.db";
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
            } else if i.starts_with("-") {
                eprintln!("Unknown option: {}", i);
                eprintln!("{}", usage);
                process::exit(1);
            }

            let db = &args[1];
            if let Err(err) = insert(db) {
                eprintln!("{}", err);
                process::exit(1);
            }
        }
    }
}
