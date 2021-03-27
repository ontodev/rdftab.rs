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

// TODO: After the current iteration, which is a pared-down version of `tree.py` from gizmos,
// look at https://github.com/ontodev/gizmos/pull/77 and try implementing that logic (or parts of
// it) instead. That logic begins by constructing a subject map as a first step, and then uses it
// to construct the predicate map in the second step.

#[derive(Debug)]
struct Prefix {
    prefix: String,
    base: String,
}

#[derive(Serialize, Debug)]
enum RDFObject {
    Nested(Vec<HashMap<String, RDFObject>>),
    Flat(String),
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

fn render_owl_restriction(
    stanza_rows: &Vec<Vec<Option<String>>>, given_rows: Vec<&Vec<Option<String>>>,
) -> HashMap<String, RDFObject> {
    // TODO: It would be good to somehow refactor all of these function calls. Everything except
    // for the closure is the same.
    let target_row = {
        if let Some(target_row) = given_rows
            .iter()
            .find(|r| {
                (r[1] != Some(String::from("rdf:type")))
                    && (r[1] != Some(String::from("owl:onProperty")))
            })
            .map(|&r| r.clone())
        {
            target_row
        }
        else {
            let v = vec![];
            v
        }
    };
    let property_row = {
        if let Some(property_row) = given_rows
            .iter()
            .find(|r| r[1] == Some(String::from("owl:onProperty")))
            .map(|&r| r.clone())
        {
            property_row
        }
        else {
            let v = vec![];
            v
        }
    };
    let rdf_type_row = {
        if let Some(rdf_type_row) = given_rows
            .iter()
            .find(|r| r[1] == Some(String::from("rdf:type")))
            .map(|&r| r.clone())
        {
            rdf_type_row
        }
        else {
            let v = vec![];
            v
        }
    };
    let rdf_type = {
        if let Some(rdf_type) = rdf_type_row.get(2).and_then(|r| r.clone()) {
            rdf_type
        }
        else {
            String::from("")
        }
    };

    if rdf_type != "owl:Restriction" {
        eprintln!(
            "ERROR Unexpected rdf type: '{}' found in OWL restriction",
            rdf_type
        );
        return HashMap::new();
    }

    let target_pred = {
        if let Some(target_pred) = target_row.get(1).and_then(|r| r.clone()) {
            target_pred
        }
        else {
            String::from("")
        }
    };
    let target_obj = {
        if let Some(target_obj) = target_row.get(2).and_then(|r| r.clone()) {
            target_obj
        }
        else {
            String::from("")
        }
    };

    eprintln!(
        "Rendering OWL restriction for {} for object {}",
        target_pred, target_obj
    );

    let mut restriction = HashMap::new();
    restriction.insert(String::from("object"), {
        let mut tmp_map = HashMap::new();
        tmp_map.insert(String::from("object"), RDFObject::Flat(rdf_type));
        RDFObject::Nested(vec![tmp_map])
    });

    if target_obj.starts_with("_:") {
        let inner_rows: Vec<_> = stanza_rows
            .iter()
            .filter(move |&r| r[0].as_ref() == Some(&target_obj))
            .collect();
        let ce = render_owl_class_expression(stanza_rows, inner_rows);
        let ce = match to_string(&ce) {
            Ok(ce) => ce,
            Err(_) => String::from(""),
        };
        restriction.insert(String::from("object"), {
            let mut tmp_map = HashMap::new();
            tmp_map.insert(String::from("object"), RDFObject::Flat(ce));
            RDFObject::Nested(vec![tmp_map])
        });
        //eprintln!("vvvvvvvvvvvvvvvvvv {:?} vvvvvvvvvvvvvvvv", restriction);
        //if 1 == 1 { process::exit(1); }
    }
    else {
        restriction.insert(String::from("object"), {
            let mut tmp_map = HashMap::new();
            tmp_map.insert(String::from("object"), RDFObject::Flat(target_obj));
            RDFObject::Nested(vec![tmp_map])
        });
        //eprintln!("llllllllllllllllll {:?} llllllllllllllll", restriction);
        //if 1 == 1 { process::exit(1); }
    }
    return restriction;
}

fn get_owl_operands(
    stanza_rows: &Vec<Vec<Option<String>>>, given_row: &Vec<Option<String>>,
) -> RDFObject {
    let given_pred = {
        if let Some(given_pred) = given_row.get(1).and_then(|r| r.clone()) {
            given_pred
        }
        else {
            String::from("")
        }
    };
    let given_obj = {
        if let Some(given_obj) = given_row.get(2).and_then(|r| r.clone()) {
            given_obj
        }
        else {
            String::from("")
        }
    };

    eprintln!("Finding operands for row with predicate: {}", given_pred);

    if !given_obj.starts_with("_:") {
        eprintln!("Found non-blank operand: {}", given_obj);

        let mut non_blank = HashMap::new();
        non_blank.insert(String::from("object"), {
            let mut tmp_map = HashMap::new();
            tmp_map.insert(String::from("object"), RDFObject::Flat(given_obj));
            RDFObject::Nested(vec![tmp_map])
        });
        let non_blank: RDFObject = RDFObject::Nested(vec![non_blank]);
        return non_blank;
    }

    let inner_rows: Vec<_> = {
        stanza_rows
            .iter()
            .filter(|r| r[0] == Some(given_obj.clone()))
            .collect()
    };

    let mut operands = vec![];
    for inner_row in inner_rows.iter() {
        let inner_subj = {
            if let Some(inner_subj) = inner_row.get(0).and_then(|r| r.clone()) {
                inner_subj
            }
            else {
                String::from("")
            }
        };
        let inner_pred = {
            if let Some(inner_pred) = inner_row.get(1).and_then(|r| r.clone()) {
                inner_pred
            }
            else {
                String::from("")
            }
        };
        let inner_obj = {
            if let Some(inner_obj) = inner_row.get(2).and_then(|r| r.clone()) {
                inner_obj
            }
            else {
                String::from("")
            }
        };

        eprintln!(
            "Found row with <s,p,o> = <{}, {}, {}>",
            inner_subj, inner_pred, inner_obj
        );

        if inner_pred == "rdf:type" {
            if inner_obj == "owl:Restriction" {
                let operand = render_owl_restriction(stanza_rows, inner_rows);
                //eprintln!("<<<<<<<<<<< {:?} >>>>>>>>>>>", operand);
                //if 1 == 1 { process::exit(1); }
                operands.push(operand);
                break;
            }
            else if inner_obj == "owl:Class" {
                let ce = render_owl_class_expression(stanza_rows, inner_rows);
                let ce = match to_string(&ce) {
                    Ok(ce) => ce,
                    Err(_) => String::from(""),
                };
                let mut operand: HashMap<String, RDFObject> = HashMap::new();
                operand.insert(String::from("object"), {
                    let mut tmp_map = HashMap::new();
                    tmp_map.insert(String::from("object"), RDFObject::Flat(ce));
                    RDFObject::Nested(vec![tmp_map])
                });
                operands.push(operand);
                break;
            }
        }
        else if inner_pred == "rdf:rest" {
            if inner_obj != "rdf:nil" {
                let inner_operands = get_owl_operands(stanza_rows, inner_row);
                operands.push({
                    let mut tmp_map = HashMap::new();
                    tmp_map.insert(inner_pred.clone(), inner_operands);
                    tmp_map
                });
            }
            eprintln!("Returned from recursing on {}", inner_pred);
        }
        else if inner_pred == "rdf:first" {
            if inner_obj.starts_with("_:") {
                eprintln!("{} points to a blank node, following the trail", inner_pred);
                let inner_operands = get_owl_operands(stanza_rows, inner_row);
                operands.push({
                    let mut tmp_map = HashMap::new();
                    tmp_map.insert(inner_pred.clone(), inner_operands);
                    tmp_map
                });
                eprintln!("Returned from recursing on {}", inner_pred);
            }
            else {
                eprintln!("Rendering non-blank object with predicate: {}", inner_pred);
                let mut operand = HashMap::new();
                operand.insert(inner_pred.clone(), {
                    let mut tmp_map = HashMap::new();
                    tmp_map.insert(String::from("object"), RDFObject::Flat(inner_obj));
                    RDFObject::Nested(vec![tmp_map])
                });
                operands.push(operand);
            }
        }
    }

    eprintln!("********************* {:?} ****************", operands);
    //if 1 == 1 { process::exit(1); }

    let operands = RDFObject::Nested(operands);
    return operands;
}

fn render_owl_class_expression(
    stanza_rows: &Vec<Vec<Option<String>>>, given_rows: Vec<&Vec<Option<String>>>,
) -> HashMap<String, RDFObject> {
    let class_row = {
        if let Some(class_row) = given_rows
            .iter()
            .find(|r| match &r[1] {
                Some(pred) => pred.starts_with("owl:"),
                None => false,
            })
            .map(|&r| r.clone())
        {
            class_row
        }
        else {
            let v = vec![];
            v
        }
    };

    let rdf_type_rows: Vec<_> = {
        given_rows
            .iter()
            .filter(|r| r[1] == Some(String::from("rdf:type")))
            .collect()
    };

    eprintln!("Found rows: {:?}, {:?}", rdf_type_rows, class_row);

    let rdf_type_objs: Vec<_> = {
        rdf_type_rows
            .iter()
            .map(|r| r[2].clone())
            .map(|c| {
                if let Some(obj) = c {
                    obj
                }
                else {
                    String::from("")
                }
            })
            .collect()
    };

    let mut rdf_part = vec![];
    for obj in rdf_type_objs.iter() {
        rdf_part.push({
            // TODO: Make it so that these are added in sorted order:
            let mut obj_map = HashMap::new();
            obj_map.insert(String::from("object"), RDFObject::Flat(obj.clone()));
            obj_map
        });
    }
    let rdf_part = RDFObject::Nested(rdf_part);

    // Add the RDF part to the class expression:
    let mut ce = HashMap::new();
    ce.insert(String::from("rdf:type"), rdf_part);

    let class_subj = {
        if let Some(class_subj) = class_row.get(0).and_then(|r| r.clone()) {
            class_subj
        }
        else {
            String::from("")
        }
    };
    let class_pred = {
        if let Some(class_pred) = class_row.get(1).and_then(|r| r.clone()) {
            class_pred
        }
        else {
            String::from("")
        }
    };
    let class_obj = {
        if let Some(class_obj) = class_row.get(2).and_then(|r| r.clone()) {
            class_obj
        }
        else {
            String::from("")
        }
    };

    eprintln!(
        "Rendering <s,p,o> = <{}, {}, {}>",
        class_subj, class_pred, class_obj
    );

    let operands = get_owl_operands(stanza_rows, &class_row);
    if vec![
        "owl:intersectionOf",
        "owl:unionOf",
        "owl:complementOf",
        "owl:oneOf",
    ]
    .iter()
    .any(|&i| i == class_pred.as_str())
    {
        ce.insert(class_pred, operands);
    }
    else if class_pred.as_str() == "owl:onProperty" {
        ce.insert(class_pred, {
            let property = vec![render_owl_restriction(stanza_rows, given_rows)];
            RDFObject::Nested(property)
        });
    }
    else if class_obj.starts_with("<") {
        ce.insert(class_pred, {
            let mut tmp_map = HashMap::new();
            tmp_map.insert(String::from("object"), {
                let mut tmp_map = HashMap::new();
                tmp_map.insert(String::from("object"), RDFObject::Flat(class_obj));
                RDFObject::Nested(vec![tmp_map])
            });
            RDFObject::Nested(vec![tmp_map])
        });
    }

    //eprintln!("..................... {:?} ...................", ce);
    //if 1 == 1 { process::exit(1); }

    return ce;
}

fn row2o(
    uber_row: &Vec<Option<String>>, stanza_rows: &Vec<Vec<Option<String>>>,
) -> Vec<Option<String>> {
    // A handy closure for getting the value of the ith column of the uber row:
    let get_uber_column = |index: usize| -> &str {
        match &uber_row[index].as_ref() {
            Some(obj) => obj,
            None => "",
        }
    };

    let uber_subj = String::from(get_uber_column(0));
    let uber_pred = String::from(get_uber_column(1));
    let uber_obj = String::from(get_uber_column(2));
    let uber_val = String::from(get_uber_column(3));
    eprintln!(
        "Called row2o on <s,p,o> = <{}, {}, {}>",
        uber_subj, uber_pred, uber_obj
    );

    if uber_obj.is_empty() {
        if uber_val.is_empty() {
            eprintln!("ERROR: Received empty object with empty value");
        }
        else {
            eprintln!("Rendering empty object with value: {}", uber_val);
        }
        return uber_row.clone();
    }
    else if uber_obj.starts_with("<") {
        eprintln!("Rendering literal IRI: {}", uber_obj);
        return uber_row.clone();
    }
    else if !uber_obj.starts_with("_:") {
        eprintln!(
            "Rendering non-blank triple: <s,p,o> = <{}, {}, {}>",
            uber_subj, uber_pred, uber_obj
        );
        return uber_row.clone();
    }
    else {
        eprintln!(
            "Rendering triple with blank object: <s,p,o> = <{}, {}, {}>",
            uber_subj, uber_pred, uber_obj
        );

        let inner_rows: Vec<_> = {
            stanza_rows
                .iter()
                .filter(|&r| r[0].as_ref() == Some(&uber_obj))
                .collect()
        };

        let object_type_row = {
            if let Some(object_type_row) = inner_rows
                .iter()
                .find(|r| r[1] == Some(String::from("rdf:type")))
                .map(|&r| r.clone())
            {
                object_type_row
            }
            else {
                let v = vec![];
                v
            }
        };
        let object_type = {
            if let Some(object_type) = object_type_row.get(2).and_then(|r| r.clone()) {
                object_type
            }
            else {
                String::from("")
            }
        };

        match object_type.as_str() {
            "owl:Class" => {
                eprintln!("Rendering OWL class pointed to by {}", uber_obj);
                let ce = render_owl_class_expression(stanza_rows, inner_rows);
                let ce = match to_string(&ce) {
                    Ok(ce) => ce,
                    Err(_) => String::from(""),
                };
                let mut row_to_return = uber_row.clone();
                row_to_return[2] = Some(ce);
                return row_to_return;
            }
            "owl:Restriction" => {
                eprintln!("Rendering OWL restriction pointed to by {}", uber_obj);
                let restr = render_owl_restriction(stanza_rows, inner_rows);
                let restr = match to_string(&restr) {
                    Ok(restr) => restr,
                    Err(_) => String::from(""),
                };
                let mut row_to_return = uber_row.clone();
                row_to_return[2] = Some(restr);
                return row_to_return;
            }
            "" => {
                eprintln!("WARNING Could not determine object type for {}", uber_pred);
                return uber_row.clone();
            }
            _ => {
                eprintln!(
                    "WARNING Unrecognised object type: {} for predicate {}",
                    object_type, uber_pred
                );
                return uber_row.clone();
            }
        }
    }
}

fn get_rows_to_insert(
    stanza_stack: &mut Vec<Vec<Option<String>>>, stanza_name: &mut String,
) -> Vec<Vec<Option<String>>> {
    let mut rows: Vec<Vec<Option<String>>> = [].to_vec();
    for s in stanza_stack.iter() {
        if stanza_name == "" {
            if let Some(ref sb) = s[1] {
                *stanza_name = sb.clone();
                eprintln!("Changing stanza name to {}", stanza_name);
            }
        }
        let mut v = vec![Some(stanza_name.to_string())];
        let s = row2o(&s, &stanza_stack);
        v.extend_from_slice(&s);
        rows.push(v);
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
