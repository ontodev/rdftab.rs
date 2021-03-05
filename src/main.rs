// Based on https://docs.rs/csv/1.1.3/csv/tutorial/index.html
use std::env;
use std::error::Error;
use std::io;
use std::process;

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

fn row2o<'a>(
    uber_row: &'a Vec<Option<String>>,
    stanza_stack: &'a Vec<Vec<Option<String>>>,
    stanza_name: &'a String,
) -> &'a Vec<Option<String>> {
    // Start with any row that describes a non-blank subject:
    //let uber_row = stanza_stack
    //    .iter()
    //    .find(|&r| !r[0].as_ref().unwrap().starts_with("_:"));
    //eprintln!("UBER ROW: {:?}", uber_row);
    let uber_subj = uber_row[0].as_ref().unwrap();
    let uber_pred = &uber_row[1].as_ref().unwrap();

    // TODO: Need to chek for none here:
    let uber_obj = String::from("foobie"); // &uber_row[2].as_ref().unwrap();
    eprintln!("Uber row items: {}, {}, {}", uber_subj, uber_pred, uber_obj);

    uber_row
}

fn get_rows_to_insert(
    stanza_stack: &mut Vec<Vec<Option<String>>>,
    stanza_name: &mut String,
) -> Vec<Vec<Option<String>>> {
    let mut rows: Vec<Vec<Option<String>>> = [].to_vec();
    //eprintln!("Thickening stanza {}", stanza_name);

    eprintln!("Stanza is: {}", stanza_name);
    for s in stanza_stack.iter() {
        //eprintln!("Thin row: {:?}", s);
        if stanza_name == "" {
            if let Some(ref sb) = s[1] {
                *stanza_name = sb.clone();
                eprintln!("Changing stanza name to {}", stanza_name);
            }
        }
        let mut v = vec![Some(stanza_name.to_string())];
        let s = row2o(&s, &stanza_stack, &stanza_name);
        v.extend_from_slice(&s);
        eprintln!("Inserting row: {:?}", v);
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
        .unwrap()
        .parse_all(&mut |t| {
            //eprintln!("Processing triple: {}", t);
            if t.subject == stanza_end {
                //eprintln!("Reached the end of the stanza: {}", stanza);
                for row in get_rows_to_insert(&mut stack, &mut stanza) {
                    let mut stmt = tx
                        .prepare_cached(
                            "INSERT INTO statements values (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        )
                        .expect("Statement ok");
                    stmt.execute(row).expect("Insert row");
                }
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
