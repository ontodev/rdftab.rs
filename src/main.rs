// Based on https://docs.rs/csv/1.1.3/csv/tutorial/index.html
use std::error::Error;
use std::env;
use std::io;
use std::process;

use rio_xml::{RdfXmlParser, RdfXmlError};
use rio_api::parser::TriplesParser;
use rio_api::model::*;

use rusqlite::{params, Connection, Result};

#[derive(Debug)]
struct Prefix {
    prefix: String,
    base: String
}

fn get_prefixes(conn: &mut Connection) -> Result<Vec<Prefix>> {
    let mut stmt = conn.prepare("SELECT prefix, base FROM prefix ORDER BY length(base) DESC")?;
    let mut rows = stmt.query(params![])?;
    let mut prefixes = Vec::new();
    while let Some(row) = rows.next()? {
        prefixes.push(Prefix { prefix: row.get(0)?, base: row.get(1)? });
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

fn insert(db: &String) -> Result<(), Box<dyn Error>> {
    let stanza_end = NamedOrBlankNode::from(NamedNode { iri: "http://example.com/stanza-end" }).into();
    let annotated_source = NamedNode { iri: "http://www.w3.org/2002/07/owl#annotatedSource" };
    let stdin = io::stdin();
    let mut stack: Vec<Vec<Option<String>>> = Vec::new();
    let mut stanza = String::from("");
    let mut conn = Connection::open(db)?;
    let prefixes = get_prefixes(&mut conn).expect("Get prefixes");
    let tx = conn.transaction()?;
    tx.execute("CREATE TABLE IF NOT EXISTS statements (
      stanza TEXT,
      subject TEXT,
      predicate TEXT,
      object TEXT,
      value TEXT,
      datatype TEXT,
      language TEXT
    )", params![])?;
    let filename = format!("file:{}", db);
    RdfXmlParser::new(stdin.lock(), filename.as_str()).unwrap().parse_all(&mut |t| {
        if t.subject == stanza_end {
            while stack.len() > 0 {
                if let Some(s) = stack.pop() {
                    if stanza == "" {
                        if let Some(ref sb) = s[1] {
                            stanza = sb.clone();
                        }
                    }
                    let mut v = vec![Some(stanza.to_string())];
                    v.extend_from_slice(&s);
                    let mut stmt = tx.prepare_cached("INSERT INTO statements values (?1, ?2, ?3, ?4, ?5, ?6, ?7)").expect("Statement ok");
                    stmt.execute(v).expect("Insert row");
                }
            }
            stanza = String::from("")
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
                    Literal::Typed { value, datatype } => (None, Some(value.to_string()), Some(shorten(&prefixes, datatype.iri)), None),
                    Literal::LanguageTaggedString { value, language } => (None, Some(value.to_string()), None, Some(language.to_string())),
                },
            };
            stack.push(vec![subject, predicate, object, value, datatype, language]);

            match t.subject {
                NamedOrBlankNode::NamedNode(node) => { stanza = shorten(&prefixes, node.iri); }
                _ => { }
            }
            if stanza == "" && t.predicate == annotated_source {
                match t.object {
                    Term::NamedNode(node) => { stanza = shorten(&prefixes, node.iri); },
                    _ => { }
                }
            }
        }
        Ok(()) as Result<(), RdfXmlError>
    }).unwrap();
    tx.commit()?;
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: rdftab target.db");
        process::exit(1);
    }
    let db = &args[1];
    if let Err(err) = insert(db) {
        println!("{}", err);
        process::exit(1);
    }
}
