# Requirements
#
# Java 8+
# Python xlsx2csv https://github.com/dilshod/xlsx2csv
# librdf raptor2 http://librdf.org/raptor/

SHEETS = prefix thin
SHEET_TSVS = $(foreach o,$(SHEETS),build/$(o).tsv)
ROBOT := java -jar bin/robot.jar
.DEFAULT_GOAL := round-trip-example

.PHONY: all
all: build/roundtrip-thin.diff

.PHONY: clean cargoclean
clean:
	rm -rf build

cargoclean:
	cargo clean

.PHONY: clobber
clobber: clean
	rm -rf bin

build bin:
	mkdir -p $@

bin/robot.jar: | bin
	curl -L -o $@ https://build.obolibrary.io/job/ontodev/job/robot/job/master/lastSuccessfulBuild/artifact/bin/robot.jar

build/thick.xlsx: | build
	curl -L -o $@ "https://docs.google.com/spreadsheets/d/19zS8lHUM5cU_Nf9Rc7-TGL6wesOD8JLINJSan3DmPqE/export?format=xlsx"

$(SHEET_TSVS): build/thick.xlsx
	xlsx2csv --ignoreempty --delimiter tab --sheetname $(basename $(notdir $@)) $< > $@

build/prefix.sql: build/prefix.tsv | build
	echo "CREATE TABLE IF NOT EXISTS prefix (" > $@
	echo "  prefix TEXT PRIMARY KEY," >> $@
	echo "  base TEXT NOT NULL" >> $@
	echo ");" >> $@
	echo "INSERT OR IGNORE INTO prefix VALUES" >> $@
	tail -n+2 $< \
	| sed 's/\(.*\)\t\(.*\)/("\1", "\2"),/' \
	| tac \
	| sed '0,/,$$/ s/,$$/;/'\
	| tac \
	>> $@

build/thin.sql: build/thin.tsv | build
	echo "CREATE TABLE IF NOT EXISTS statements (" > $@
	echo "  stanza TEXT NOT NULL," >> $@
	echo "  subject TEXT NOT NULL," >> $@
	echo "  predicate TEXT NOT NULL," >> $@
	echo "  object TEXT," >> $@
	echo "  value TEXT," >> $@
	echo "  datatype TEXT," >> $@
	echo "  language TEXT" >> $@
	echo ");" >> $@
	echo "INSERT OR IGNORE INTO statements VALUES" >> $@
	tail -n+2 $< \
	| awk -v FS='\t' -v OFS='\t' '{print $$1,$$2,$$3,$$4,$$5,$$6,$$7}' \
	| sed 's/\t/", "/g' \
	| sed 's/^/("/g' \
	| sed 's/$$/"),/g' \
	| sed 's/""/NULL/g' \
	| tac \
	| sed '0,/,$$/ s/,$$/;/'\
	| tac \
	>> $@

build/thin.db: build/prefix.sql build/thin.sql
	rm -f $@
	cat $^ | sqlite3 $@

build/thin.ttl: build/thin.db
	sqlite3 $< < src/turtle.sql > $@

build/thin.owl: build/thin.ttl | bin/robot.jar
	$(ROBOT) convert --input $< --output $@

build/thin.rdf: build/thin.ttl
	rapper -i turtle -o rdfxml-abbrev $< > $@

target/release/rdftab: src/main.rs
	cargo build --release

target/debug/rdftab: src/main.rs
	cargo build

build/roundtrip-thin.db: target/debug/rdftab build/prefix.sql build/thin.rdf
	rm -f $@
	sqlite3 $@ < $(word 2,$^)
	$< $@ < $(word 3,$^)

build/roundtrip-thin.tsv: build/roundtrip-thin.db
	sqlite3 $< ".mode tabs" ".header on" "select * from statements" \
	| sed s/_:riog0000000./_:b/g \
	| sort \
	> $@

build/sorted-thin.tsv: build/thin.tsv
	sed s/_:b./_:b/g $< \
	| sort \
	> $@

build/roundtrip-thin.diff: build/sorted-thin.tsv build/roundtrip-thin.tsv
	diff $^

build/obi.owl: | build
	wget https://raw.githubusercontent.com/obi-ontology/obi/v2021-04-06/obi.owl -O $@

build/obi.ttl: build/obi.owl
	robot convert --input $< --format ttl --output $@

build/obi.rdf: build/obi.ttl
	rapper -i turtle -o rdfxml-abbrev $< > $@

build/obi_core.db: build/prefix.sql obi_core_no_trailing_ws.owl
	rm -f $@
	sqlite3 $@ < $<
	rdftab $@ < obi_core_no_trailing_ws.owl

build/thick.db: build/prefix.sql target/debug/rdftab
	rm -f $@
	sqlite3 $@ < $<

.PHONY: round-trip-example round-trip-obi-core round-trip-obi
round-trip-example: build/thick.db build/thin.rdf target/debug/rdftab round-trip.py
	rdftab -r $< < $(word 2,$^) | round-trip.py $(word 2,$^)

round-trip-obi-core: build/thick.db obi_core_no_trailing_ws.owl target/debug/rdftab round-trip.py
	rdftab -r $< < $(word 2,$^) | round-trip.py $(word 2,$^)

round-trip-obi: build/thick.db obi.rdf target/debug/rdftab round-trip.py
	rdftab -r $< < $(word 2,$^) | round-trip.py $(word 2,$^)

.PHONY: remote-perf
remote-perf:
	scp src/main.rs Makefile debian-sandbox:Knocean/rdftab.rs/src/
	ssh debian-sandbox "cd Knocean/rdftab.rs && cargo build && rm -f build/obi_core.db && sqlite3 build/obi_core.db < build/prefix.sql && time rdftab build/obi_core.db < obi_core_no_trailing_ws.owl"
