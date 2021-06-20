# Requirements
#
# Java 8+
# Python xlsx2csv https://github.com/dilshod/xlsx2csv
# librdf raptor2 http://librdf.org/raptor/

SHEETS = prefix thin
SHEET_TSVS = $(foreach o,$(SHEETS),build/$(o).tsv)
ROBOT := java -jar bin/robot.jar
.DEFAULT_GOAL := rdftab

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

rdftab: target/release/rdftab
	rm -f $@
	ln -s $<

target/release/rdftab: src/main.rs
	cargo build --release

build/roundtrip-thin.db: rdftab build/prefix.sql build/thin.rdf
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

build/obi_core.db: build/prefix.sql obi_core.owl rdftab
	rm -f $@
	sqlite3 $@ < $<
	$(word 3,$^) $@ < obi_core.owl

build/thick.db: build/prefix.sql
	rm -f $@
	sqlite3 $@ < $<

build/obi-full-round-trip.ttl: build/thick.db obi.rdf rdftab
	$(word 3,$^) -r $< < $(word 2,$^) > $@

build/obi-core-round-trip.ttl: build/thick.db obi_core.owl rdftab
	$(word 3,$^) -r $< < $(word 2,$^) > $@

.PHONY: round-trip-example round-trip-obi-core round-trip-obi
round-trip-example: build/thick.db build/thin.rdf rdftab round-trip.py
	@echo "`date` Running example round trip ..."
	$(word 3,$^) -r $< < $(word 2,$^) > triples.ttl
	@echo "`date` Triples have been generated"
	$(word 4,$^) $(word 2,$^) < triples.ttl
	@echo "`date` Done!"

round-trip-obi-core: build/thick.db obi_core.owl rdftab round-trip.py
	@echo "`date` Running obi core round trip ..."
	$(word 3,$^) -r $< < $(word 2,$^) > triples.ttl
	@echo "`date` Triples have been generated"
	$(word 4,$^) $(word 2,$^) < triples.ttl
	@echo "`date` Done!"

round-trip-obi: build/thick.db obi.rdf rdftab round-trip.py obi_bfo_0000027.ttl
	@echo "`date` Running obi round trip ..."
	$(word 3,$^) -r $< < $(word 2,$^) > triples.ttl
	@echo "`date` Triples have been generated"
	$(word 4,$^) $(word 5,$^) < triples.ttl
	@echo "`date` Done!"
