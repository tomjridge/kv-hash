TMP_DOC_DIR:=/tmp/minimal_ocaml
scratch:=/tmp/l/github/scratch

default: all

all::
	dune build test/test.exe
#	dune build bin/example.exe

-include Makefile.ocaml

run_test:
	OCAMLRUNPARAM=b dune exec test/test.exe

# for auto-completion of Makefile target
clean::
