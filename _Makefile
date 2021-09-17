TMP_DOC_DIR:=/tmp/minimal_ocaml
scratch:=/tmp/l/github/scratch

default: all

all::
	dune build test/test.exe
	dune build test/test2.exe
	dune build test/test3.exe
#	dune build bin/example.exe

-include Makefile.ocaml

run_test:
	time OCAMLRUNPARAM=b dune exec test/test3.exe
#	time OCAMLRUNPARAM=b dune exec test/test2.exe

# for auto-completion of Makefile target
clean::
