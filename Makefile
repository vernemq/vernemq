DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
				public_key syntax_tools compiler

.PHONY: deps test

all: deps compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean
	
distclean: clean
	./rebar delete-deps

include tools.mk
