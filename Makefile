DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
				public_key mnesia syntax_tools compiler

NETSPLIT_TESTS = vmq_netsplit_publish_tests,\
				 vmq_netsplit_register_consistency_tests,\
				 vmq_netsplit_register_multiple_session_tests,\
				 vmq_netsplit_register_not_ready_tests,\
				 vmq_netsplit_subscribe_tests

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

netsplit:
	ERL_FLAGS="-epmd_port 43690" ./rebar eunit -D NETSPLIT skip_deps=true suites=$(NETSPLIT_TESTS)

include tools.mk
