REBAR=$(shell which rebar)

# =============================================================================
# Verify that the programs we need to run are installed on this system
# =============================================================================
ERL = $(shell which erl)

ifeq ($(ERL),)
	$(error "Erlang not available on this system")
endif 

ifeq ($(REBAR),)
	$(error "Rebar not available on this system")
endif

all: compile test

deps:
	$(REBAR) get-deps

compile: deps
	$(REBAR) compile


default-test: compile
	$(REBAR) skip_deps=true eunit suites=vmq_clean_session_tests,vmq_cluster_tests,vmq_connect_tests,vmq_last_will_tests,vmq_publish_tests,vmq_retain_tests,vmq_ssl_tests,vmq_subscribe_tests


netsplit-test: compile
	ERL_FLAGS="-epmd_port 43690" $(REBAR) skip_deps=true eunit suites=vmq_netsplit_publish_tests,vmq_netsplit_register_consistency_tests,vmq_netsplit_register_multiple_session_tests,vmq_netsplit_register_not_ready_tests,vmq_netsplit_subscribe_tests

test: compile default-test netsplit-test

clean:
	- rm -rf $(CURDIR)/.eunit
	- rm -rf $(CURDIR)/ebin
	$(REBAR) skip_deps=true clean
