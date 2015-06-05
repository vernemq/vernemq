.PHONY: test

all: compile

compile:
	./rebar3 compile

clean:
	./rebar3 clean

test:
	./rebar3 ct
	
