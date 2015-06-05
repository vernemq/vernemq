.PHONY: test

all: compile

compile:
	./rebar3 compile

clean:
	./rebar3 clean

test:
	erl -sname epmd_starter_node -eval "init:stop()"
	./rebar3 ct 
	
