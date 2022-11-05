.PHONY: deps test

all: deps compile

compile: deps
	rebar3 compile

deps:
	rebar3 get-deps

clean:
	rebar3 clean

distclean: clean
	rebar3 delete-deps


