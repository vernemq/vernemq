BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
GIT_VERSION      = $(shell git describe --tags)
OVERLAY_VARS    ?=
REBAR ?= $(BASE_DIR)/rebar3
PSQL_MIGRATION_EXEC = $(BASE_DIR)/_build/default/bin/psql_migration

$(if $(ERLANG_BIN),,$(warning "Warning: No Erlang found in your path, this will probably not work"))


all: compile

compile:
	$(REBAR) $(PROFILE) compile


rpi32: PROFILE = as rpi32
rpi32: rel


##
## Release targets
##
rel:
	cat vars.config > vars.generated
	echo "{app_version, \"${GIT_VERSION}\"}." >> vars.generated
ifeq ($(OVERLAY_VARS),)
else
	cat $(OVERLAY_VARS) >> vars.generated
endif
	$(REBAR) $(PROFILE) release

##
## Developer targets
##
##  devN - Make a dev build for node N
dev% :
	./gen_dev $@ vars/dev_vars.config.src vars/$@_vars.config
	cat vars/$@_vars.config > vars.generated
	(./rebar3 as $@ release)

.PHONY: all compile rpi32 rel
export OVERLAY_VARS

.PHONY: test
test:
	@docker-compose -f docker-compose.test.yml down --volumes
	@docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit

# DB #############

db-migrate:
	$(PSQL_MIGRATION_EXEC) run

db-create-migration: ##@database create a migration, profile FILENAME env var
	$(PSQL_MIGRATION_EXEC) new $(FILENAME)

db-list-migrations: ##@database list migrations indicating which have been applied
	$(PSQL_MIGRATION_EXEC) list

db-setup: ##@database creates the database specified in your DATABASE_URL, and runs any existing migrations.
	$(PSQL_MIGRATION_EXEC) setup

db-rollback: ##@database reverts the last migration
	$(PSQL_MIGRATION_EXEC) revert

db-reset: ##@database resets your database by dropping the database in your DATABASE_URL and then runs `setup`
	$(PSQL_MIGRATION_EXEC) reset
