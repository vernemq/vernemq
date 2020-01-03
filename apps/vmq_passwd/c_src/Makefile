CC? = gcc
CFLAGS_EXTRA =

ifeq ($(shell uname -s), Darwin)
	# OSX with homebrew
	OPENSSL_DIR ?= $(shell brew --prefix openssl)
	CFLAGS_EXTRA += -L${OPENSSL_DIR}/lib -I${OPENSSL_DIR}/include
endif

all: compile

compile:
	$(CC) $(CFLAGS) $(CFLAGS_EXTRA) vmq_passwd.c -lcrypto -o ../priv/vmq_passwd
