CC? = gcc
CFLAGS_EXTRA =

ifeq ($(shell uname -s), Darwin)
	# OSX with homebrew
    HAS_BREW := $(shell command -v brew;)

	ifdef HAS_BREW
		OPENSSL_DIR ?= $(shell brew --prefix openssl)
	endif

	# Default dir
	OPENSSL_DIR ?= /usr/local/opt/openssl

	CFLAGS_EXTRA += -L${OPENSSL_DIR}/lib -I${OPENSSL_DIR}/include
endif

all: compile

compile:
	$(CC) $(CFLAGS) $(CFLAGS_EXTRA) vmq_passwd.c -lcrypto -o ../priv/vmq_passwd
