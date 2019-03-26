BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
GIT_VERSION      = $(shell git describe --tags)
NOW              = $(shell date +%s)
OS               = $(shell uname -s)
OVERLAY_VARS    ?=
REBAR ?= $(BASE_DIR)/rebar3

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
ifeq ($(OVERLAY_VARS),)
	cat vars.config > vars.generated
	$(REBAR) $(PROFILE) release
else
	cat vars.config > vars.generated
	cat $(OVERLAY_VARS) >> vars.generated
	$(REBAR) $(PROFILE) release
endif

##
## Support RPM and Debian based linux systems
##
ifeq ($(OS),Linux)
ARCH          = $(shell uname -m)
ISRPM         = $(shell cat /etc/redhat-release 2> /dev/null)
ISDEB         = $(shell cat /etc/debian_version 2> /dev/null)
ISSLES        = $(shell cat /etc/SuSE-release 2> /dev/null)
ifneq ($(ISRPM),)
PKGTARGET      = rpm
else
ifneq ($(ISDEB),)
PKGTARGET      = deb
else
ifneq ($(ISSLES),)
PKGTARGET      = rpm
endif  # SLES
endif  # deb
endif  # rpm
endif # linux

package: $(PKGTARGET)

pkg_rel: pkg_clean
	$(MAKE) rel

pkg_strip:
	$(shell find _build/default/rel/vernemq -type f -executable | xargs strip --remove-section=.comment --remove-section=.note --strip-unneeded)

deb: OVERLAY_VARS=vars/deb_vars.config
deb: pkg_rel pkg_strip
	# required for running dpkg-shlibdeps
	mkdir -p debian
	touch debian/control

	$(eval DEPENDS := $(shell find _build/default/rel/vernemq -type f -name "*.so" | xargs dpkg-shlibdeps -O | cut -c16- | sed "s/,/\" --depends \"/g"))
	fpm -s dir -t deb -v "$(GIT_VERSION)" \
		--force \
		--name vernemq \
		--epoch $(NOW) \
		--license "Apache 2.0" \
		--url "https://vernemq.com" \
		--vendor "Octavo Labs AG" \
		--maintainer "<info@vernemq.com>" \
		--description "VerneMQ is a MQTT message broker" \
		--depends logrotate \
		--depends sudo \
		--depends adduser \
		--depends "$(DEPENDS)" \
		--deb-user vernemq \
		--deb-group vernemq \
		--deb-changelog ./changelog.md \
		--deb-no-default-config-files \
		--deb-systemd files/vernemq.service \
		--after-install files/deb-vernemq.postinst \
		--config-files /etc/vernemq/vernemq.conf \
		_build/default/rel/vernemq/bin/vernemq=/usr/bin/vernemq \
		_build/default/rel/vernemq/bin/vmq-admin=/usr/bin/vmq-admin \
		_build/default/rel/vernemq/data=/var/lib/vernemq/ \
		_build/default/rel/vernemq/etc/=/etc/vernemq/ \
		_build/default/rel/vernemq/bin/=/usr/lib/vernemq/bin/ \
		_build/default/rel/vernemq/lib=/usr/lib/vernemq/ \
		_build/default/rel/vernemq/releases=/usr/lib/vernemq/ \
		_build/default/rel/vernemq/erts-$(shell erl -eval 'erlang:display(erlang:system_info(version)), halt().'  -noshell)=/usr/lib/vernemq/ \
		_build/default/rel/vernemq/share/=/usr/share/vernemq/ \
		_build/default/rel/vernemq/log/=/var/log/vernemq/

rpm: OVERLAY_VARS=vars/rpm_vars.config
rpm: pkg_rel pkg_strip

	fpm -s dir -t rpm -v "$(GIT_VERSION)" \
		--force \
		--name vernemq \
		--epoch $(NOW) \
		--license "Apache 2.0" \
		--url "https://vernemq.com" \
		--vendor "Octavo Labs AG" \
		--maintainer "<info@vernemq.com>" \
		--description "VerneMQ is a MQTT message broker" \
		--depends logrotate \
		--depends sudo \
		--rpm-user vernemq \
		--rpm-group vernemq \
		--rpm-autoreqprov \
		--rpm-compression xz \
		--before-install files/rpm-vernemq.preinst \
		--after-install files/rpm-vernemq.postinst \
		--config-files /etc/vernemq/vernemq.conf \
		files/vernemq.service=/etc/systemd/system/vernemq.service \
		_build/default/rel/vernemq/bin/vernemq=/usr/sbin/vernemq \
		_build/default/rel/vernemq/bin/vmq-admin=/usr/sbin/vmq-admin \
		_build/default/rel/vernemq/data=/var/lib/vernemq/ \
		_build/default/rel/vernemq/etc/=/etc/vernemq/ \
		_build/default/rel/vernemq/bin/=/usr/lib64/vernemq/bin/ \
		_build/default/rel/vernemq/lib=/usr/lib64/vernemq/ \
		_build/default/rel/vernemq/releases=/usr/lib64/vernemq/ \
		_build/default/rel/vernemq/erts-$(shell erl -eval 'erlang:display(erlang:system_info(version)), halt().'  -noshell)=/usr/lib64/vernemq/ \
		_build/default/rel/vernemq/share/=/usr/share/vernemq/ \
		_build/default/rel/vernemq/log/=/var/log/vernemq/

pkg_clean:
	rm -rf _build/default/rel

##
## Developer targets
##
##  devN - Make a dev build for node N
dev% :
	./gen_dev $@ vars/dev_vars.config.src vars/$@_vars.config
	cat vars/$@_vars.config > vars.generated
	(./rebar3 as $@ release)

.PHONY: all compile rpi32 pkg_rel pkg_clean deb rpm rel
export OVERLAY_VARS
