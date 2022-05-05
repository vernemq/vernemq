FROM erlang:22.3.4.16-alpine AS build-env

WORKDIR /vernemq-build

ARG VERNEMQ_GIT_REF=Branch_1.12.4
ARG TARGET=rel
ARG VERNEMQ_REPO=https://github.com/widhaprasa/vernemq.git

# Defaults
ENV DOCKER_VERNEMQ_KUBERNETES_LABEL_SELECTOR="app=vernemq" \
    DOCKER_VERNEMQ_LOG__CONSOLE=console

RUN apk --no-cache --update --available upgrade && \
    apk add --no-cache git autoconf build-base bsd-compat-headers cmake openssl-dev curl bash snappy-dev && \
    git clone -b $VERNEMQ_GIT_REF $VERNEMQ_REPO .

COPY bin/build.sh build.sh

RUN ./build.sh $TARGET

FROM alpine:3.9.6

RUN apk --no-cache --update --available upgrade && \
    apk add --no-cache ncurses-libs openssl libstdc++ jq curl bash snappy-dev nano && \
    addgroup --gid 10000 vernemq && \
    adduser --uid 10000 -H -D -G vernemq -h /vernemq vernemq && \
    install -d -o vernemq -g vernemq /vernemq

# Defaults
ENV DOCKER_VERNEMQ_KUBERNETES_LABEL_SELECTOR="app=vernemq" \
    DOCKER_VERNEMQ_LOG__CONSOLE=console \
    PATH="/vernemq/bin:$PATH" \
    DOCKER_VERNEMQ_ALLOW_ANONYMOUS=off \
    DOCKER_VERNEMQ_plugins.vmq_passwd=off \
    DOCKER_VERNEMQ_plugins.vmq_acl=off \
    DOCKER_VERNEMQ_plugins.vmq_diversity=on \
    DOCKER_VERNEMQ_vmq_diversity.auth_mysql.enabled=on \
    DOCKER_VERNEMQ_vmq_diversity.mysql.host=vernemq-mysql \
    DOCKER_VERNEMQ_vmq_diversity.mysql.port=3306 \
    DOCKER_VERNEMQ_vmq_diversity.mysql.user=vmq_mysql \
    DOCKER_VERNEMQ_vmq_diversity.mysql.password=vmq_mysql \
    DOCKER_VERNEMQ_vmq_diversity.mysql.database=vmq_mysql \
    DOCKER_VERNEMQ_vmq_diversity.mysql.password_hash_method=sha256

WORKDIR /vernemq

COPY --chown=10000:10000 bin/vernemq.sh /usr/sbin/start_vernemq
COPY --chown=10000:10000 files/vm.args /vernemq/etc/vm.args
COPY --chown=10000:10000 --from=build-env /vernemq-build/release /vernemq

RUN ln -s /vernemq/etc /etc/vernemq && \
    ln -s /vernemq/data /var/lib/vernemq && \
    ln -s /vernemq/log /var/log/vernemq

# Ports
# 1883  MQTT
# 8883  MQTT/SSL
# 8080  MQTT WebSockets
# 44053 VerneMQ Message Distribution
# 4369  EPMD - Erlang Port Mapper Daemon
# 8888  Prometheus Metrics
# 9100 9101 9102 9103 9104 9105 9106 9107 9108 9109  Specific Distributed Erlang Port Range

EXPOSE 1883 8883 8080 44053 4369 8888 \
       9100 9101 9102 9103 9104 9105 9106 9107 9108 9109

VOLUME ["/vernemq/log", "/vernemq/data", "/vernemq/etc"]

HEALTHCHECK CMD vernemq ping | grep -q pong

USER vernemq
CMD ["start_vernemq"]