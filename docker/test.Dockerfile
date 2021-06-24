FROM erlang:23.0

RUN apt-get update -y && apt-get install -y libsnappy-dev

WORKDIR /vernemq
