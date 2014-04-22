-module(emqttd).
-export([start/0, stop/0]).

start() ->
    application:start(ranch),
    application:start(bitcask),
    application:start(mnesia),
    application:start(locks),
    application:start(emqttd).

stop() ->
    application:stop(emqttd),
    application:stop(locks),
    application:stop(mnesia),
    application:stop(bitcask),
    application:stop(ranch).

