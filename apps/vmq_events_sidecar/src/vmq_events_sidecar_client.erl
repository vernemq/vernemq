-module(vmq_events_sidecar_client).

-behavior(shackle_client).

%% API
-export([
  init/1,
  setup/2,
  handle_request/2,
  handle_data/2,
  terminate/1
]).

-record(state, {}).
-type state() :: #state {}.

-spec init(undefined) ->
  {ok, state()}.

init(_Opts) ->
  {ok, #state {}}.

-spec setup(inet:socket(), state()) ->
  {ok, state()}.
setup(_Socket, State) ->
  {ok, State}.

-spec handle_request(term(), state()) ->
  {ok, undefined, iolist(), state()}.
handle_request({cast, Data}, #state {} = State) ->
  lager:error("cast handle request called ~p~n", [error]),
  {ok, undefined, [Data], State};

handle_request(Request, #state {} = State) ->
  Val = vmq_events_sidecar_format:encode(Request),
  Size = byte_size(Val),
  SizeByte = <<Size:32>>,
  Data = <<SizeByte/binary, Val/binary>>,
  {ok, undefined, [Data], State}.

%%Use byte_size or length to get length of Data and append.

-spec handle_data(binary(), state()) ->
  {ok, [], state()}.
handle_data(_Data, State) ->
  {ok, [], State}.

-spec terminate(state()) -> ok.
terminate(_State) ->
  ok.
