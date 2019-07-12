-module(vmq_cowboy_websocket_h).
-behavior(cowboy_stream).


-export([init/3]).
-export([data/4]).
-export([info/3]).
-export([terminate/3]).
-export([early_error/5]).

init(StreamID, Req, Opts) ->
    cowboy_stream:init(StreamID, Req, Opts).

-spec data(cowboy_stream:streamid(), cowboy_stream:fin(), cowboy_req:resp_body(), State)
	-> {cowboy_stream:commands(), State} when State::any().
data(StreamID, IsFin, Data, Next) ->
    cowboy_stream:data(StreamID, IsFin, Data, Next).

-spec info(cowboy_stream:streamid(), any(), State)
	-> {cowboy_stream:commands(), State} when State::any().
info(StreamID, {switch_protocol, Headers, cowboy_websocket, State}, Next) ->
    %% rewrite module so our takeover/7 function is called.
    cowboy_stream:info(StreamID, {switch_protocol, Headers, vmq_cowboy_websocket, State}, Next);
info(StreamID, Info, Next) ->
    cowboy_stream:info(StreamID, Info, Next).

-spec terminate(cowboy_stream:streamid(), cowboy_stream:reason(), any()) -> any().
terminate(StreamID, Reason, Next) ->
    cowboy_stream:terminate(StreamID, Reason, Next).

-spec early_error(cowboy_stream:streamid(), cowboy_stream:reason(),
	cowboy_stream:partial_req(), Resp, cowboy:opts()) -> Resp
	when Resp::cowboy_stream:resp_command().
early_error(StreamID, Reason, PartialReq, Resp, Opts) ->
    cowboy_stream:early_error(StreamID, Reason, PartialReq, Resp, Opts).
