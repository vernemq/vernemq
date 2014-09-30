-module(vmq_tcp).
-behaviour(ranch_protocol).
-include_lib("public_key/include/public_key.hrl").

-export([start_link/4]).
-export([init/4]).

-spec start_link(_,_,_,_) -> {'ok',pid()}.
start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

-spec init(_,_,atom() | tuple(),maybe_improper_list()) -> any().
init(Ref, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{nodelay, true},
                                    {packet, raw},
                                    {active, once}]),
    NewOpts =
    case Transport of
        ranch_ssl ->
            case proplists:get_value(use_identity_as_username, Opts) of
                true ->
                    [{preauth, socket_to_common_name(Socket)}|Opts];
                false ->
                    Opts
            end;
        _ ->
            Opts
    end,


    {ok, Peer} = Transport:peername(Socket),
    vmq_systree:incr_socket_count(),
    loop(Socket, Transport, vmq_fsm:init(Peer,
                                            fun(Bin) ->
                                                    vmq_systree:incr_bytes_sent(byte_size(Bin)),
                                                    Transport:send(Socket, Bin)
                                            end, NewOpts)).

-spec loop(_,atom() | tuple(),'stop' | {_,_}) -> any().
loop(Socket, Transport, stop) ->
    Transport:close(Socket);
loop(Socket, Transport, FSMState) ->
    Transport:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Data} ->
            vmq_systree:incr_bytes_received(byte_size(Data)),
            loop(Socket, Transport,
                 vmq_fsm:handle_input(Data, FSMState));
        {ssl, Socket, Data} ->
            vmq_systree:incr_bytes_received(byte_size(Data)),
            loop(Socket, Transport,
                 vmq_fsm:handle_input(Data, FSMState));
        {tcp_closed, Socket} ->
            vmq_fsm:handle_close(FSMState);
        {ssl_closed, Socket} ->
            vmq_fsm:handle_close(FSMState);
        {tcp_error, Socket, Reason} ->
            vmq_fsm:handle_error(Reason, FSMState);
        {ssl_error, Socket, Reason} ->
            vmq_fsm:handle_error(Reason, FSMState);
        Msg ->
            loop(Socket, Transport,
                 vmq_fsm:handle_fsm_msg(Msg, FSMState))
    end.

-spec socket_to_common_name({'sslsocket',_,pid() | {port(),_}}) -> 'undefined' | [any()] | {'error',[any()],binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | [])} | {'incomplete',[any()],binary()}.
socket_to_common_name(Socket) ->
    case ssl:peercert(Socket) of
        {error, no_peercert} ->
            undefined;
        {ok, Cert} ->
            OTPCert = public_key:pkix_decode_cert(Cert, otp),
            TBSCert = OTPCert#'OTPCertificate'.tbsCertificate,
            Subject = TBSCert#'OTPTBSCertificate'.subject,
            extract_cn(Subject)
    end.

-spec extract_cn({'rdnSequence',maybe_improper_list()}) -> 'undefined' | [any()] | {'error',[any()],binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | [])} | {'incomplete',[any()],binary()}.
extract_cn({rdnSequence, List}) ->
    extract_cn2(List).
-spec extract_cn2(maybe_improper_list()) -> 'undefined' | [any()] | {'error',[any()],binary() | maybe_improper_list(binary() | maybe_improper_list(any(),binary() | []) | char(),binary() | [])} | {'incomplete',[any()],binary()}.
extract_cn2([[#'AttributeTypeAndValue'{type=?'id-at-commonName', value={utf8String, CN}}]|_]) ->
    unicode:characters_to_list(CN);
extract_cn2([_|Rest]) ->
    extract_cn2(Rest);
extract_cn2([]) -> undefined.
