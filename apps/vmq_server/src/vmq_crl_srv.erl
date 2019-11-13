%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_crl_srv).
-include_lib("public_key/include/public_key.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0,
         check_crl/2,
         refresh_crls/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).
-define(TAB, ?MODULE).

-type state() :: #state{}.
-type otp_cert() :: #'OTPCertificate'{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec check_crl(_, otp_cert()) -> boolean().
check_crl(File, #'OTPCertificate'{tbsCertificate=TBSCert} = Cert) ->
    SerialNr = TBSCert#'OTPTBSCertificate'.serialNumber,
    case ets:lookup(?TAB, File) of
        [{_, Serials}] ->
            not lists:member(SerialNr, Serials);
        [] ->
            %% no crl loaded
            gen_server:call(?MODULE, {add_crl, File}),
            check_crl(File, Cert)
    end.

refresh_crls() ->
    gen_server:call(?MODULE, purge_crls).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([]) -> {'ok', state()}.
init([]) ->
    _ = ets:new(?TAB, [public, named_table, {read_concurrency, true}]),
    schedule_crl_purge_tick(),
    {ok, #state{}}.


-spec handle_call({'add_crl',atom() | binary() |
[atom() | [any()] | char()]}, _, _) -> {'reply','ok', _}.
handle_call({add_crl, File}, _From, State) ->
    {ok, Bin} = file:read_file(File),
    Serials =
    lists:flatten([begin
                       CRL = public_key:pem_entry_decode(E) ,
                       #'TBSCertList'{revokedCertificates=Revoked} =
                           CRL#'CertificateList'.tbsCertList,
                       case Revoked of
                           _ when is_list(Revoked) ->
                               [SerialNr ||
                                   #'TBSCertList_revokedCertificates_SEQOF'{
                                      userCertificate=SerialNr} <- Revoked];
                           asn1_NOVALUE ->
                               %% case if the pem entry doesn't revoke
                               %% any certificates.
                               []
                       end
                   end || E <- public_key:pem_decode(Bin)]),
    ets:insert(?TAB, {File, Serials}),
    Reply = ok,
    {reply, Reply, State};
handle_call(purge_crls, _From, State) ->
    purge_crls(),
    {reply, ok, State}.

-spec handle_cast(_, _) -> {'noreply', _}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info(crl_purge_tick, State) ->
    purge_crls(),
    schedule_crl_purge_tick(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

schedule_crl_purge_tick() ->
    TickMS = vmq_config:get_env(crl_refresh_interval, 60000),
    case TickMS of
        0 -> ok;
        _ ->
            erlang:send_after(TickMS, ?MODULE, crl_purge_tick)
    end.

purge_crls() ->
    ets:delete_all_objects(?TAB).
