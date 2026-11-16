%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
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

-module(vmq_ssl_ca_cache).
-include_lib("public_key/include/public_key.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    ca_certs/1,
    refresh/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {}).
-define(TAB, ?MODULE).
%% default of the ssl application for 'ssl_pem_cache_clean'
-define(PEM_CACHE_CLEAN, 120000).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> 'ignore' | {'error', _} | {'ok', pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% The CA certificates of a cafile, used as trust anchors by the
%% partial_chain option. They are cached and reloaded with the next
%% lookup after a purge.
-spec ca_certs(file:name_all()) -> [public_key:der_encoded()].
ca_certs(File) ->
    case ets:lookup(?TAB, File) of
        [{_, CaCerts}] ->
            CaCerts;
        [] ->
            %% no ca certs loaded
            gen_server:call(?MODULE, {add_ca_certs, File}),
            ca_certs(File)
    end.

-spec refresh() -> 'ok'.
refresh() ->
    gen_server:call(?MODULE, purge).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([]) -> {'ok', state()}.
init([]) ->
    _ = ets:new(?TAB, [protected, named_table, {read_concurrency, true}]),
    schedule_purge_tick(),
    {ok, #state{}}.

-spec handle_call({'add_ca_certs', file:name_all()} | 'purge', _, _) -> {'reply', 'ok', _}.
handle_call({add_ca_certs, File}, _From, State) ->
    %% concurrent handshakes ask for the same file, read it only once
    _ = ets:member(?TAB, File) orelse ets:insert(?TAB, {File, read_ca_certs(File)}),
    {reply, ok, State};
handle_call(purge, _From, State) ->
    purge(),
    {reply, ok, State}.

-spec handle_cast(_, _) -> {'noreply', _}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(_, _) -> {'noreply', _}.
handle_info(purge_tick, State) ->
    purge(),
    schedule_purge_tick(),
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

%% The ssl application rereads a changed cafile when it validates its
%% PEM cache, the trust anchors are purged with the same interval.
schedule_purge_tick() ->
    TickMS =
        case application:get_env(ssl, ssl_pem_cache_clean) of
            {ok, Interval} when is_integer(Interval), Interval > 0 -> Interval;
            _ -> ?PEM_CACHE_CLEAN
        end,
    erlang:send_after(TickMS, self(), purge_tick).

purge() ->
    ets:delete_all_objects(?TAB).

%% An unreadable cafile yields no trust anchors, the failure is cached
%% until the next purge so that not every handshake hits the disk.
read_ca_certs(File) ->
    case file:read_file(File) of
        {ok, Bin} ->
            CaCerts = [
                Der
             || {'Certificate', Der, not_encrypted} <- public_key:pem_decode(Bin),
                is_ca_cert(Der)
            ],
            case CaCerts of
                [] ->
                    ?LOG_WARNING("partial_chain: no CA certificate found in cafile ~p", [File]);
                _ ->
                    ok
            end,
            CaCerts;
        {error, Reason} ->
            ?LOG_ERROR("partial_chain: can't read cafile ~p due to ~p", [File, Reason]),
            []
    end.

%% Only certificates that are allowed to sign other certificates become
%% trust anchors, an end-entity certificate must not vouch for itself.
is_ca_cert(Der) ->
    try public_key:pkix_decode_cert(Der, otp) of
        #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{extensions = Extensions}} when
            is_list(Extensions)
        ->
            case lists:keyfind(?'id-ce-basicConstraints', #'Extension'.extnID, Extensions) of
                #'Extension'{extnValue = #'BasicConstraints'{cA = true}} -> true;
                _ -> false
            end;
        _ ->
            false
    catch
        _:_ ->
            false
    end.
