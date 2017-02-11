%% Copyright 2017 Erlio GmbH Basel Switzerland (http://erl.io)
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
%%%-------------------------------------------------------------------

-module(vmq_diversity_cache).

-behaviour(gen_server2).

%% API
-export([start_link/0,
         install/1,
         match_publish_acl/3,
         match_subscribe_acl/3,
         clear_cache/0,
         clear_cache/2,
         entries/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-import(luerl_lib, [badarg_error/3]).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

install(St) ->
    luerl_emul:alloc_table(table(), St).

-spec match_publish_acl(binary(), binary(), [binary()]) ->
    true | false | no_cache.
match_publish_acl(MP, ClientId, Topic) ->
    match_acl(MP, ClientId, Topic, publish).

-spec match_subscribe_acl(binary(), binary(), [binary()]) ->
    true | false | no_cache.
match_subscribe_acl(MP, ClientId, Topic) ->
    match_acl(MP, ClientId, Topic, subscribe).

match_acl(MP, ClientId, Topic, Type) ->
    Key = key(MP, ClientId),
    match(Key, Type, Topic).

clear_cache() ->
    _ = [ets:delete_all_objects(t(T)) || T <- [cache, publish, subscribe]],
    ok.

clear_cache(MP, ClientId) ->
    Key = key(MP, ClientId),
    case ets:lookup(t(cache), Key) of
        [] -> ok;
        [{_, PubAclHashes, SubAclHashes}] ->
            gen_server2:call(?MODULE, {delete_cache, Key, PubAclHashes, SubAclHashes})
    end.

entries(MP, ClientId) when is_binary(MP) ->
    Key = key(MP, ClientId),
    case ets:lookup(t(cache), Key) of
        [] -> [];
        [{_, PubAclHashes, SubAclHashes}] ->
            [{publish, entries_(publish, PubAclHashes)},
             {subscribe, entries_(subscribe, SubAclHashes)}]
    end.

entries_(Type, Hashes) ->
    lists:foldl(fun(H, Acc) ->
                        [{_, Topic, _}] = ets:lookup(t(Type), H),
                        [Topic|Acc]
                end, [], Hashes).


%%%===================================================================
%%% Luerl specific
%%%===================================================================
table() ->
    [
     {<<"insert">>, {function, fun insert/2}},
     {<<"match_subscribe">>, {function, fun match_subscribe/2}},
     {<<"match_publish">>, {function, fun match_publish/2}}
    ].

insert(As, St) ->
    case As of
        [MP, ClientId, User, PubAcls, SubAcls]
          when is_binary(MP)
               and is_binary(ClientId)
               and is_binary(User)
               and is_tuple(PubAcls)
               and is_tuple(SubAcls) ->
            case {validate_acls(MP, User, ClientId, luerl:decode(PubAcls, St), []),
                  validate_acls(MP, User, ClientId, luerl:decode(SubAcls, St), [])} of
                {error, _} ->
                    badarg_error(execute_parse, As, St);
                {_, error} ->
                    badarg_error(execute_parse, As, St);
                {VPubAcls, VSubAcls} ->
                    Key = key(MP, ClientId),
                    gen_server2:call(?MODULE,
                                     {insert_cache, Key, VPubAcls, VSubAcls}),
                    {[true], St}
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

match_subscribe(As, St) ->
    match_topic(As, St, subscribe).

match_publish(As, St) ->
    match_topic(As, St, publish).

match_topic(As, St, Type) ->
    case As of
        [MP, ClientId, Topic]
          when is_binary(MP)
               and is_binary(ClientId)
               and is_binary(Topic) ->
            case vmq_topic:validate_topic(Type, Topic) of
                {ok, Words} ->
                    case match_acl(MP, ClientId, Words, Type) of
                        true ->
                            {[true], St};
                        _ ->
                            {[false], St}
                    end;
                _ ->
                    badarg_error(execute_parse, As, St)
            end;
        _ ->
            badarg_error(execute_parse, As, St)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    _ = [ets:new(t(T), [public, named_table,
                        {read_concurrency, true},
                        {write_concurrency, true}])
         || T <- [cache, publish, subscribe]],
    {ok, #state{}}.

handle_call({insert_cache, Key, PubAcls, SubAcls}, _From, State) ->
    insert_cache(Key, PubAcls, SubAcls),
    {reply, ok, State};
handle_call({delete_cache, Key, PubAclHashes, SubAclHashes}, _From, State) ->
    delete_cache_(t(publish), PubAclHashes),
    delete_cache_(t(subscribe), SubAclHashes),
    ets:delete(t(cache), Key),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
validate_acls(MP, User, ClientId, [{_, Topic}|Rest], Acc) when is_binary(Topic) ->
    %% we use validate_topic(subscibe... because this would allow that
    %% an ACL contains wildcards
    case vmq_topic:validate_topic(subscribe, Topic) of
        {ok, Words} ->
            validate_acls(MP, User, ClientId, Rest,
                          [subst(MP, User, ClientId, Words, [])|Acc]);
        {error, Reason} ->
            lager:error("Can't validate ACL topic ~p for client ~p due to ~p",
                        [Topic, ClientId, Reason]),
            validate_acls(MP, User, ClientId, Rest, Acc)
    end;
validate_acls(_, _, _, [], Acc) -> Acc.

subst(MP, User, ClientId, [<<"%u">>|Rest], Acc) ->
    subst(MP, User, ClientId, Rest, [User|Acc]);
subst(MP, User, ClientId, [<<"%c">>|Rest], Acc) ->
    subst(MP, User, ClientId, Rest, [ClientId|Acc]);
subst(MP, User, ClientId, [<<"%m">>|Rest], Acc) ->
    subst(MP, User, ClientId, Rest, [MP|Acc]);
subst(MP, User, ClientId, [W|Rest], Acc) ->
    subst(MP, User, ClientId, Rest, [W|Acc]);
subst(_MP, _User, _ClientId, [], Acc) -> lists:reverse(Acc).

key(MP, ClientId) when is_binary(MP)
                       and is_binary(ClientId) ->
    {MP, ClientId}.

match(Key, Type, Topic) ->
    case ets:lookup(t(cache), Key) of
        [] -> no_cache;
        [{_, PubAclHashes, _}] when Type == publish ->
            match_(t(Type), Topic, PubAclHashes);
        [{_, _, SubAclHashes}] when Type == subscribe ->
            match_(t(Type), Topic, SubAclHashes)
    end.

match_(Table, Topic, [TopicHash|Rest]) ->
    case ets:lookup(Table, TopicHash) of
        [{_, TopicAcl, _}] ->
            case vmq_topic:match(Topic, TopicAcl) of
                true -> true;
                false ->
                    match_(Table, Topic, Rest)
            end;
        _ ->
            match_(Table, Topic, Rest)
    end;
match_(_, _, []) -> false.

insert_cache(Key, PubAcls, SubAcls) ->
    PubAclHashes = insert_cache_(t(publish), PubAcls, []),
    SubAclHashes = insert_cache_(t(subscribe), SubAcls, []),
    ets:insert(t(cache), {Key, PubAclHashes, SubAclHashes}).

insert_cache_(Table, [Topic|Rest], Acc) ->
    TopicHash = erlang:phash2(Topic),
    ets:update_counter(Table, TopicHash,
                       {3, 1}, % Update Op
                       {TopicHash, Topic, 0}),
    insert_cache_(Table, Rest, [TopicHash|Acc]);
insert_cache_(_, [], Acc) -> Acc.

delete_cache_(Table, [TopicHash|Rest]) ->
    case ets:update_counter(Table, TopicHash,
                       {3, -1}, % Update Op
                       {TopicHash, ignored, 0}) of
        R when R =< 0 ->
            ets:delete(Table, TopicHash);
        _ ->
            ignore
    end,
    delete_cache_(Table, Rest);
delete_cache_(_, []) -> ok.

t(cache) -> vmq_diversity_cache;
t(publish) -> vmq_diversity_pub_cache;
t(subscribe) -> vmq_diversity_sub_cache.
