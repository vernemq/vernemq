%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(vmq_cli_json_writer).

%% @doc Write status information in JSON format.
%%
%% The current clique -> JSON translation looks something like this:
%% ```
%% {text, "hello world"} ->
%%     {"type" : "text", "text" : "hello world"}
%% {text, [<<he>>, $l, "lo", ["world"]]} ->
%%     {"type" : "text", "text" : "hello world"}
%% {list, ["a", "b", <<"c">>]} ->
%%     {"type" : "list", "list" : ["a", "b", "c"]}
%% {list, "Camels", ["Dromedary", "Bactrian", "Sopwith"] ->
%%     {"type" : "list", "title" : "Camels", "list" : ["Dromedary", "Bactrian", "Sopwith"]}
%% {alert, [{text, "Shields failing!"}]} ->
%%     {"type" : "alert", "alert" : [{"type" : "text", "text" : "Shields failing!"}]}
%% usage ->
%%     {"type" : "usage",
%%      "usage" : "Usage: riak-admin cluster self-destruct [--delay <delayseconds>]"}
%% {table, [[{name, "Nick"}, {species, "human"}], [{name, "Rowlf"}, {species, "dog"}]]} ->
%%     {"type" : "table",
%%      "table" : [{"name" : "Nick", "species" : "human"}, {"name", "Rowlf", "species", "dog"}]}
%% '''

-behavior(clique_writer).

-export([write/1]).

-include_lib("clique/include/clique_status_types.hrl").

-record(context, {alert_set=false :: boolean(),
                  alert_list=[] :: [elem()],
                  output=[] :: iolist()}).

-spec write(status()) -> {iolist(), iolist()}.
write(Status) ->
    case lists:reverse(prepare(Status)) of
        [PreparedOutput] ->
            {jsx:encode(PreparedOutput), []};
        PreparedOutput ->
            {[jsx:encode(PreparedOutput), "\n"], []}
    end.

%% @doc Returns status data that's been prepared for conversion to JSON.
%% Just reverse the list and pass it to mochijson2:encode and you're set.
prepare(Status) ->
    Ctx = clique_status:parse(Status, fun prepare_status/2, #context{}),
    Ctx#context.output.

%% @doc Write status information in JSON format.
-spec prepare_status(elem(), #context{}) -> #context{}.
prepare_status(alert, Ctx=#context{alert_set=true}) ->
    %% TODO: Should we just return an error instead?
    throw({error, nested_alert, Ctx});
prepare_status(alert, Ctx) ->
    Ctx#context{alert_set=true};
prepare_status(alert_done, Ctx = #context{alert_list=AList, output=Output}) ->
    %% AList is already reversed, and prepare returns reversed output, so they cancel out
    AlertJsonVal = prepare(AList),
    AlertJson = #{<<"type">> => <<"alert">>, <<"alert">> => AlertJsonVal},
    Ctx#context{alert_set=false, alert_list=[], output=[AlertJson | Output]};
prepare_status(Term, Ctx=#context{alert_set=true, alert_list=AList}) ->
    Ctx#context{alert_list=[Term | AList]};
prepare_status({list, Data}, Ctx=#context{output=Output}) ->
    Ctx#context{output=[prepare_list(Data) | Output]};
prepare_status({list, Title, Data}, Ctx=#context{output=Output}) ->
    Ctx#context{output=[prepare_list(Title, Data) | Output]};
prepare_status({text, Text}, Ctx=#context{output=Output}) ->
    Ctx#context{output=[prepare_text(Text) | Output]};
prepare_status({table, Rows}, Ctx=#context{output=Output}) ->
    Ctx#context{output=[prepare_table(Rows) | Output]};
prepare_status(done, Ctx) ->
    Ctx.

prepare_list(Data) ->
    prepare_list(undefined, Data).

prepare_list(Title, Data) ->
    FlattenedData = [erlang:iolist_to_binary(S) || S <- Data],
    TitleProp = case Title of
                    undefined ->
                        [];
                    _ ->
                        [{<<"title">>, erlang:iolist_to_binary(Title)}]
                end,
    Props = lists:flatten([{<<"type">>, <<"list">>}, TitleProp, {<<"list">>, FlattenedData}]),
    maps:from_list(Props).

prepare_text(Text) ->
    #{<<"type">> => <<"text">>, <<"text">> => erlang:iolist_to_binary(Text)}.

prepare_table(Rows) ->
    TableData = [prepare_table_row(R) || R <- Rows],
    #{<<"type">> => <<"table">>, <<"table">> => TableData}.

prepare_table_row(Row) ->
    [{key_to_binary(K), prepare_table_value(V)} || {K, V} <- Row].

key_to_binary(Key) when is_atom(Key) ->
    list_to_binary(atom_to_list(Key));
key_to_binary(Key) when is_list(Key) ->
    list_to_binary(Key).

prepare_table_value(Value) when is_list(Value) ->
    %% TODO: This could definitely be done more efficiently.
    %% Maybe we could write a strip func that works directly on iolists?
    list_to_binary(string:strip(binary_to_list(iolist_to_binary(Value))));
prepare_table_value(Value) when is_pid(Value) ->
    list_to_binary(pid_to_list(Value));
prepare_table_value(undefined) ->
    null;
prepare_table_value(Value) ->
    Value.


