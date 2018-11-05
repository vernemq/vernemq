%% Copyright 2018 Octavo Labs AG Basel Switzerland (http://erl.io)
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

-module(vmq_health_http).

-behaviour(vmq_http_config).

-export([routes/0]).

-export([handle/2, init/3, terminate/3]).

routes() ->
  [{"/health", ?MODULE, []}].

init(_Type, Req, _Opts) ->
  {ok, Req, undefined}.

handle(Req, State) ->
    {ContentType, Req2} = cowboy_req:header(<<"content-type">>, Req,
                                            <<"application/json">>),
    {ok, reply(Req2, ContentType), State}.

terminate(_Reason, _Req, _State) -> ok.

reply(Req, <<"application/json">>) ->
    {Code, Payload} = case check_health_concerns() of
      [] ->
        {200, [{<<"status">>, <<"OK">>}]};
      Concerns ->
        {503, [{<<"status">>, <<"DOWN">>},
               {<<"reasons">>, Concerns}]}
    end,
    {ok, Req2} = cowboy_req:reply(Code, [{<<"content-type">>, <<"application/json">>}],
                                  jsx:encode(Payload), Req),
    Req2.


-spec check_health_concerns() -> [] | [Concern :: string()].
check_health_concerns() ->
  lists:filtermap(
    fun(Status) ->
      case Status of
        ok -> false;
        {error, Reason} -> {true, list_to_binary(Reason)}
      end
    end, [cluster_status(), listeners_status()]).


-spec cluster_status() -> ok | {error, Reason :: string()}.
cluster_status() ->
  ThisNode = node(),
  try
    case vmq_cluster:status() of
      [] ->
        {error, "Unknown cluster status"};
      Status ->
        case lists:keyfind(ThisNode, 1, Status) of
          {ThisNode, true} -> ok;
          false -> {error, "Node has not joined cluster"}
        end
    end
  catch
    Exception:Reason ->
      lager:debug("Cluster status check failed ~p:~p", [Exception, Reason]),
      {error, "Unknown cluster status"}
  end.

-spec listeners_status() -> ok | {error, Reason :: string()}.
listeners_status() ->
  NotRunningListeners = lists:filtermap(
      fun({Type, _, _, Status, _, _}) ->
        case Status of
          running ->
            false;
          _ ->
            {true, Type}
        end
      end, vmq_ranch_config:listeners()),
  case NotRunningListeners of
    [] ->
      ok;
    Listeners ->
      {error, io_lib:format("Listeners are not ready: ~p", Listeners)}
  end.

