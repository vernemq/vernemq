%%%-------------------------------------------------------------------
%% @doc vmq_webhooks top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vmq_webhooks_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
-spec start_link() -> 'ignore' | {'error', any()} | {'ok', pid()}.
start_link() ->
    case supervisor:start_link({local, ?SERVER}, ?MODULE, []) of
        {ok, _} = Ret ->
            spawn(fun() ->
                Webhooks = application:get_env(vmq_webhooks, user_webhooks, []),
                register_webhooks(Webhooks)
            end),
            Ret;
        E ->
            E
    end.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupFlags =
        #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs =
        [
            #{
                id => vmq_webhooks_plugin,
                start => {vmq_webhooks_plugin, start_link, []},
                restart => permanent,
                type => worker,
                modules => [vmq_webhooks_plugin]
            }
        ],
    {ok, {SupFlags, ChildSpecs}}.
%%====================================================================
%% Internal functions
%%====================================================================
-spec register_webhooks([{_, map()}]) -> [any()].
register_webhooks(Webhooks) ->
    [register_webhook(Webhook) || Webhook <- Webhooks].

-spec register_webhook({_, #{'endpoint' := binary(), 'hook' := atom(), 'options' := _, _ => _}}) ->
    any().
register_webhook({Name, #{hook := HookName, endpoint := Endpoint, options := Opts}}) ->
    case vmq_webhooks_plugin:register_endpoint(Endpoint, HookName, Opts) of
        ok ->
            ok;
        {error, Reason} ->
            lager:error(
                "failed to register the ~p webhook ~p ~p ~p due to ~p",
                [Name, Endpoint, HookName, Opts, Reason]
            )
    end.
