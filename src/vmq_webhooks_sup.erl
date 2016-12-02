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

start_link() ->
    case supervisor:start_link({local, ?SERVER}, ?MODULE, []) of
        {ok, _} = Ret ->
            spawn(fun() ->
                          ConfDir = application:get_env(vmq_webhooks, config_dir, code:priv_dir(vmq_webhooks)),
                          register_hooks(ConfDir)
                  end),
            Ret;
        E -> E
    end.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupFlags =
        #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs =
        [#{id => vmq_webhooks_plugin,
           start => {vmq_webhooks_plugin, start_link, []},
           restart => permanent,
           type => worker,
           modules => [vmq_webhooks_plugin]}],
    {ok, {SupFlags, ChildSpecs}}.
%%====================================================================
%% Internal functions
%%====================================================================


register_hooks(ConfDir) ->
    File = ConfDir ++ "/vmq_webhooks.conf",
    case file:consult(File) of
        {ok, [{hooks,1,Hooks}]} ->
            lists:foreach(
              fun({Endpoint, HookName, Opts}) when is_list(Endpoint),
                                                   is_atom(HookName),
                                                   is_list(Opts) ->
                      register_hook(list_to_binary(Endpoint), HookName, Opts);
                 (Unknown) ->
                      lager:warning("Unknown webhook: ~n", [Unknown])
              end,
              Hooks);
        {error, Reason} ->
            lager:error("Failed to load ~p, reason: ~p", [File, Reason])
    end.

register_hook(Endpoint, HookName, Opts) ->
    case vmq_webhooks_plugin:register_endpoint(Endpoint, HookName, maps:from_list(Opts)) of
        ok ->
            ok;
        {error, Reason} ->
            lager:error("Failed to register webhook: ~p ~p ~p, reason: ~p",
                        [Endpoint, HookName, Opts, Reason])
    end.
