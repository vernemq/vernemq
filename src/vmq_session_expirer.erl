-module(vmq_session_expirer).

-behaviour(gen_server).

%% API
-export([start_link/0,
         change_config_now/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {duration=0}).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link() -> {ok, pid()} | ignore | {error, atom()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec change_config_now(_,[any()],_) -> 'ok'.
change_config_now(_New, Changed, _Deleted) ->
    %% we are only interested if the config changes
    {_, NewDuration} = proplists:get_value(persistent_client_expiration, Changed, {undefined,"never"}),
    ParsedDuration = parse_duration(NewDuration),
    gen_server:cast(?MODULE, {new_duration, ParsedDuration}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
-spec init([any()]) -> {ok, #state{}}.
init([]) ->
    Duration = application:get_env(vmq_server, persistent_client_expiration, "never"),
    ParsedDuration = parse_duration(Duration),
    erlang:send_after(5000, self(), expire_clients),
    {ok, #state{duration=ParsedDuration}}.

-spec handle_call(_, _, #state{}) -> {reply, ok, #state{}}.
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

-spec handle_cast(_, #state{}) -> {noreply, #state{}}.
handle_cast({new_duration, Duration}, State) ->
    {noreply, State#state{duration=Duration}}.

-spec handle_info(_, #state{}) -> {noreply, #state{}}.
handle_info(expire_clients, State) ->
    case State#state.duration of
        0 ->
            ok;
        Duration ->
            vmq_reg:remove_expired_clients(Duration)
    end,
    erlang:send_after(5000, self(), expire_clients),
    {noreply, State}.

-spec terminate(_, #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, #state{}, _) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
parse_duration("never") -> 0;
parse_duration(Duration) ->
    [Entity|D] = lists:reverse(Duration),
    parse_duration(Entity, list_to_integer(lists:reverse(D))).
parse_duration("h", Duration) -> Duration * 60 * 60;
parse_duration("d", Duration) -> Duration * parse_duration("h", 24);
parse_duration("w", Duration) -> Duration * parse_duration("d", 7);
parse_duration("m", Duration) -> Duration * parse_duration("w", 4);
parse_duration("y", Duration) -> Duration * parse_duration("m", 12).
