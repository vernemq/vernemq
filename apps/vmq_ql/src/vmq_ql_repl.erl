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

-module(vmq_ql_repl).

-export([start/0,
         repl_start/0]).

-define(PROMPT, "vmql> ").
-define(SUB_PROMPT, "> ").

-record(state, {remote_node, query_pid}).

start() ->
    case init:get_plain_arguments() of
        ["-vmql_eval"|As] ->
            user:start(),
            run_string(As);
        [S|As] ->
            user:start(),
            run_file([S|As]);
        [] ->
            user_drv:start(['tty_sl -c -e', {vmq_ql_repl, repl_start, []}])
    end.

run_file(_) ->
    io:put_chars(user, "not implemented\n").

run_string(_) ->
    io:put_chars(user, "not implemented\n").


init_state() ->
    os:cmd(os:find_executable("epmd") ++ " -daemon"),
    %% maybe start distribution
    case proplists:get_value(remote_node, init:get_arguments()) of
        undefined ->
            init:stop();
        [RemoteNode] ->
            case ets:info(sys_dist) of
                undefined ->
                    %% started without -sname or -name arg
                    NodenameStr = "vmql-" ++ integer_to_list(erlang:phash2(os:timestamp())),
                    StartOpts =
                    case length(re:split(RemoteNode, "@")) of
                        1 -> % shortname
                            [list_to_atom(NodenameStr), shortnames];
                        2 -> % longname
                            {ok, Hostname} = inet:gethostname(),
                            [list_to_atom(NodenameStr ++ "@" ++ Hostname), longnames]
                    end,
                    {ok, _} = net_kernel:start(StartOpts);
                _ ->
                    ok
            end,
            Cookie = list_to_atom(proplists:get_value(cookie, init:get_arguments(), "vmq")),
            erlang:set_cookie(node(), Cookie),
            RemoteNodeAtom = list_to_atom(RemoteNode),
            #state{remote_node=RemoteNodeAtom}
    end.

repl_start() ->
    State = init_state(),
    spawn(fun() -> server(State) end).

server(State) ->
    process_flag(trap_exit, true),
    io:put_chars(make_banner()),
    Eval = start_eval(State),
    server_loop(Eval, State).

start_eval(State) ->
    Self = self(),
    spawn_link(fun() -> eval_loop(Self, State) end).

eval_loop(Shell, #state{remote_node=Remote} = State) ->
    receive
        {eval_query, Shell, QueryStr} ->
            case rpc:call(Remote, vmq_ql_query_mgr, rpc_query, [Shell, QueryStr], 5000) of
                {badrpc, Error} ->
                    io:format(user, "Can't connect to ~p due to~p~n", [Remote, Error]);
                QueryPid ->
                    QueryPid ! {fetch, 1}
            end,
            eval_loop(Shell, State)
    end.

shell_eval("more\n", Eval, #state{query_pid=QueryPid} = State) when is_pid(QueryPid) ->
    QueryPid ! {fetch, 1},
    query_result_loop(Eval, State);
shell_eval("more\n", Eval, State) ->
    io:format("Outch, no runninq query!~n", []),
    {Eval, State};
shell_eval("\n", Eval, State) ->
    {Eval, State};
shell_eval(QueryString, Eval, State) ->
    case State#state.query_pid of
        undefined ->
            ignore;
        QueryPid ->
            QueryPid ! cancel
    end,
    Eval ! {eval_query, self(), QueryString},
    query_result_loop(Eval, State).

query_result_loop(Eval, State) ->
    receive
        {query_result, QueryPid, Result} ->
            io:format(user, "~p~n", [Result]),
            {Eval, State#state{query_pid=QueryPid}};
        end_of_result ->
            io:format(user, "end of result~n", []),
            {Eval, State#state{query_pid=undefined}};
        {'EXIT', Eval, Error} ->
            io:format(user, "Eval exited, ~p~n", [Error]),
            Eval1 = start_eval(State),
            {Eval1, State}
    end.

server_loop(Eval, State) ->
    Prompt = "vmql> ",
    {Ret, Eval1} = read_line(Prompt, Eval, State),
    case Ret of
        {ok, QueryString} ->
            {Eval2, State1} = shell_eval(QueryString, Eval1, State),
            server_loop(Eval2, State1);
        {error, _E} ->
            server_loop(Eval1, State)
    end.

read_line(Prompt, Eval, State) ->
    Read = fun() ->
                   Ret = read_line(Prompt),
                   exit(Ret)
           end,
    Reader = spawn_link(Read),
    read_line_(Reader, Eval, State).

read_line_(Reader, Eval, _State) ->
    receive
        {'EXIT', Reader, Ret} ->
            {Ret, Eval};
        {'EXIT', Eval, {Reason, _Stack}} ->
            {error, Reason};
        {'EXIT', Eval, Reason} ->
            {error, Reason}
    end.

read_line(Prompt) ->
    case io:get_line(standard_io, Prompt) of
        {error, Error} -> {error, Error};
        Line ->
            {ok, Line}
    end.

make_banner() ->
    lists:flatten([
                   "   _   ____  _______    __  \n",
                   "  | | / /  |/  / __ \\  / /  \n",
                   "  | |/ / /|_/ / /_/ / / /__ \n",
                   "  |___/_/  /_/\\___\\_\\/____/ \n",
                   "                            \n"
                  ]).
