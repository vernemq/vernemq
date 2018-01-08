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
-module(vmq_tracer_cli).

-export([register_cli/0]).

register_cli() ->
    clique:register_usage(["vmq-admin", "trace"], trace_usage()),
    clique:register_usage(["vmq-admin", "trace", "client"], trace_client_usage()),
    
    trace_client_cmd().

trace_client_cmd() ->
    Cmd = ["vmq-admin", "trace", "client"],
    KeySpecs = [{'client-id',
                 [{typecast, fun(CId) -> list_to_binary(CId) end}]}
               ],
    FlagSpecs =
        [
         {rate_max, [{longname, "rate-max"},
                     {typecast, fun(RM) -> list_to_integer(RM) end}]},
         {mountpoint, [{longname, "mountpoint"},
                       {typecast, fun(MP) -> MP end}]},
         {rate_interval, [{longname, "rate-interval"},
                          {typecast, fun(RI) -> list_to_integer(RI) end}]},
         {trunc_payload, [{longname, "trunc-payload"},
                          {typecast, fun(TP) -> list_to_integer(TP) end}]}
        ],
    
    Callback =
        fun(_, Keys, Flags) ->
                RM = proplists:get_value(rate_max, Flags, 10),
                RI = proplists:get_value(rate_interval, Flags, 100),
                TP = proplists:get_value(trunc_payload, Flags, 200),
                MP = proplists:get_value(mountpoint, Flags, ""),
                case proplists:get_value('client-id', Keys) of
                    undefined ->
                        Text = clique_status:text(trace_client_usage()),
                        [clique_status:alert([Text])];
                    CId ->
                        %% the group leader comes from the calling node
                        %% as this is invoked via the rpc
                        %% mechanism. Monitoring it ensures that this
                        %% process is notified when the calling node
                        %% and group_leader are terminated (user
                        %% pressing ^C)
                        _MRef = monitor(process, group_leader()),
                        TOpts =
                            #{ io_server => erlang:group_leader(),
                               max_rate => {RM,RI},
                               client_id => CId,
                               mountpoint => MP,
                               payload_limit => TP},
                        case vmq_tracer:start_link(TOpts) of
                            {ok, Pid} ->
                                monitor(process, Pid),
                                vmq_tracer:trace_existing_session(),
                                trace_loop(),
                                Text = clique_status:text("Done."),
                                [clique_status:alert([Text])];
                            {error, {already_started, _}} ->
                                Text = clique_status:text("Cannot start trace as another trace is already running."),
                                [clique_status:alert([Text])]
                        end
                end
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

trace_loop() ->
    receive
        {'DOWN', _MRef, _, _, _} ->
            ok;
        _ ->
            trace_loop()
    end.

trace_usage() ->
    ["vmq-admin trace <sub-command>\n\n",
     "  Trace various aspects of VerneMQ.\n\n",
     "  The tracing sub-system is a tool to track down and debug configuration and\n"
     "  production issues, but note that the output format and features cannot be\n"
     "  considered stable and may change between releases of VerneMQ without warning.\n\n"
     "  Sub-commands:\n",
     "    client      Trace client activities\n",
     "  Use --help after a sub-command for more details.\n"
    ].

trace_client_usage() ->
    ["vmq-admin trace client client-id=<client-id>\n\n",
     "  Trace client activities.\n\n",
     "  Note, tracing has a performance impact on the running system and\n",
     "  as such care should be taken, especially when running high load.\n",
     "  This is also the reason for the low default rate limit settings.\n\n",
     "Options\n\n",
     "  --mountpoint=<Mountpoint>\n",
     "      the mountpoint for the client to trace.\n"
     "      Defaults to \"\" which is the default mountpoint.\n"
     "  --rate-max=<RateMaxMessages>\n",
     "      the maximum number of messages for the given interval,\n",
     "      defaults to 10.\n",
     "  --rate-interval=<RateIntervalMS>\n",
     "      the interval in milliseconds over which the max number of messages\n",
     "      is allowed. Defaults to 100.\n",
     "  --trunc-payload=<SizeInBytes>\n",
     "      control when the payload should be truncated for display.\n",
     "      Defaults to 200.\n"
    ].
    
    
