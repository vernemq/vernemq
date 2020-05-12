-module(vmq_cli_human_writer).

%% @doc This module provides callback functions to the status parsing code in
%% clique_status:parse/3. It specifically formats the output for a human at the
%% console and handles an opaque context passed back during parsing.

-behavior(clique_writer).

%% API
-export([write/1]).

-include_lib("clique/include/clique_status_types.hrl").
-include_lib("stdout_formatter/include/stdout_formatter.hrl").

-record(context, {alert_set=false :: boolean(),
                  output="" :: iolist()}).

-spec write(status()) -> {iolist(), iolist()}.
write(Status) ->
    Ctx = clique_status:parse(Status, fun write_status/2, #context{}),
    {Ctx#context.output, []}.

%% @doc Write status information in console format.
-spec write_status(elem(), #context{}) -> #context{}.
write_status(alert, Ctx=#context{alert_set=false}) ->
    Ctx#context{alert_set=true};
write_status(alert, Ctx) ->
    %% TODO: Should we just return an error instead?
    throw({error, nested_alert, Ctx});
write_status(alert_done, Ctx) ->
    Ctx#context{alert_set=false};
write_status({list, Data}, Ctx=#context{output=Output}) ->
    Ctx#context{output=Output++write_list(Data)};
write_status({list, Title, Data}, Ctx=#context{output=Output}) ->
    Ctx#context{output=Output++write_list(Title, Data)};
write_status({text, Text}, Ctx=#context{output=Output}) ->
    Ctx#context{output=Output++Text++"\n"};
write_status({table, Rows}, Ctx=#context{output=Output}) ->
    Ctx#context{output=Output++write_table(Rows)};
write_status(done, Ctx) ->
    Ctx.

-spec write_table([{iolist(), iolist()}]) -> iolist().
write_table([]) ->
    "";
write_table(Rows0) ->
    Header = [Name || {Name, _Val} <- hd(Rows0)],
    Rows = [[Val || {_Name, Val} <- Row] || Row <- Rows0],
    Table = stdout_formatter:to_string(
        #table{rows=[#row{cells = Header, props=#{title => true}} | Rows], 
        props=#{cell_padding => {0,1}, border_drawing => ascii}}),
    io_lib:format("~ts~n", [Table]).

%% @doc Write a list horizontally
write_list(Title, Items) when is_atom(Title) ->
    write_list(atom_to_list(Title), Items);
%% Assume all items are of same type
write_list(Title, Items) when is_atom(hd(Items)) ->
    Items2 = [atom_to_list(Item) || Item <- Items],
    write_list(Title, Items2);
write_list(Title, Items) ->
    %% Todo: add bold/color for Title when supported
    Title ++ ":" ++ write_list(Items) ++ "\n".

write_list(Items) ->
    lists:foldl(fun(Item, Acc) ->
                    Acc++" "++Item
                end, "", Items).