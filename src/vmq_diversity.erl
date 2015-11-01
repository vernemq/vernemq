-module(vmq_diversity).

-export([load_script/1,
         reload_script/1]).

load_script(Script) when is_list(Script) ->
    case filelib:is_file(Script) of
        true ->
            vmq_diversity_script_sup:start_script(Script);
        false ->
            {error, cant_find_script}
    end.

reload_script(Script) ->
    vmq_diversity_script_sup:reload_script(Script).
