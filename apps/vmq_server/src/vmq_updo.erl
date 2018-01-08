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

-module(vmq_updo).
-include_lib("sasl/src/systools.hrl").

-export([run/0,
         run/1,
         dry_run/0,
         dry_run/1,
         dry_run_low/0,
         dry_run_low/1]).

run() ->
    translate_and_run(high_level_script()).

dry_run() ->
    high_level_script().

dry_run_low() ->
    {_, LLinstrs} = translate_and_check(high_level_script()),
    LLinstrs.

%%% The following functions are the same as above, except they take a file
%%% containing additional high or low level instructions that are appended to
%%% the automatically generated script.

run(Filename) ->
    translate_and_run(high_level_script(Filename)).

dry_run(Filename) ->
    high_level_script(Filename).

dry_run_low(Filename) ->
    {_, LLinstrs} = translate_and_check(high_level_script(Filename)),
    LLinstrs.


%%% ========================================================================
%%% Internal
%%% ========================================================================

high_level_script() ->
    script(updated_modules()).

high_level_script(Filename) ->
    high_level_script() ++ read_script(Filename).

updated_modules() ->
    [ MF || {Mod, Filename} = MF <- code:all_loaded(),
                                    is_list(Filename),
                                    not code:is_sticky(Mod),
                                    is_updated(Mod, Filename) ].

is_updated(Module, Filename) ->
    LoadedVer = proplists:get_value(vsn, Module:module_info(attributes)),
    case beam_lib:version(Filename) of
        {ok, {_, FileVer}} -> FileVer /= LoadedVer;
        {error, _, _}      -> false
    end.

read_script(Filename) ->
    case file:consult(Filename) of
	{ok, HLinstrs} ->
	    HLinstrs;
	{error, Error} when is_tuple(Error) ->
        error_logger:error_msg("failed to parse line ~s~n", [file:format_error(Error)]),
	    exit(parse_error);
	{error, Error} ->
        error_logger:error_msg("failed to open file ~s~n", [file:format_error(Error)]),
	    exit(Error)
    end.

%%%
%%% Generate high level upgrade scripts from the instruction set of
%%% appup files.
%%%
%%% See http://www.erlang.org/doc/man/appup.html
%%% http://www.erlang.org/doc/design_principles/appup_cookbook.html
%%%
%% Return a list of upgrade instructions for the given list of modules.
%%
script(ModFiles) ->
    lists:map(fun instruction/1, dependencies(ModFiles)).

%% Return the upgrade instruction for a module.
%%
instruction({Mod, Deps}) ->
    case is_supervisor(Mod) of
        true ->
            {update, Mod, supervisor};
        false ->
            case is_special(Mod) of
                true ->
                    {update, Mod, {advanced, []}, Deps};
                false ->
                    {load_module, Mod, Deps}
            end
    end.

%% Establish the dependencies between a list of modules.
%%
%% A tuple is returned for each module with its name and a (possibly
%% empty) list of the other modules it makes calls to.
%%
dependencies(ModFiles) ->
    {Mods, Filenames} = lists:unzip(ModFiles),
    {ok, Xref} = xref:start([{xref_mode, modules}]),
    {ok, Calls} =
    try add_files_to_xref(Xref, Filenames),
        xref:q(Xref, "strict ME || AM")
    after
        xref:stop(Xref)
    end,
    [ {Caller, proplists:get_all_values(Caller, Calls)} || Caller <- Mods ].
add_files_to_xref(Xref, [Filename|T]) ->
    {ok, _} = xref:add_module(Xref, Filename, [{warnings, false}]),
    add_files_to_xref(Xref, T);
add_files_to_xref(_, []) ->
    ok.
%% Is this the module for a "special process" (as it is known in OTP),
%% meaning a process running under a supervisor.
%%
is_special(Mod) ->
    Exports = Mod:module_info(exports),
    lists:member({code_change, 3}, Exports)
    orelse lists:member({system_code_change, 4}, Exports).
is_supervisor(Mod) ->
    Attrs = Mod:module_info(attributes),
    lists:member(supervisor, proplists:get_value(behaviour, Attrs, [])
                 ++ proplists:get_value(behavior, Attrs, [])).


translate_and_run(HLinstrs) ->
    {Apps, LLinstrs} = translate(HLinstrs, loaded_apps()),
    LibDirs = app_lib_dirs(Apps),
    check_script(LLinstrs, LibDirs),
    % The interface to release_handler_1 changed in R15.
    _ = code:ensure_loaded(release_handler_1),
    release_handler_1:eval_script(LLinstrs, [], LibDirs, LibDirs, []).

translate_and_check(HLinstrs) ->
    {Apps, LLinstrs} = translate(HLinstrs, loaded_apps()),
    LibDirs = app_lib_dirs(Apps),
    check_script(LLinstrs, LibDirs),
    {ok, LLinstrs}.

translate(HLinstrs, AppsNow) ->
    AppsAfter = apps_after(HLinstrs, AppsNow),
    AppsNowRecs = app_records(AppsNow),
    AppAfterRecs = app_records(AppsAfter),
    case systools_rc:translate_scripts([HLinstrs], AppAfterRecs, AppsNowRecs) of
	{ok, LLinstrs} ->
            {AppsAfter, LLinstrs};
	{error, systools_rc, Error} ->
        error_logger:error_msg(systools_rc:format_error(Error)),
	    exit(parse_error)
    end.

check_script(LLinstrs, LibDirs) ->
    case release_handler_1:check_script(LLinstrs, LibDirs) of
        {ok, _} ->
            ok;
        {error, Error} ->
            exit(Error)
    end.

apps_after(HLinstrs, Before) ->
    Added = [ Name || {add_application, Name} <- HLinstrs ],
    Removed = [ Name || {remove_application, Name} <- HLinstrs ],
    lists:usort(Before ++ Added) -- Removed.

loaded_apps() ->
    [ App || {App, _, _} <- application:loaded_applications() ].

app_records(Apps) ->
    [ #application{name = A, modules = find_app_modules(A)} || A <- Apps ].

find_app_modules(App) ->
    Ext = code:objfile_extension(),
    case code:lib_dir(App, ebin) of
	Path when is_list(Path) ->
	    Files = filelib:wildcard("*" ++ Ext, Path),
	    [ list_to_atom(filename:basename(F, Ext)) || F <- Files ];
	{error, _} ->
        error_logger:error_msg("can't find lib dir for application '~s'~n", [App]),
	    exit({unknown_application, App})
    end.

app_lib_dirs(Apps) ->
    [ {App, "", code:lib_dir(App)} || App <- Apps ].
