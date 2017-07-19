%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com]
%% @copyright Yariv Sadan 2006-2007
%%
%% @doc  Smerl: Simple Metaprogramming for Erlang
%%
%%  Smerl is an Erlang library
%%  that simplifies the creation and manipulation of Erlang modules in
%%  runtime.
%%
%%  You don't need to know Smerl in order to use ErlyWeb; Smerl
%%  is included in ErlyWeb because ErlyWeb uses it internally.
%%
%%  Smerl uses Erlang's capabilities for hot code swapping and
%%  abstract syntax tree transformations to do its magic. Smerl is inspired by
%%  the rdbms_codegen.erl module in the RDBMS application written by
%%  Ulf Wiger. RDBMS is part of Jungerl ([http://jungerl.sf.net]).
%%
%%  Here's a quick example illustrating how to use Smerl:
%%  ```
%%  test_smerl() ->
%%    M1 = smerl:new(foo),
%%    {ok, M2} = smerl:add_func(M1, "bar() -> 1 + 1."),
%%    smerl:compile(M2),
%%    foo:bar(),   % returns 2``
%%    smerl:has_func(M2, bar, 0). % returns true
%%  '''
%%
%%  New functions can be expressed either as strings of Erlang code
%%  or as abstract forms. For more information, read the Abstract Format
%%  section in the ERTS User's guide
%%   ([http://erlang.org/doc/doc-5.5/erts-5.5/doc/html/absform.html#4]).
%%
%%  Using the abstract format, the 3rd line of the above example
%%   would be written as
%%   ```
%%     {ok,M2} = smerl:add_func(M1, {function,1,bar,0,
%%                              [{clause,1,[],[],
%%                               [{op,1,'+',{integer,1,1},{integer,1,1}}]}]).
%%   '''
%%
%%   <p>The abstact format may look more verbose in this example, but
%%   it's also easier to manipulate in code.</p>
%%

%% Copyright (c) Yariv Sadan 2006-2007
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without restriction,
%% including without limitation the rights to use, copy, modify, merge,
%% publish, distribute, sublicense, and/or sell copies of the Software,
%% and to permit persons to whom the Software is furnished to do
%% so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included
%% in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
%% MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
%% IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
%% CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
%% TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
%% SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


-module(smerl).
-author("Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com").
-export([new/1,
         for_file/1,
         for_file/2,
         for_file/3,
         get_module/1,
         set_module/2,
         get_forms/1,
         set_forms/2,
         get_exports/1,
         set_exports/2,
         get_export_all/1,
         set_export_all/2,
         remove_export/3,
         add_func/2,
         add_func/3,
         remove_func/3,
         has_func/3,
         replace_func/2,
         %	 replace_func/3,
         compile/1,
         compile/2,
         rename/2,
         embed_all/2,
         to_src/1,
         to_src/2
        ]).

-define(L(Obj), io:format("LOG ~s ~w ~p\n", [?FILE, ?LINE, Obj])).
-define(S(Obj), io:format("LOG ~s ~w ~s\n", [?FILE, ?LINE, Obj])).

-include_lib("kernel/include/file.hrl").

%% @type meta_mod(). A data structure holding the abstract representation
%%  for a module.
%% @type func_form(). The abstract form for the function, as described
%%    in the ERTS Users' manual.

%% The record type holding the abstract representation for a module.
-record(meta_mod, {module, file, exports = [], forms = [],
                   export_all = false}).

%% @doc Create a new meta_mod for a module with the given name.
%%
%% @spec new(Module::atom()) -> meta_mod()
new(ModuleName) when is_atom(ModuleName) ->
    #meta_mod{module = ModuleName}.

%% @equiv for_file(SrcFilePath, [])
for_file(SrcFilePath) ->
    for_file(SrcFilePath, []).

%% @equiv for_file(SrcFilePath, IncludePaths, [])
for_file(SrcFilePath, IncludePaths) ->
    for_file(SrcFilePath, IncludePaths, []).

%% @doc Create a meta_mod for a module from its source file.
%%
%% @spec for_file(SrcFilePath::string(), IncludePaths::[string()],
%%   Macros::[{atom(), term()}]) ->
%%  {ok, meta_mod()} | {error, invalid_module}
for_file(SrcFilePath, IncludePaths, Macros) ->
    case epp:parse_file(SrcFilePath, [filename:dirname(SrcFilePath) |
                                      IncludePaths], Macros) of
        {ok, Forms} ->
            mod_for_forms(Forms);
        _err ->
            {error, {invalid_module, SrcFilePath}}
    end.

mod_for_forms([{attribute,_,file,{FileName,_FileNum}},
               {attribute, _, module, ModuleName}|Forms]) ->
    {Exports, OtherForms, ExportAll} =
    lists:foldl(
      fun({attribute, _, export, ExportList},
          {ExportsAcc, FormsAcc, ExportAll}) ->
              {ExportList ++ ExportsAcc, FormsAcc, ExportAll};
         ({attribute, _, compile, export_all},
          {ExportsAcc, FormsAcc, _ExportAll}) ->
              {ExportsAcc, FormsAcc, true};
         ({eof, _}, Acc) ->
              Acc;
         (Form, {ExportsAcc, FormsAcc, ExportAll}) ->
              {ExportsAcc, [Form | FormsAcc], ExportAll}
      end, {[], [], false}, Forms),
    {ok, #meta_mod{module = ModuleName,
                   file = FileName,
                   exports = Exports,
                   forms = OtherForms,
                   export_all = ExportAll
                  }};
mod_for_forms(Mod) ->
    {error, {invalid_module, Mod}}.

%% @doc Return the module name for the meta_mod.
%%
%% @spec(MetaMod::meta_mod()) -> atom()
get_module(MetaMod) ->
    MetaMod#meta_mod.module.

%% @doc Set the meta_mod's module name.
%%
%% @spec set_module(MetaMod::meta_mod(), NewName::atom()) ->
%%   NewMod::meta_mod()
set_module(MetaMod, NewName) ->
    MetaMod#meta_mod{module = NewName}.

%% @doc Return the list of function forms in the meta_mod.
%%
%% @spec get_forms(MetaMod::meta_mod()) -> [Form]
get_forms(MetaMod) ->
    MetaMod#meta_mod.forms.

set_forms(MetaMod, Forms) ->
    MetaMod#meta_mod{forms = Forms}.

%% @doc Return the list of exports in the meta_mod.
%%
%% @spec get_exports(MetaMod::meta_mod()) ->
%%   [{FuncName::atom(), Arity::integer()}]
get_exports(MetaMod) ->
    case MetaMod#meta_mod.export_all of
        false ->
            MetaMod#meta_mod.exports;
        true ->
            lists:foldl(
              fun({function, _L, Name, Arity, _Clauses}, Exports) ->
                      [{Name, Arity} | Exports];
                 (_Form, Exports) ->
                      Exports
              end, [], MetaMod#meta_mod.forms)
    end.

%% @doc Set the meta_mod's export list to the new list.
%%
%% @spec set_exports(MetaMod::meta_mod(),
%%   Exports::[{FuncName::atom(), Arity::integer()}]) -> NewMod::meta_mod()
set_exports(MetaMod, Exports) ->
    MetaMod#meta_mod{exports = Exports}.

%% @doc Get the export_all value for the module.
%%
%% @spec get_export_all(MetaMod::meta_mod) -> true | false
get_export_all(MetaMod) ->
    MetaMod#meta_mod.export_all.

%% @doc Set the export_all value for the module.
%%
%% @spec set_export_all(MetaMod::meta_mod(), Val::true | false) ->
%%   NewMetaMod::meta_mod()
set_export_all(MetaMod, Val) ->
    MetaMod#meta_mod{export_all = Val}.

%% @doc Remove the export from the list of exports in the meta_mod.
%%
%% @spec remove_export(MetaMod::meta_mod(), FuncName::atom(),
%%   Arity::integer()) -> NewMod::meta_mod()
remove_export(MetaMod, FuncName, Arity) ->
    MetaMod#meta_mod{exports =
                     lists:delete({FuncName, Arity},
                                  MetaMod#meta_mod.exports)}.

%% @doc Add a new function to the meta_mod and return the resulting meta_mod.
%% This function calls add_func(MetaMod, Form, true).
%%
%% @spec add_func(MetaMod::meta_mod(), Form::func_form() | string()) ->
%%   {ok, NewMod::meta_mod()} | {error, parse_error}
add_func(MetaMod, Form) ->
    add_func(MetaMod, Form, true).

%% @doc Add a new function to the meta_mod and return the new MetaMod
%% record. Export is a boolean variable indicating if the function should
%% be added to the module's exports.
%%
%% @spec add_func(MetaMod::meta_mod(), Func::func_form() | string(),
%%   Export::boolean()) ->
%%   {ok, NewMod::meta_mod()} | {error, parse_error}
add_func(MetaMod, Func, Export) when is_list(Func) ->
    case parse_func_string(Func) of
        {ok, Form} ->
            add_func(MetaMod, Form, Export);
        Err ->
            Err
    end;
add_func(MetaMod, {function, _Line, FuncName, Arity, _Clauses} = Form,
         true) ->
    Foo = {ok, MetaMod#meta_mod{
                 exports = [{FuncName, Arity} | MetaMod#meta_mod.exports],
                 forms = [Form | MetaMod#meta_mod.forms]
                }},
    Foo;
add_func(MetaMod, {function, _Line, _FuncName, _Arity, _Clauses} = Form,
         false) ->
    {ok, MetaMod#meta_mod{forms = [Form | MetaMod#meta_mod.forms]}};

%%add_func(MetaMod, Name, Fun) when is_function(Fun) ->
%%    add_func(MetaMod, Name, Fun, true);

add_func(_, _, _) ->
    {error, parse_error}.

%% add_func(MetaMod, Name, Fun, Export) when is_function(Fun) ->
%%     case form_for_fun(Name, Fun) of
%% 	{ok, Form} ->
%% 	    add_func(MetaMod, Form, Export);
%% 	Err ->
%% 	    Err
%%     end.

%% form_for_fun(Name, Fun) ->
%%     Line = 999,
%%     Info = erlang:fun_info(Fun),
%%     case Info of
%% 	[{module, _ModName}, _FuncName, _Arity, _Env, {type, external}] ->
%% 	    {error, cant_add_external_funcs};
%% 	[_Pid, _Module, _NewIdx, _NewUniq, _Index, _Uniq, _Name,
%% 	 {arity, Arity},
%% 	 {env, [Vars, _Unknown1, _Unknown2, Clauses]},
%% 	 {type, local}] ->
%% 	    EnvVars = lists:map(
%% 			fun({VarName, Val}) ->
%% 				{match,Line,{var,Line,VarName},
%% 				 erl_parse:abstract(Val)}
%% 			end, Vars),
%% 	    NewClauses = lists:map(
%% 			   fun({clause, Line1, Params, Guards, Exprs}) ->
%% 				   {clause, Line1, Params, Guards,
%% 				    EnvVars ++ Exprs}
%% 			   end, Clauses),
%% 	    {ok, {function, Line, Name, Arity, NewClauses}};
%% 	_Other ->
%% 	    {error, bad_fun}
%%     end.


parse_func_string(Func) ->
    case erl_scan:string(Func) of
        {ok, Toks, _} ->
            case erl_parse:parse_form(Toks) of
                {ok, _Form} = Res ->
                    Res;
                _Err ->
                    {error, parse_error}
            end;
        _Err ->
            {error, parse_error}
    end.

%% @doc Try to remove the function from the meta_mod.
%% If the function exists, the new meta_mod is returned. Otherwise,
%% original meta_mod is returned.
%%
%% @spec remove_func(MetaMod::meta_mod(), FuncName::string(), Arity::integer())
%%   -> NewMod::meta_mod()
%%
remove_func(MetaMod, FuncName, Arity) ->
    MetaMod#meta_mod{forms =
                     lists:filter(
                       fun({function, _Line, FuncName1, Arity1, _Clauses})
                             when FuncName1 =:= FuncName, Arity =:= Arity1->
                               false;
                          (_) ->
                               true
                       end, MetaMod#meta_mod.forms),
                     exports =
                     lists:filter(
                       fun({FuncName1, Arity1})
                             when FuncName1 =:= FuncName,
                                  Arity1 =:= Arity ->
                               false;
                          (_) ->
                               true
                       end, MetaMod#meta_mod.exports)
                    }.

%% @doc Check whether the meta_mod has a function with the given name
%%   and arity.
%% @spec has_func(MetaMod::meta_mod(), FuncName::atom(), Arity::integer()) ->
%%   bool()
has_func(MetaMod, FuncName, Arity) ->
    lists:any(fun({function, _Line, FuncName1, Arity1, _Clauses})
                    when FuncName1 == FuncName, Arity1 == Arity ->
                      true;
                 (_) ->
                      false
              end, MetaMod#meta_mod.forms).


%% @doc
%% Replace an existing function with the new one. If the function doesn't exist
%% the new function is added to the meta_mod.
%% This is tantamount to calling smerl:remove_func followed by smerl:add_func.
%%
%% @spec replace_func(MetaMod::meta_mod(), Function::string() | func_form()) ->
%%   {ok, NewMod::meta_mod()} | {error, Error}
replace_func(MetaMod, Function) when is_list(Function) ->
    case parse_func_string(Function) of
        {ok, Form} ->
            replace_func(MetaMod, Form);
        Err ->
            Err
    end;
replace_func(MetaMod, {function, _Line, FuncName, Arity, _Clauses} = Form) ->
    Mod1 = remove_func(MetaMod, FuncName, Arity),
    add_func(Mod1, Form);
replace_func(_MetaMod, _) ->
    {error, parse_error}.

%% %% @doc Simliar to replace_func/2, but accepts a function
%% %%   name + fun expression.
%% %%
%% %% @spec replace_func(MetaMod::meta_mod(), Name::atom(), Fun::function()) ->
%% %%   {ok, NewMod::meta_mod()} | {error, Error}
%% replace_func(MetaMod, Name, Fun) when is_function(Fun) ->
%%     case form_for_fun(Name, Fun) of
%% 	{ok, Form} ->
%% 	    replace_func(MetaMod, Form);
%% 	Err ->
%% 	    Err
%%     end.

%% @doc Compile the module represented by the meta_mod and load the
%% resulting BEAM into the emulator. This function calls
%% compile(MetaMod, [report_errors, report_warnings]).
%%
%% @spec compile(MetaMod::meta_mod()) -> ok | {error, Error}
compile(MetaMod) ->
    compile(MetaMod, undefined).

%% @doc Compile the module represented by the meta_mod and load the
%% resulting BEAM into the emulator. 'Options' is a list of options as
%% described in the 'compile' module in the Erlang documentation.
%%
%% If the 'outdir' option is provided,
%% the .beam file is written to the destination directory.
%%
%% @spec compile(MetaMod::meta_mod(), Options::[term()]) -> ok | {error, Error}
compile(MetaMod, undefined) ->
    compile(MetaMod, [report_errors, report_warnings,
                      return_errors]);

compile(MetaMod, Options) ->
    Forms = [{attribute, 2, module, MetaMod#meta_mod.module},
             {attribute, 3, export, get_exports(MetaMod)}],
    FileName =
    case MetaMod#meta_mod.file of
        undefined -> atom_to_list(get_module(MetaMod));
        Val -> Val
    end,

    Forms1 = [{attribute, 1, file, {FileName, 1}} | Forms],
    Forms2 = Forms1 ++ lists:reverse(MetaMod#meta_mod.forms),

    case compile:forms(Forms2, Options) of
        {ok, Module, Bin} ->
            Res =
            case lists:keysearch(outdir, 1, Options) of
                {value, {outdir, OutDir}} ->
                    file:write_file(
                      OutDir ++
                      ['/' | atom_to_list(MetaMod#meta_mod.module)] ++
                      ".beam", Bin);
                false -> ok
            end,
            case Res of
                ok ->
                    code:purge(Module),
                    case code:load_binary(
                           Module,
                           atom_to_list(Module) ++ ".erl", Bin) of
                        {module, _Module} ->
                            ok;
                        Err ->
                            Err
                    end;
                Err ->
                    Err
            end;
        Err ->
            Err
    end.

%% @doc Change the name of the function represented by the form.
%%
%% @spec rename(Form::func_form(), NewName::atom()) ->
%%   {ok, NewForm::func_form()} | {error, Err}
rename({function, Line, _Name, Arity, Clauses}, NewName) ->
    {function, Line, NewName, Arity, Clauses}.

replace_vars(Clause, Vals) ->
    Tree =
    erl_syntax_lib:map(
      fun({var,_L2,Name} = Expr) ->
              case proplists:lookup(Name, Vals) of
                  none ->
                      Expr;
                  {_, Val} ->
                      Val
              end;
         (Expr) ->
              Expr
      end, Clause),
    {clause, _, _, _, NewExprs} = erl_syntax:revert(Tree),
    NewExprs.

%% @doc This function takes a function form and list of name/value pairs,
%% and replaces all the function's parameters that whose names match an
%% element from the list with the predefined value.
%%
%% @spec embed_params(Func::func_form(),
%%   Vals::[{Name::atom(), Value::term()}]) -> NewForm::func_form()
embed_params({function, L, Name, Arity, Clauses}, Vals) ->
    NewClauses =
    lists:map(
      fun({clause, L1, Params, Guards, _Exprs} = Clause) ->
              {EmbeddedVals, OtherParams} =
              lists:foldr(
                fun({var,_, VarName} = Param, {Embedded, Rest}) ->
                        case proplists:lookup(VarName, Vals) of
                            none ->
                                {Embedded, [Param | Rest]};
                            {_, Val} ->
                                {[{VarName, erl_parse:abstract(Val)} |
                                  Embedded], Rest}
                        end;
                   (Param, {Embedded, Rest}) ->
                        {Embedded, [Param | Rest]}
                end, {[], []}, Params),
              NewExprs = replace_vars(Clause, EmbeddedVals),
              {clause, L1, OtherParams, Guards, NewExprs}


              %% 		  {Params1, Matches1, _RemainingVals} =
              %% 		      lists:foldl(
              %% 			fun({var, _L2, ParamName} = Param,
              %% 			    {Params2, Matches2, Vals1}) ->
              %% 				case lists:keysearch(ParamName, 1, Vals1) of
              %% 				    {value, {_Name, Val} = Elem} ->
              %% 					Match = {match, L1, Param,
              %% 						 erl_parse:abstract(Val)},
              %% 					{Params2, [Match | Matches2],
              %% 					 lists:delete(Elem, Vals1)};
              %% 				    false ->
              %% 					{[Param | Params2], Matches2, Vals1}
              %% 				end;
              %% 			   (Param, {Params2, Matches2, Vals1}) ->
              %% 				{[Param | Params2], Matches2, Vals1}
              %% 			end, {[], [], Vals}, Params),
              %% 		  [{clause, L1, lists:reverse(Params1), Guards,
              %% 				lists:reverse(Matches1) ++ Exprs} | Clauses1]
      end, Clauses),
    NewArity =
    case NewClauses of
        [{clause, _L2, Params, _Guards, _Exprs}|_] ->
            length(Params);
        _ ->
            Arity
    end,
    {function, L, Name, NewArity, NewClauses}.

%% @doc Apply the embed_params function with the list of {Name, Value}
%% pairs to all forms in the meta_mod. Exports
%% for functions whose arities change due to the embedding are preserved.
%%
%% @spec embed_all(MetaMod::meta_mod(), Vals::[{Name::atom(),
%%   Value::term()}]) -> NewMetaMod::meta_mod()
embed_all(MetaMod, Vals) ->
    Forms = get_forms(MetaMod),
    Exports = get_exports(MetaMod),
    {NewForms, Exports3, NewExports} =
    lists:foldl(
      fun({function, _L, Name, Arity, _Clauses} = Form,
          {Forms1, Exports1, NewExports1}) ->
              {function, _, _, NewArity, _} = NewForm =
              embed_params(Form, Vals),
              Exports2 = lists:delete({Name, Arity}, Exports1),
              NewExports2 =
              case length(Exports2) == length(Exports1) of
                  true ->
                      NewExports1;
                  false ->
                      [{Name, NewArity} | NewExports1]
              end,
              {[NewForm | Forms1], Exports2, NewExports2};
         (Form, {Forms1, Exports1, NewExports1}) ->
              {[Form | Forms1], Exports1, NewExports1}
      end, {[], Exports, []}, Forms),
    #meta_mod{module = get_module(MetaMod),
              exports = Exports3 ++ NewExports,
              forms = lists:reverse(NewForms),
              export_all = get_export_all(MetaMod)}.

%% @doc Return the pretty-printed source code for the module.
%%
%% @spec to_src(MetaMod::meta_mod()) -> string()
to_src(MetaMod) ->
    ExportsForm =
    {attribute,1,export,get_exports(MetaMod)},
    AllForms = [{attribute,1,module,get_module(MetaMod)}, ExportsForm |
                get_forms(MetaMod)],
    erl_prettypr:format(erl_syntax:form_list(AllForms)).

%% @doc Write the pretty printed source code for the module
%%   to the file with the given file name.
%%
%% @spec to_src(MetaMod::meta_mod(), FileName::string()) ->
%%   ok | {error, Error}
to_src(MetaMod, FileName) ->
    Src = to_src(MetaMod),
    file:write_file(FileName, list_to_binary(Src)).
