-record(hook, {
    name :: atom(),
    plugin :: atom() | undefined,
    module :: module(),
    function :: atom(),
    arity :: non_neg_integer(),
    compat ::
        {Name :: atom(), Mod :: module(), Fun :: atom(), Arity :: arity()}
        | undefined,
    opts = [] :: [any()]
}).
