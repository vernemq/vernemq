
-type metric_label() :: {atom(), string()}.

-type metric_id() :: atom().

-record(metric_def,
        {type        :: atom(),
         labels      :: [metric_label()],
         id          :: metric_id(),
         name        :: atom(),
         description :: undefined | binary()}).
-type metric_def() :: #metric_def{}.

-type metric_val() :: {Def :: metric_def(), Val :: any()}.
