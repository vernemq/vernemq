-ifndef('mzmetrics_header.hrl').
-define('mzmetrics_header.hrl', include_protection).

%% A metric Family is the highest level namespace for the metrics activity.
%% Metrics in one Family are different from metrics in other families.
-type family_name() :: non_neg_integer().

%% A resource is a user-level addressable entity for which we keep different
%% counter values. The kinds of counters that we keep for a resource are fixed
%% per family. Typically you'd have the names of your units of accounting
%% (such as domain names, rabbit names, car plates) as resources,
%% and each of these would have a few counters, the list of which is the same
%% across all such resources (across all domains, etc) in one family.
%%
%% Think of family as a hash and resource_name() as a hash key.
-type resource_name() :: nonempty_string().

%% Maximum number of families supported by the runtime.
%% Feel free to change that, but it'll require a VM reload to affect change.
-define(MAX_FAMILIES, 2).

%% For each Rabbit we define 8 counters.
-define(METRICS_FAMILY_RABBITS, 0).
-define(MAX_CNTRS_PER_RABBIT, 8).

%% For each Car we define 8 counters.
-define(METRICS_FAMILY_CARS, 1).
-define(MAX_CNTRS_PER_CAR, 8).

-define(MAX_CNTRS_PER_UNKNOWN_FAMILY, 8).

%% The Flags argument to mzmetrics:get_all_resources(, Flags :: integer()).
-define(CNTR_RESET, 1).
-define(CNTR_DONT_RESET, 0).

-endif.
