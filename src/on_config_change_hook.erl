-module(on_config_change_hook).
-include("vmq_types.hrl").

-type config_val()          :: {Property :: atom(), Value :: any()}.
-type application_config()  :: {Application :: atom(), [config_val()]}.
-type all_config()          :: [application_config()].

%% called as an 'all'-hook, return value is ignored
-callback change_config(AllConfig :: all_config()) -> any().

-export_type([all_config/0, application_config/0, config_val/0]).
