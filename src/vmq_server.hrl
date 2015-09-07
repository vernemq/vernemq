-include_lib("vmq_commons/include/vmq_types.hrl").
-type plugin_id()       :: {plugin, atom(), pid()}.

-record(vmq_msg, {
          msg_ref               :: msg_ref(),
          routing_key           :: routing_key(),
          payload               :: payload(),
          retain=false          :: flag(),
          dup=false             :: flag(),
          qos                   :: qos(),
          trade_consistency=false :: boolean(),
          reg_view=vmq_reg_trie   :: atom(),
          mountpoint            :: mountpoint(),
          persisted=false       :: flag()
         }).
-type msg()             :: #vmq_msg{}.
