-include_lib("emqtt_commons/include/emqtt_frame.hrl").
-type plugin_id()       :: {plugin, atom(), pid()}.


-record(vmq_msg, {
          msg_ref               :: msg_ref(),
          routing_key           :: routing_key(),
          payload               :: payload(),
          retain=false          :: flag(),
          dup=false             :: flag(),
          qos                   :: qos()
         }).

-type msg()             :: #vmq_msg{}.
