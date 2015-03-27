{export, "cover.log"}.
{level, details}.
{incl_dirs, ["../../src"]}.
{incl_mods, [
%% improve ugly handish import
    vmq_auth
    , vmq_msg_store     
    , vmq_server_app       
    , vmq_session_proxy
    , vmq_cluster           
    , vmq_queue         
    , vmq_session_proxy_sup
    , vmq_cluster_node      
    , vmq_ranch_config  
    , vmq_server_cli       
%    , vmq_ssl
    , vmq_cluster_node_sup  
    , vmq_ranch         
    , vmq_server_cmd       
    , vmq_sysmon
    , vmq_config_cli        
    , vmq_reg           
    , vmq_server           
    , vmq_sysmon_handler
    , vmq_config            
    , vmq_reg_leader    
%    , vmq_updo
%    , vmq_crl_srv           
%    , vmq_reg_pets      
    , vmq_server_sup       
%    , vmq_websocket
    , vmq_exo               
    , vmq_reg_sup       
    , vmq_session
    , vmq_listener_cli      
    , vmq_reg_trie      
    , vmq_session_expirer
    ]}.

