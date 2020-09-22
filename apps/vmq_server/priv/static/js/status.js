// Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
$(function() {
    var config = {
        reload_interval: 10000,
        template: $("#node_list_template").html(),
        target: $("#node_list"),
        cluster_status: {
            url: "status.json",
            last_calculated: Date.now(),
            rates: {}
        }
    };

    function calc_rate(node_name, name, interval, value) {
        var node_rates = config.cluster_status.rates[node_name];
        if (typeof node_rates === 'undefined') {
            node_rates = {}
        }
        var last_value = node_rates[name];
        if (typeof last_value === 'undefined') {
            node_rates[name] = value;
            config.cluster_status.rates[node_name] = node_rates;
            return 0
        } else {
            node_rates[name] = value;
            config.cluster_status.rates[node_name] = node_rates;
            return cap_zero(Math.round((value - last_value) / interval));
        }
    }

    function calc_cluster_view(myself, cluster_status, cluster_issues) {
        var ready = [myself];
        var not_ready = [];
        $.each(cluster_status, function(index, node) {
            $.each(node, function(name, running){
                if (running === true) {
                    if (myself !== name) {
                        ready.push(name);
                    }
                } else {
                    var msg = "<strong>"+name+"</strong> is not reachable."
                    cluster_issues.push({type: "danger", node: myself, message: msg});
                    not_ready.push(name);
                }
            });
        });
        return {ready: ready, not_ready: not_ready, num_nodes: ready.length + not_ready.length}
    }

    function listener_types(listeners) {
        var listener_types = {}
        $.each(listeners, function(i) {
            var listener = listeners[i];
            if (listener.type.startsWith("mqtt") && listener.status === "running") {
                listener_types[listener.type.toUpperCase()] = true;
            }
        });
        return Object.keys(listener_types);
    }

    function listener_check(myself, listeners, cluster_view, cluster_issues) {
        $.each(listeners, function(i) {
            var listener = listeners[i];
            if (listener.type.startsWith("mqtt") && listener.status !== "running") {
                var msg = "<strong>"+listener.type.toUpperCase()+"</strong> listener ("+listener.ip+":"+listener.port+") is <strong>"+listener.status+"</strong>";
                cluster_issues.push({type: "info", node: myself, message: msg});
            }
            if (cluster_view.num_nodes > 0) {
                if (listener.type.startsWith("vmq") && listener.status === "running" && listener.ip === "0.0.0.0") {
                    var msg = "Cluster communication is using the <strong>0.0.0.0</strong> interface.";
                    cluster_issues.push({type: "warning", node: myself, message: msg});

                }
            }
        })
    }

    function version_check(nodes, cluster_issues) {
        var versions = {};
        $.each(nodes, function(n) {
            var node = nodes[n];
            var nodes_with_version = versions[node.version]
            if (typeof nodes_with_version === 'undefined') {
                nodes_with_version = [];
            }
            nodes_with_version.push(node.node);
            versions[node.version] = nodes_with_version;
        });
        if (Object.keys(versions).length > 1) {
            var s = "";
            $.each(versions, function(version) {
                s += (version + " ")
            })
            var msg = "Different VerneMQ versions <strong>"+s+"</strong> running in the same cluster";
            cluster_issues.push({type: "warning", node: "all", message: msg});
        }
    }

    function cap_zero(val) {
        if (val < 0) {
            return 0;
        } else {
            return val;
        }
    }

    function calc_routing_score(node_name, rate_interval, local_matched, remote_matched) {
        var local_matched_rate = calc_rate(node_name, "local_matched", rate_interval, local_matched);
        var remote_matched_rate = calc_rate(node_name, "remote_matched", rate_interval, remote_matched);
        var all = local_matched_rate + remote_matched_rate;
        if (all > 0) {
            return "" + Math.floor(local_matched_rate * 100 / all) + " / " + Math.floor(remote_matched_rate * 100 / all);
        }
        return "0 / 0";
    }

    function cluster_status() {
        $.ajax({
            url: config.cluster_status.url,
            success: function(response) {
                var response_obj = response[0];
                var nodes = Object.keys(response_obj)
                var total = {active: true, clients_online: 0, clients_offline: 0, connect_rate: 0, msg_in_rate: 0,
                    msg_out_rate: 0, msg_drop_rate: 0, msg_queued: 0};
                var now = Date.now();
                var cluster_size = 0;
                var cluster_issues = [];
                nodes = $.map(nodes, function(node_name) {
                    var this_node = response_obj[node_name];
                    var rate_interval = (now - config.cluster_status.last_calculated) / 1000;
                    var connect_rate = calc_rate(node_name, "connect", rate_interval, this_node.num_online)
                    var msg_in_rate = calc_rate(node_name, "msg_in", rate_interval, this_node.msg_in)
                    var msg_out_rate = calc_rate(node_name, "msg_out", rate_interval, this_node.msg_out)
                    var msg_drop_rate = calc_rate(node_name, "queue_drop", rate_interval, this_node.msg_drop)
                    var cluster_view = calc_cluster_view(node_name, this_node.mystatus, cluster_issues);
                    var routing_score = calc_routing_score(node_name, rate_interval,
                                                           this_node.matches_local, this_node.matches_remote);
                    var node = {
                        node: node_name,
                        clients_online: this_node.num_online,
                        clients_offline: this_node.num_offline,
                        connect_rate: connect_rate,
                        msg_in_rate: msg_in_rate,
                        msg_out_rate: msg_out_rate,
                        msg_drop_rate: msg_drop_rate,
                        msg_queued: cap_zero(this_node.queue_in - (this_node.queue_out + this_node.queue_drop + this_node.queue_unhandled)),
                        subscriptions: this_node.num_subscriptions,
                        retained: this_node.num_retained,
                        cluster_view: cluster_view,
                        listeners: listener_types(this_node.listeners),
                        version: this_node.version,
                        routing_score: routing_score
                    };
                    listener_check(node_name, this_node.listeners, cluster_view, cluster_issues);
                    cluster_size = Math.max(cluster_size, cluster_view.num_nodes);
                    total.clients_online += node.clients_online;
                    total.clients_offline += node.clients_offline;
                    total.connect_rate += node.connect_rate;
                    total.msg_in_rate += node.msg_in_rate;
                    total.msg_out_rate += node.msg_out_rate;
                    total.msg_drop_rate += node.msg_drop_rate;
                    total.msg_queued += node.msg_queued;
                    return node;
                });
                version_check(nodes, cluster_issues);
                config.cluster_status.last_calculated = now;
                var output = Mustache.render(config.template, {nodes: nodes, cluster_size: cluster_size, cluster_issues: cluster_issues, total: total});

                config.target.html(output);
                setTimeout(function() {cluster_status();}, config.reload_interval);
            }
        });
    }

    Mustache.parse(config.template);

    cluster_status(node_list_template);
});


