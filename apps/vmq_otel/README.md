## VerneMQ Opentelemetry Plugin (vmq_otel)

This does nothing currently. It's a stub for an Opentelemetry plugin.


It adds the Opentelemetry API to the Verne release. The plugin itself starts and configures the Opentelemetry application. In addition, the plugin allows the configuration of the Opentelemetry Exporter.

Configuration example in the `vernemq.conf` file:

```
plugins.vmq_otel = on
vmq_otel.telemetry.span_processor = batch
vmq_otel.telemetry.traces_exporter = otel_exporter_stdout
vmq_otel.telemetry.traces_exporter.options = [{a, b}]
vmq_otel.exporter.protocol = http_protobuf
vmq_otel.exporter.endpoint = http://localhost:4318

```
Note that we could add custom tracers and exporters to the plugin (`vmq_otel_tracer` and `vmq_otel_exporter`). The plugin works with the automatically configured default tracer currently.

You can configure a traces exporter and a span_processor. With `otel_exporter_stdout` traces will just end up in the Verne console, so this is for development only.
We likely need to add more Opentelemetry and Exporter settings to the `vmq_otel` schema.
To make this do anything, we need to add spans to specific code points in VerneMQ itself.

Those spans would be no_ops as long as `vmq_otel` is not started. When we start `vmq_otel`, Verne will start creating spans.
I don't have a concept so far to enable distributed tracing (catching external span ids).

Example of adding trace points to the code
Add the `otel_tracer` lib to the module:

`-include_lib("opentelemetry_api/include/otel_tracer.hrl").`

Let's say we would like to trace the `incr_queue_in` counter in `vmq_queue`. We'd wrap the function call in a `?with_span`

```
online({enqueue, Msg}, State) ->
    ?with_span(<<"vmq_queue:queue_in">>, #{}, fun(_SpanCtx) ->
        _ = vmq_metrics:incr_queue_in()
    end),
    {next_state, online, insert(Msg, State)};
```

This would then export a span to the VerneMQ console as soon as we send a message to the broker (and have a matching consumer).
