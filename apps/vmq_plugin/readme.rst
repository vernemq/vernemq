vmq_plugin
==========

.. image:: https://travis-ci.org/erlio/vmq_plugin.svg
    :target: https://travis-ci.org/erlio/vmq_plugin

vmq_plugin is a simple plugin framework for Erlang OTP projects.

A plugin takes the form of an Erlang OTP application or just a plain MFA.

How does it work
----------------

Your application uses one or more of the following hook function calls:

.. code-block:: erlang
    
    %% iterates over all Plugins serving the Hook 'HookName'
    vmq_plugin:all(HookName :: atom(), HookArgs :: [any()]).
    %% iterates over all Plugins serving the Hook 'HookName' until one returns 'ok' | {'ok', any()}
    vmq_plugin:all_till_ok(HookName :: atom(), HookArgs :: [any()]).
    %% calls the top priority Plugin that serves the Hook 'HookName'
    vmq_plugin:only(HookName:: atom(), HookArgs :: [any()]).

Your plugin has to announce at the ``vmq_plugin_mgr`` that is is serving a hook. If your plugin is packaged as an OTP application it is enough to put ``{vmq_plugin_hooks, Hooks}`` into your plugin application environment (``<plugin>.app.src``) and call ``vmq_plugin_mgr:enable_plugin(PluginApp)``.
``Hooks`` is a list containing either ``{Module, Function, Arity}`` or ``{HookName, Module, Function, Arity}``. In the first case the ``HookName`` defaults to the ``Function`` name.
In case your plugin is as simple as a single module, a call to ``vmq_plugin_mgr:enable_module_plugin(Module, Function, Arity)`` or ``vmq_plugin_mgr:enable_module_plugin(HookName, Module, Function, Arity)`` will register the plugin.

When registering a plugin as above a background process reprogrammes, recompiles, and reloads the ``vmq_plugin`` module. Moreover the whole configuration is written to a specifig config file, that can be inspected and modified if needed. It defaults to ``./vmq_plugin.conf`` and can be configured using the ``plugin_config`` environment variable.

During plugin registration the ``vmq_plugin_mgr`` searches the code path for the plugin. If the beam files are not yet part of the code path, ``vmq_plugin_mgr:enable_plugin(PluginApp, Path)`` could be used. The default path can be changed using the ``plugin_dir`` environment variable.

Plugins packaged as OTP application as well as their ``included_applications`` are automatically started when enabled through ``application:ensure_all_started(PluginApp)``. If this behaviour isn't desired a custom start function can be provided at ``PluginApp:start/0``.

When disabling a plugin through ``vmq_plugin_mgr:disable_plugin(PluginApp)`` the OTP application is stopped via ``application:stop(PluginApp)``. This behaviour can be changed via a custom ``PluginApp:stop/0`` function. All modules belonging to the application will be unloaded.


Advanced
--------

To ensure that the plugin applications are started after the main application (the one exposing the hooks) is started, you can define the ``{wait_for_proc, RegisteredProcess :: atom()}`` environment variable. This ensures that the initialization process gets deferred until the ``RegisteredProcess`` is registered.
