# VerneMQ MQTTv5 preview

[![Build Status](https://travis-ci.org/erlio/vernemq.svg?branch=mqtt5-preview)](https://travis-ci.org/erlio/vernemq)
[![Slack Invite](https://slack-invite.vernemq.com/badge.svg)](https://slack-invite.vernemq.com)


This branch is an early release of VerneMQ with MQTTv5 support and is made
public to allow the community and industry to start working with MQTTv5 and to
get as much feedback as possible.

When more complete and stable this branch will be merged into the VerneMQ master
branch and be generally available in the VerneMQ releases. MQTTv5 support will
be marked as in beta until we're sure it's ready for prime time.

Note: this branch will be rebased occasionally to keep up with the master branch
and due to refactorings.

## Current status

As this is a work in progress, not everything is implemented and there are a
number of known issues. If you'd like to contribute we'd love your pull requests
or feedback!

Currently supported features are:

- All MQTTv5 frames are implemented.
- MQTTv5 enhanced authentication and reauthentication.
- Publishes with payload format indicator, response topic, correlation data,
  user properties and content type.
- Message expiration.
- Delayed last will and testament.
- Retained messages.
- Shared subscriptions.
- Session expiration interval.
- Request/response using 'response topic' and 'correlation data' properties.
- Client to broker topic aliases.
- MQTTv5 and older prototocols can be enabled at the same time (set
  `allowed_protocol_versions=3,4,5` on the listener to enable respectively MQTT
  v3.1, 3.1.1 and 5.0).

Currently known issues for MQTTv5 are:

- Tracing (`vmq-admin trace`) doesn't yet support tracing MQTTv5 sessions.
- New subscription flags (No Local, Retain as Published, Retain Handling) are
  ignored and subscriptions therefore currently work as in MQTTv4.
- The plugin hooks for MQTTv5 sessions doesn't yet handle the new MQTTv5
  features.
- Broker to client topic aliases are not yet implemented.
- Subscriber IDs are not yet implemented.
- Bridge plugin does not yet support MQTTv5.
- Receive maximum flow control has not yet been implemented.
- Server and client receive maximum is not yet implemented.
- Server and client maximum packet size enforcement is not yet implemented.

Current limitations/open questions:

- Currently when using `allow_multiple_sessions=on`:
  - If using delayed last will and testament it is only sent for the last
    connected client.
  - The session expiration used is the one from the last connected client *or*
    the last client which send a new session expiration value when
    disconnecting.
- If using `allow_multiple_sessions=on`, should it be allowed to mix MQTTv5 and
  MQTTv4 sessions? What are the pros and cons?

Unknown issues:

We are sure there are many. Please play with this and let us know if something
doesn't look right! We'd love to get your feedback!

## Trying it out

On this branch MQTTv5 is enabled by default and all MQTTv5 actions are allowed
as a plugin called `vmq_allow_all_v5` is enabled which implements the
`auth_on_register_v1`, `auth_on_subscribe_v1` and `auth_on_publish_v1`, all of
them simply return `ok` allowing the actions.

So to get started, simply create a clone of this repository and switch to this
branch and then build VerneMQ using `make rel` as usual. VerneMQ can then be
started by running `_build/default/rel/vernemq/bin/vernemq console` and you can
now connect MQTTv5 clients to the listener running on `localhost:1883`.

##  Reporting issues

As this is a work in progress we really would appreciate your feedback! To
report an issue or a question, please open an issue on github and prefix the
issue title with `MQTTv5:`. You're also more than welcome to reach out to us on
[slack](https://slack-invite.vernemq.com) or get in touch with us via our
[contact form](https://vernemq.com/services.html).

## MQTTv5 plugins vs MQTTv4 plugins

MQTTv5 plugins use a different set of hooks as MQTTv5 has more features than
MQTTv4. The hooks in MQTTv5 has the same names as in MQTTv4, but are post-fixed
with `_v1` (hook version 1), so for example in MQTTv5 `auth_on_register_v1`
corresponds to the MQTTv4 hook `auth_on_register`. MQTTv5 also introduces a new
`on_auth_v1` hook to support enhanced (re-)authentication.
