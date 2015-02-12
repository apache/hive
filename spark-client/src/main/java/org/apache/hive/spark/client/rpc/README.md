Spark Client RPC
================

Basic flow of events:

- Client side creates an RPC server
- Client side spawns RemoteDriver, which manages the SparkContext, and provides a secret
- Client side sets up a timer to wait for RemoteDriver to connect back
- RemoteDriver connects back to client, SASL handshake ensues
- Connection is established and now there's a session between the client and the driver.

Features of the RPC layer:

- All messages serialized via Kryo
- All messages are replied to. It's either an empty "ack" or an actual response - that depends
  on the message.
- RPC send API is asynchronous - callers get a future that can be used to wait for the message.
- Currently, no connection retry. If connection goes down, both sides tear down the session.

Notes:

- Because serialization is using Kryo, types need explicit empty constructors or things will
  fail to deserialize. This can be seen in the way exceptions are propagated - the throwing
  side sends just a string stack trace to the remote, because certain fields on exceptions
  don't have empty constructors.
- The above is especially important because at the moment there's no way to register custom
  serializers in the RPC library.

Future work:

- Random initial RPC id + id wrapping.
- SSL / security in general.
- Remove "Serializable" from the API. Not needed with kryo.
