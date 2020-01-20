# Future-Bus

*Note: As there are now equivalent implementations for this type of channel in tokio and async-std, I would recommend using those over this repository, as they are better maintained and probably faster.*

A bus type (SPMC) channel where multiple consumers can subscribe to single source channel.
The bus can internally use any channel/receiver combination. By default it provides constructor
methods to use the `futures::channel::mpsc` channels.
