# Future-Bus



A bus type (SPMC) channel where multiple consumers can subscribe to single source channel.
The bus can internally use any channel/receiver combination. By default it provides constructor
methods to use the `futures::channel::mpsc` channels.