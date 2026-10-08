# wakuwaku-iggy

Typed event publishing and consuming on [Apache Iggy](https://iggy.apache.org),
with per-key ordering and algebraic batching.

An Iggy partition is totally ordered, but business logic usually only needs the
events of one key (a user, an order, …) in order. Each event type declares:

- its **partition key**, which picks the Iggy topic and partition;
- its **ordering** (`Relaxed`, `Acquire`, `Release`, `AcqRel`) relative to the
  key's other events;
- its **algebra** (`NonAssociative`, `Commutative`, `Idempotent`,
  `IdempotentCommutative`, `Associative`), which says how a run of its events
  can be collapsed or folded.

The consumer uses this to process each polled batch per key and concurrently,
collapse or fold runs before calling handlers, and isolate failing keys in a
retry topic without blocking the others.

## Usage

```toml
[dependencies]
wakuwaku-iggy = "0.1"
```

1. Implement `PartitionKey` for the key type and `Event` for each event type.
2. Publish with `Publisher::publish`.
3. Implement `EventHandler` for each event type, register the handlers with
   `IggyConsumerRegisterCenter`, and `start` it with a `ConsumerConfig`.

See the crate documentation for the full model and an end-to-end example.

## License

MIT
