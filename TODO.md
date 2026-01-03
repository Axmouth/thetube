deny topic etc names beyond simple fs compatible setups

test big payloads

TODO:
Broker should no longer:
loop compute_start_offset() by calling is_inflight_or_acked() repeatedly.
Instead:
start = stroma.next_deliverable(tp,g, current_cursor, upper)
Also: redelivery queue should remain bounded by inflight cap (it mostly is already), but don’t let it accumulate unbounded offsets from repeated failures—Stroma can own "expired offsets" listing.

Redelivery not counted as inflight(not added to set)? Consider how to handle

Revisit batch size/timeout dynamic rampups based on load (Revisit PID controller setup?)

refactor batching to not be ad hoc but separate layer with centralized reusable logic

play with having code return dummy responses and see max broker throughput and bottlenecks

ui login

reorganize for structs/models in a common crate, to avoid circles between metrics and storage too

more cleanup tests

cleanup leftover inflight without message (or better figure why it happens)

Make sure cleanup reclaims space(seems not to?)

config

Handshake two way

ack correct matching(sub id?), in tcp layer

partitions nuke or adjust

max unconfirmed per publisher

slow ingress on memory/storage pressure

better enforced write guarantees(getting a confirm, etc, means already on disk) (enqueue confirms, send on batch write?)

unwrap/expect cleanup

proper connection handling(heartbeat, ping pong, early dc detection)

experiment with spreading delivered messages by making some consumers slower and see what happens

better handling of batching slowdown when confirms not drained

better error handling(return loud errors when failing to publish)

diagnostics queue/channel to allow sending up non fatal issues and tallying/observing them better?

multiple brokers on same storage tests (must fail)

shutdown stops publishing immediately, tries to drain inflight, ensure no late ack flushes race, ensure batchers drain once

define a formal delivery state machine (very useful later)?

Eval persisting inflight map, or delivery tags for inflight, so inflight state can be recovered on startup after crash

reeval:
Persist a delivery cursor
Instead of inferring:
store next_offset durably per (topic, group)
advance it only after mark_inflight_batch succeeds
on restart, read it directly

clusters (leader through shared networked storage initially, raft replication later?)

cli

confirm stream(option, config) and test

metadata(content type, redelivered, etc)

RabidMQ easter egg(--version during April 1st? rabbitmq compatibility layer?)

routing layer? (in order to keep invariants stable, maybe we'd need a separate layer, broker delivers to itself, runs script, acks onces derived message completes, effectively)

dead letter queue? (expired, nacked too much, etc) (part of routing?)

nack/reject

queues with ttl to discard(not just resend)

define ops/infra story for easy convenient deployments and handling(not error prone by default, don't assume user does things right)

write an operator runbook

rabbitmq compatible endpoint?

replace epoch in delivery tag with gen and seq. Seq is simple monotonic counter, gen is increased per instance(process? task?) created

dls or embedded language to script transformations, routing, etc?

recovered_cursors, topics, etc, cleanup after inactivity? (when empty, occasional cleanup)