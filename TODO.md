stats

config

ack correct matching(sub id?), in tcp layer

partitions nuke or adjust

proper connection handling(heartbeat, ping pong, early dc detection)

experiment with spreading delivered messages by making some consumers slower and see what happens

better handling of batching slowdown when confirms not drained

better error handling(return loud errors when failing to publish)

diagnostics queue/channel to allow sending up non fatal issues and tallying/observing them better?

multiple brokers on same storage tests (must fail)

shutdown stops publishing immediately, tries to drain inflight, ensure no late ack flushes race, ensure batchers drain once

define a formal delivery state machine (very useful later)?

Persist a delivery cursor
Instead of inferring:
store next_offset durably per (topic, group)
advance it only after mark_inflight_batch succeeds
on restart, read it directly

clusters (leader through shared networked storage initially, raft replication later?)

admin/dashboard

cli

confirm stream(option, config) and test

metadata(content type, redelivered, etc)

RabidMQ easter egg(--version during April 1st? rabbitmq compatibility layer?)

fast consumer crash detection?(might need keeping track of un-acked messages)

routing layer? (in order to keep invariants stable, maybe we'd need a separate layer, broker delivers to itself, runs script, acks onces derived message completes, effectively)

dead letter queue? (expired, nacked too much, etc) (part of routing?)

nack/reject

queues with ttl to discard(not just resend)

define ops/infra story for easy convenient deployments and handling(not error prone by default, don't assume user does things right)

write an operator runbook