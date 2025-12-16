stats

config

better error handling(return loud errors when failing to publish)

multiple brokers on same storage tests (must fail)

shutdown stops publishing immediately, tries to drain inflight, ensure no late ack flushes race, ensure batchers drain once

define a formal delivery state machine (very useful later)?

Persist a delivery cursor
Instead of inferring:
store next_offset durably per (topic, group)
advance it only after mark_inflight_batch succeeds
on restart, read it directly

tracing (no more prints..)

auth

clusters (leader through shared networked storage initially, raft replication later?)

admin/dashboard

cli

CLIENT (more than library, to play with)

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