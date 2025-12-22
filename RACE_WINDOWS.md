(TODO: Deprecated, Review again)

# ## Known race windows

What remains are **subtle but real**.

---

### 🚨 Race 1: Duplicate redelivery under load (you just fixed this)

Root cause (now fixed):

* expired inflight cleared
* redelivery enqueued
* delivery loop also fetched it again

Fix you applied:

* don’t reinsert offsets ≥ next_offset
* avoid touching next_offset on redelivery

✅ Correct.

---

### ⚠️ Race 2: Ack vs redelivery worker

Timeline:

```
T1: redelivery_worker lists expired offset X
T2: consumer ACKs X
T3: worker clears inflight and queues redelivery
```

Result:

* message X gets redelivered even though it was acked

**Why it still can happen**

* `list_expired()` is a snapshot
* ack and expiry are not atomic

**How production brokers solve this**

* inflight record contains a generation / delivery_id
* ack only succeeds if generation matches
* redelivery increments generation

**Acceptable shortcut (for now)**
Before pushing to redelivery queue:

```rust
if storage.is_acked(topic, group, offset) {
    continue;
}
```

This makes redelivery *idempotent*.

(formalize: "Each offset is delivered at least once per group and at most once per delivery generation.")

---

### ⚠️ Race 3: Restart + in-memory next_offset

You already added:

```rust
next_offset = lowest_unacked_offset()
```

That was **essential** and correct.

Remaining caveat:

* if inflight entries exist at restart
* they must be treated as expired or ignored

Right now:

* they *will* expire via TTL
* that’s acceptable behavior

Document this as:

> After restart, in-flight messages may be redelivered.

That’s industry standard.

---

### ⚠️ Race 4: Multiple consumers joining simultaneously

You do:

```rust
delivery_task_started.swap(true)
```

This is correct.

But:

* two consumers can register
* one delivery loop snapshot may miss the other briefly

This causes:

* temporary imbalance
* **not correctness issues**

This is fine.
