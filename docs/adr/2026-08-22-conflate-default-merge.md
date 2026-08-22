# Default `conflate`'s merge to latest-wins, set it with `WithDefaultMerge`

- **Status:** accepted, implemented
- **Date:** 2026-08-22
- **Scope:** `github.com/amorey/gobus`, package `conflate`
- **Breaking:** yes. `conflate.New` changes shape; every call site edits.

## Context

`conflate.New` took the hub's `Merge` as a required positional argument and
panicked on nil, on the reasoning that coalescing policy is the point of the bus
and so must never be implicit. The cost lands on the common case. Latest-value
fan-out — the newer value supersedes the undelivered one, the slot survives — is
what most consumers of this bus want, and stating it took a literal at every
construction site:

```go
hub := conflate.New[string](func(_, next Update) (Update, bool) { return next, true })
```

That is four lines of type noise around one word of meaning, repeated per hub,
and it is the shape both examples and most of the test suite were carrying.

The sister package already answered the same question the other way.
`watch.New` takes `Option[V]`s and treats an omitted `WithAccept` as
last-writer-wins, on the reasoning that a state bus has a meaningful identity
rule so omitting the option is a statement rather than an oversight. The two
packages disagreed on a point where they did not have to.

## Decision

```go
func New[K comparable, V any](opts ...Option[V]) *Hub[K, V]
func WithDefaultMerge[V any](merge Merge[V]) Option[V]
```

With no options the hub installs `latest`, an unexported `Merge` returning
`(next, true)`. `New` panics on a nil `Option`; `WithDefaultMerge` panics on a
nil `Merge`. The per-receiver `Hub.WithMerge` is unchanged and still overrides
the hub's policy for one handle.

`New` resolves the default into `shared.merge` rather than leaving the field
nil and branching at enqueue time, so the hub's merge stays non-nil by
construction and `enqueueLocked`'s existing "receiver's merge, else the hub's"
fallback needs no second nil check.

`WithDefaultMerge` is package-level, unlike the per-receiver options, because
there is no hub yet when it is built — the same forcing that makes
`watch.WithAccept` package-level. It carries `V` alone, so a call site spells
only `K`. An option depending on `K` would force both type arguments at every
call site and should not be added without meaning to.

### The name

`WithMerge` was unavailable: it is the per-receiver option (`Hub.WithMerge`),
whose blast radius is one handle rather than the whole bus. Two options reading
alike at a call site would hide exactly that difference, and shadowing a method
name with a package-level function makes a review diff ambiguous about which
one is meant. Renaming the method to `WithReceiverMerge` and taking `WithMerge`
for the hub was considered and rejected: it renames existing exported surface to
solve a problem that a more precise name on the *new* symbol solves for free.
`Default` is accurate — the value is the fallback a receiver resolves to when it
has none of its own, which is precisely `enqueueLocked`'s behavior.

## Consequences

The common construction becomes `conflate.New[string, Update]()`. It costs one
more type argument than the strict form did — `V` used to be inferable from the
merge argument and now comes from nowhere — and drops the literal entirely. The
net is shorter and quieter at the call site.

The policy-is-explicit convention now reads the same in both packages: omitting
the option is a statement of the default, supplying a nil one is a mistake and
panics. What is genuinely given up is the compiler forcing a decision. A caller
who has not thought about coalescing now gets latest-wins silently, and
latest-wins *discards an event that was never delivered*. That is a real hazard
for a consumer that needed annihilation or accumulation, and it is not
detectable by the bus. It is mitigated only by documentation: the package doc,
the README and `New` itself all lead with what the default does.

Every `conflate.New` call site edits. There is no deprecation window — the
module is pre-1.0 and the old form does not compile against the new signature,
so the break is loud rather than silent.

`Merge` remains required in spirit for the buses that need it: a hub that
combines values or annihilates them still supplies one, and nothing about
`Hub.WithMerge` or the per-receiver override changes.
