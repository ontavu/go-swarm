# go-swarm

Groups of goroutines that share a cancel and are waited on together. The standard library
only, no dependency.

```go
import "github.com/ontavu/go-swarm"
```

The package is `swarm`. A group holds the cancel and the `sync.WaitGroup` of its members,
and counts those still running. It comes in two kinds:

| Group | Made by | The members' context is cancelled when |
|---|---|---|
| Swarm | `swarm.New(ctx)` | the group is cancelled, or `ctx` is |
| Ensemble | `swarm.NewEnsemble(ctx)` | any member returns, the group is cancelled, or `ctx` is |

```go
s := swarm.New(ctx)
for _, cam := range cams {
	s.Run(cam.Run) // counted before Run returns: s.Count() never misses it
}

// At shutdown, whoever cancels also waits.
s.Cancel()
s.Wait()
```

`swarm.Run(ctx, f, g, ...)` and `swarm.EnsembleRun(ctx, f, g, ...)` run their callbacks in
a fresh group and return when every callback has returned. `swarm.Run` does not cancel
them: a member that never looks at its context keeps it from returning. `EnsembleRun` is
the one for a server and its watchdog, where one returning has to release the other.

A member is a `func(context.Context)` and must return once its context is done. The group
cannot stop a goroutine that does not.

## Used by

[Cams](https://github.com/ontavu/cams): its LAN agent,
[cams-agent](https://github.com/ontavu/cams-agent), and its cloud hub.

## Licence

[MPL-2.0](LICENSE). The authors are listed in [AUTHORS](AUTHORS).
