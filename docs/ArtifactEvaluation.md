# Artifact Evaluation

Begin by connecting to the provided machine. Run

```bash
ssh <ASSIGNED_HOST>
```

Where `<ASSIGNED_HOST>` is the host name provided in the individual `~/.ssh/config` configs.

Once connected, clone the Artifact Evaluation repository and build the AE specific dockerfile:

```bash
git clone https://github.com/JRafaelSoares/ZipLogAE
cd ZipLogAE
docker build . -f ./DockerfileAE -t ziplog
```

This dockerfile contains all of Ziplog's different versions pre-compiled, with all essential setup complete.

Once built, start the container and enter it following these commands:

```bash
docker run --privileged --network host -d ziplog sleep infinity
docker exec -it $(docker ps -q -f ancestor=ziplog) bash
```

Once inside, you can run the prepared microbenchmarks to ensure functionality of each system.

> [NOTE]
> As all deployments are done inside a single machine, the obtained results simply depict the functioning of the system and not its evaluated performance.

## Ziplog RDMA

The first branch (`rdma`) contains the Ziplog implementation with our own RDMA library.

Begin by entering its directory:

```bash
cd /src/rdma
```

To test its functionality, we include two small microbenchmarks:
* **Append Benchmark** -- Deploys a `Zipper`, two `storage` replicas, one `proxy` through which the append benchmark is run, and a `subscriber module`.
* **Lock Service Benchmark** -- Substitutes the proxy and subscriber by the Lock Service implementation depicted in the paper.

> Note: Due to the cleanups at the end of each run. 
> This is normal behavior as long as the results of each benchmark are printed accordingly
### Append Benchmark

```bash
./scripts/run_append_benchmark.sh
```

Estimated running time: 20 seconds

You will see two lines for the performance of the append benchmark and the subscriber:

**Append:**

```
[INFO] [client_main] [1788973724101089] Final statistics: throughput: 201367 IOPS       median latency: 4 us    99% latency: 9 us       99.9% latency: 12 us.
```

**Subscriber:**

```
[INFO] [subscriber_main] [1788973725114592] Final statistics: throughput: 201501 IOPS   median latency: 5 us    99% latency: 9 us       99.9% latency: 14 us.
```

### Lock Service Benchmark

```bash
./scripts/run_lock_service.sh
```

Estimated running time: 20 seconds

Example Result, depicting the latency of acquiring and releasing the lock:

```
[INFO] [lock_main] [1788973415278985] Final statistics [acquire]: median latency: 49 us 99% latency: 51 us      99.9% latency: 56 us.
[INFO] [lock_main] [1788973415281622] Final statistics [release]: median latency: 49 us 99% latency: 51 us      99.9% latency: 54 us.
```

## ZipKVS

The ZipKVS implementation spans across two branches:
* `zipkvs` -- Holds the ZipKVS prototype.
* `retwis` -- Holds the Retwis benchmark client.

Begin by entering the `retwis` directory and running the local benchmark:


```bash
cd /src/retwis
./store/tools/run_local_experiment.sh
```

Estimated running time: 20 seconds

Results are written to `/tmp/client.0.log`, which prints the start and end time of each committed transaction.

Example Result:

```
...
69 1788973860.830127 1788973860.882127 52000 1
70 1788973860.882127 1788973860.970127 88000 1
# Commit_Ratio: 1.000000
# Overall_Latency: 116627.085714
```

## Ziplog eRPC

The `ziperpc` branch holds the Ziplog prototype built on top of the eRPC communication library, which was used to evaluate against LazyLog.

Begin by entering its directory and running the local benchmark:

```bash
cd /src/ziperpc
./scripts/experiments/local_pc.sh
```


**End-to-End Latency Benchmark:**

Estimated running time: 20 seconds

Example Result:

```
[consumer]: Throughput: 9980.65 msg/s
[consumer]: latency metrics
[consumer]: percentile latencies
        p50: 99631
        p95: 100791
        p99: 102781
        p99.9: 111661
[producer]: Throughput: 9808.36 msg/s
[producer]: latency metrics
[producer]: percentile latencies
        p50: 21763
        p95: 28218
        p99: 30280
        p99.9: 34638
```

## Ziplog TCP

Finally, the `tcp` branch holds the Ziplog TCP implementation which we use to compare against SpecLog.

Begin by entering its directory and running the local benchmark:

```bash
cd /src/tcp
./benchmark-local.sh
```

Estimated running time: 2 seconds

Example Result:

```
[*] latency stats (µs):
count: 10000
min:   63
p50:   157
p99:   327
max:   841
mean:  172
```
