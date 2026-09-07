# process_logs.py is a module and library to process the results of a TAPIR
# benchmark. The script takes in the log produced by a TAPIR client (or the
# concatenation of many of these logs) and outputs a JSON object that
# summarizes the results of the benchmark (e.g., average latency, average
# throughput).

from multiprocessing import Process, Pipe
import math
import time

import argparse
import collections
import json

LogEntry = collections.namedtuple('LogEntry', [
    'txn_id',
    'start_time_sec',
    'end_time_sec',
    'latency_micros',
    'status',
    'txn_type',
    'extra',
])

BenchmarkResult = collections.namedtuple('BenchmarkResult', [
    # Counts.
    'num_transactions',
    'num_successful_transactions',
    'num_failed_transactions',
    'num_failed_write_transactions',
    'num_failed_readonly_transactions',
    'abort_rate',
    'write_txn_abort_rate',
    'readonly_txn_abort_rate',

    # Throughputs.
    'throughput_all',
    'throughput_success',
    'throughput_failure',

    # Latencies.
    'average_latency_all',
    'median_latency_all',
    'p99_latency_all',
    'p999_latency_all',
    'average_latency_success',
    'median_latency_success',
    'p99_latency_success',
    'p999_latency_success',
    'average_latency_failure',
    'median_latency_failure',
    'p99_latency_failure',
    'p999_latency_failure',
    'follow_txn_avg_latency_success',
    'tweet_txn_avg_latency_success',

    # Extra.
    'extra_all',
    'extra_success',
    'extra_failure',
])


def mean(xs):
    if len(xs) == 0:
        return 0
    else:
        return float(sum(xs)) / len(xs)


def median(xs):
    if len(xs) == 0:
        return 0
    else:
        return xs[int(len(xs) / 2)]


def p99(xs):
    if len(xs) == 0:
        return 0
    else:
        return xs[int(99 * float(len(xs) / 100))]


def p999(xs):
    if len(xs) == 0:
        return 0
    else:
        return xs[int(999 * float(len(xs) / 1000))]


def process_IA_microbench_logs(client_log_filename):
    """
    One line in the log:
        [INFO] [client_main] [1697835459009552] Final statistics: throughput: 515481 IOPS       median latency: 7 us    99% latency: 15 us      99.9% latency: 20 us mean: 9.13735.
    """
    tpts = []
    p50s = []
    p99s = []
    p999s = []
    means = []
    with open(client_log_filename, 'r') as f:
        for line in f:
            # -2 to remove the ending period
            l = line[:-2].strip().split()
            assert l[5] == "throughput:"
            tpts.append(int(l[6]))
            assert (l[8] == "median" and l[9] == "latency:")
            p50s.append(int(l[10]))
            assert (l[12] == "99%" and l[13] == "latency:")
            p99s.append(int(l[14]))
            assert (l[16] == "99.9%" and l[17] == "latency:")
            p999s.append(int(l[18]))
            assert (l[20] == "mean:")
            means.append(float(l[21]))

    return BenchmarkResult(
        num_transactions = 0,
        num_successful_transactions = 0,
        num_failed_transactions = 0,
        abort_rate = 0,

        throughput_all = sum(tpts),
        throughput_success = 0,
        throughput_failure = 0,

        average_latency_all = mean(means),
        median_latency_all = mean(p50s),
        p99_latency_all = mean(p99s),
        p999_latency_all = mean(p999s),
        average_latency_success = 0,
        median_latency_success = 0,
        p99_latency_success = 0,
        p999_latency_success = 0,
        average_latency_failure = 0,
        median_latency_failure = 0,
        p99_latency_failure = 0,
        p999_latency_failure = 0,
        follow_txn_avg_latency_success = 0,
        tweet_txn_avg_latency_success = 0,

        extra_all = 0,
        extra_success = 0,
        extra_failure = 0,
    )

kSuccTxnStatus = 1
kAbortWriteTxnStatus = 2
kAbortReadonlyTxnStatus = 3

def process_log(lines, conn):
    all_lats = []
    succ_lats = []
    fail_lats = []
    fail_write_txn_lats = []
    fail_read_txn_lats = []

    for line in lines:
        parts = line.strip().split()
        assert len(parts) == 5 or len(parts) == 7, parts

        if len(parts) == 7:
            txn_type = int(parts[5])
            extra = int(parts[6])
        else:
            txn_type = -1
            extra = 0

        lat = int(parts[3])
        all_lats.append(lat)
        status = int(parts[4])
        if status == kSuccTxnStatus:
            succ_lats.append(lat)
        else:
            fail_lats.append(lat)
            if status == kAbortWriteTxnStatus:
                fail_write_txn_lats.append(lat)
            else:
                fail_read_txn_lats.append(lat)
                
    conn.send([all_lats, succ_lats, fail_lats, fail_write_txn_lats, fail_read_txn_lats])

def process_client_logs_parallel(client_log_filename, warmup_sec, duration_sec):
    """Processes a concatenation of client logs.

    process_client_logs takes in a file, client_log, of client log entries that
    look something like this:

        1 1540674576.757905 1540674576.758526 621 1
        2 1540674576.758569 1540674576.759168 599 1
        3 1540674576.759174 1540674576.759846 672 1
        4 1540674576.759851 1540674576.760529 678 1

    or like this:

        4 1540674576.759851 1540674576.760529 678 1 2 4

    where
        - the first column is incremented per client,
        - the second column is the start time of the txn (in seconds),
        - the third column is the end time of the txn (in seconds),
        - the fourth column is the latency of the transaction in microseconds,
        - the fifth column is 1 if the txn was successful and 2 if write-txn abort, and 3 if readonly-txn abort.
        - the sixth column is the transaction type,
        - the seventh column is the number of extra retries.

    process_client_logs outputs a summary of the results. The first `warmup`
    seconds of data is ignored, and the next `duration` seconds is analyzed.
    All data after this duration is ignored.
    """
    # Extract the log entries, ignoring comments and empty lines.
    log_entries = []
    lines = []
    with open(client_log_filename, 'r') as f:
        for line in f:
            if line.startswith('#') or line.strip() == "":
                continue
            lines.append(line)

    # Create split for each threads
    kNumThreads = 8
    offset = math.ceil(len(lines) / kNumThreads)
    left = 0
    line_split = []
    for i in range(kNumThreads):
        ll = None
        if left + offset > len(lines):
            ll = lines[left : ]
        else:
            ll = lines[left : left + offset]
        left += offset
        line_split.append(ll)

    # Spawn processes
    processes = []
    entries = [[]] * kNumThreads
    rets = []
    for i in range(kNumThreads):
        parent_conn, child_conn = Pipe()
        p = Process(target=process_log, args=(line_split[i], child_conn))
        p.start()
        processes.append([p, parent_conn])

    for p in processes:
        rets.append(p[1].recv())
        p[0].join()


    all_latencies = []
    success_latencies = []
    failure_latencies = []
    failure_write_txn_latencies = []
    failure_readonly_txn_latencies = []
    follow_txn_success_latencies = []
    tweet_txn_success_latencies = []

    all_num_extra = 0.0
    success_num_extra = 0.0
    failure_num_extra = 0.0

    # Aggregate latency results
    for ret in rets:
        all_latencies += ret[0]
        success_latencies += ret[1]
        failure_latencies += ret[2]
        failure_write_txn_latencies += ret[3]
        failure_readonly_txn_latencies += ret[4]

    if len(all_latencies) == 0:
        raise ValueError("Zero completed transactions.")

    all_latencies.sort()
    success_latencies.sort()
    failure_latencies.sort()
    failure_write_txn_latencies.sort()
    failure_readonly_txn_latencies.sort()

    num_transactions = len(all_latencies)
    num_successful_transactions = len(success_latencies)
    num_failed_transactions = len(failure_latencies)
    num_failed_write_transactions = len(failure_write_txn_latencies)
    num_failed_readonly_transactions = len(failure_readonly_txn_latencies)

    return BenchmarkResult(
        num_transactions = num_transactions,
        num_successful_transactions = num_successful_transactions,
        num_failed_transactions = num_failed_transactions,
        num_failed_write_transactions = num_failed_write_transactions,
        num_failed_readonly_transactions = num_failed_readonly_transactions,
        abort_rate = float(num_failed_transactions) / num_transactions,
        write_txn_abort_rate = float(num_failed_write_transactions) / num_transactions,
        readonly_txn_abort_rate = float(num_failed_readonly_transactions) / num_transactions,

        throughput_all = float(num_transactions) / duration_sec,
        throughput_success = float(num_successful_transactions) / duration_sec,
        throughput_failure = float(num_failed_transactions) / duration_sec,

        average_latency_all = mean(all_latencies),
        median_latency_all = median(all_latencies),
        p99_latency_all = p99(all_latencies),
        p999_latency_all = p999(all_latencies),
        average_latency_success = mean(success_latencies),
        median_latency_success = median(success_latencies),
        p99_latency_success = p99(success_latencies),
        p999_latency_success = p999(success_latencies),
        average_latency_failure = mean(failure_latencies),
        median_latency_failure = median(failure_latencies),
        p99_latency_failure = p99(failure_latencies),
        p999_latency_failure = p999(failure_latencies),
        follow_txn_avg_latency_success = mean(follow_txn_success_latencies),
        tweet_txn_avg_latency_success = mean(tweet_txn_success_latencies),

        extra_all = all_num_extra,
        extra_success = success_num_extra,
        extra_failure = failure_num_extra,
    )

def process_client_logs(client_log_filename, warmup_sec, duration_sec):
    """Processes a concatenation of client logs.

    process_client_logs takes in a file, client_log, of client log entries that
    look something like this:

        1 1540674576.757905 1540674576.758526 621 1
        2 1540674576.758569 1540674576.759168 599 1
        3 1540674576.759174 1540674576.759846 672 1
        4 1540674576.759851 1540674576.760529 678 1

    or like this:

        4 1540674576.759851 1540674576.760529 678 1 2 4

    where
        - the first column is incremented per client,
        - the second column is the start time of the txn (in seconds),
        - the third column is the end time of the txn (in seconds),
        - the fourth column is the latency of the transaction in microseconds,
        - the fifth column is 1 if the txn was successful and 2 if write-txn abort, and 3 if readonly-txn abort.
        - the sixth column is the transaction type,
        - the seventh column is the number of extra retries.

    process_client_logs outputs a summary of the results. The first `warmup`
    seconds of data is ignored, and the next `duration` seconds is analyzed.
    All data after this duration is ignored.
    """
    # Extract the log entries, ignoring comments and empty lines.
    log_entries = []
    with open(client_log_filename, 'r') as f:
        for line in f:
            if line.startswith('#') or line.strip() == "":
                continue

            parts = line.strip().split()
            assert len(parts) == 5 or len(parts) == 7, parts

            if len(parts) == 7:
                txn_type = int(parts[5])
                extra = int(parts[6])
            else:
                txn_type = -1
                extra = 0

            log_entries.append(LogEntry(
                txn_id=int(parts[0]),
                start_time_sec=float(parts[1]),
                end_time_sec=float(parts[2]),
                latency_micros=int(parts[3]),
                status=bool(int(parts[4])),
                txn_type=txn_type,
                extra=extra,
            ))

    if len(log_entries) == 0:
        raise ValueError("Zero transactions logged.")

    # Process the log entries.
    #log_entries.sort(key=lambda x: x.end_time_sec)
    #start_time_sec = log_entries[0].end_time_sec + warmup_sec
    #end_time_sec = start_time_sec + duration_sec

    all_latencies = []
    success_latencies = []
    failure_latencies = []
    failure_writetxn_latencies = []
    failure_readonlytxn_latencies = []
    follow_txn_success_latencies = []
    tweet_txn_success_latencies = []

    all_num_extra = 0.0
    success_num_extra = 0.0
    failure_num_extra = 0.0

    for entry in log_entries:
        #if entry.end_time_sec < start_time_sec:
        #    continue

        #if entry.end_time_sec > end_time_sec:
        #    break

        all_latencies.append(entry.latency_micros)
        all_num_extra += entry.extra

        if entry.status == kSuccTxnStatus:
            success_latencies.append(entry.latency_micros)
            success_num_extra += entry.extra
            if entry.txn_type == 2:
                follow_txn_success_latencies.append(entry.latency_micros)
            if entry.txn_type == 3:
                tweet_txn_success_latencies.append(entry.latency_micros)
        else:
            failure_latencies.append(entry.latency_micros)
            failure_num_extra += entry.extra
            if entry.status == kAbortWriteTxnStatus:
                failure_writetxn_latencies.append(entry.latency_micros)
            else:
                failure_readonlytxn_latencies.append(entry.latency_micros)

    if len(all_latencies) == 0:
        raise ValueError("Zero completed transactions.")

    all_latencies.sort()
    success_latencies.sort()
    failure_latencies.sort()
    failure_writetxn_latencies.sort()
    failure_readonlytxn_latencies.sort()

    num_transactions = len(all_latencies)
    num_successful_transactions = len(success_latencies)
    num_failed_transactions = len(failure_latencies)
    num_failed_write_transactions = len(failure_writetxn_latencies)
    num_failed_readonly_transactions = len(failure_readonlytxn_latencies)

    return BenchmarkResult(
        num_transactions = num_transactions,
        num_successful_transactions = num_successful_transactions,
        num_failed_transactions = num_failed_transactions,
        num_failed_write_transactions = num_failed_write_transactions,
        num_failed_readonly_transactions = num_failed_readonly_transactions,
        abort_rate = float(num_failed_transactions) / num_transactions,
        write_txn_abort_rate = float(num_failed_write_transactions) / num_transactions,
        readonly_txn_abort_rate = float(num_failed_readonly_transactions) / num_transactions,

        throughput_all = float(num_transactions) / duration_sec,
        throughput_success = float(num_successful_transactions) / duration_sec,
        throughput_failure = float(num_failed_transactions) / duration_sec,

        average_latency_all = mean(all_latencies),
        median_latency_all = median(all_latencies),
        p99_latency_all = p99(all_latencies),
        p999_latency_all = p999(all_latencies),
        average_latency_success = mean(success_latencies),
        median_latency_success = median(success_latencies),
        p99_latency_success = p99(success_latencies),
        p999_latency_success = p999(success_latencies),
        average_latency_failure = mean(failure_latencies),
        median_latency_failure = median(failure_latencies),
        p99_latency_failure = p99(failure_latencies),
        p999_latency_failure = p999(failure_latencies),
        follow_txn_avg_latency_success = mean(follow_txn_success_latencies),
        tweet_txn_avg_latency_success = mean(tweet_txn_success_latencies),

        extra_all = all_num_extra,
        extra_success = success_num_extra,
        extra_failure = failure_num_extra,
    )

def main(args):
    result = process_client_logs(args.client_log, args.warmup, args.duration)
    print(json.dumps(result._asdict(), indent=4))

def parser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--warmup',
        type=float,
        default=10,
        help='The initial warmup time (in seconds) to ignore.')
    parser.add_argument(
        '--duration',
        type=float,
        required=True,
        help='The total number of seconds of log entries to analyze.')
    parser.add_argument(
        'client_log',
        type=str,
        help='The client log to parse.')
    return parser

if __name__ == '__main__':
    main(parser().parse_args())
