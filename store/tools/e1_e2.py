from collections import defaultdict
from meerkat_benchmarks import (
    Parameters, ParametersAndResult, parse_args, run_suite, ziplog_order_servers, ziplog_storage_servers, clients)
from sheets_query import header
import benchmark


"""
    The scalability graph.
"""

def main(args):
    # Set benchmark parameters.
    base_parameters = Parameters(
        exp_type=None,
        config_file_directory=args.config_file_directory,
        f=1,
        key_file=args.key_file,
        num_keys=None,
        num_server_threads=None,
        repl_scheme=None,
        ziplog_order_binary=args.ziplog_order_binary,
        ziplog_order_port=args.ziplog_order_port,
        ziplog_order_cpus=args.ziplog_order_cpus,
        ziplog_storage_binary=args.ziplog_storage_binary,
        client_cpus=None,
        subscriber_cpus=None,
        get_cpus=None,
        client_binary=args.client_binary,
        client_rate=None,
#        benchmark_duration_seconds=15,
#        benchmark_warmup_seconds=5,
#        benchmark_duration_seconds=80,
#        benchmark_warmup_seconds=35,
#        benchmark_duration_seconds=150,
#        benchmark_warmup_seconds=60,
        benchmark_duration_seconds=30,
        benchmark_warmup_seconds=10,
#        benchmark_duration_seconds=50,
#        benchmark_warmup_seconds=20,
#        benchmark_duration_seconds=60,
#        benchmark_warmup_seconds=20,
        transaction_length=1,
        write_percentage=0,
        zipf_coefficient=None,
        num_client_machines=None,
        num_clients_per_machine=1,
        num_threads_per_client=None,
        num_fibers_per_client_thread=None,
        suite_directory=args.suite_directory,
    )

    base_rate = 65536
    rate = 51200
    #rate = 204800
    #rate = 409600
    base_num_keys = 500000
    all_get_cpus = "0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62"
    parameters_list = [
      base_parameters._replace(
          exp_type = exp_type,
          num_server_threads = num_server_threads,
          client_cpus=client_cpus,
          subscriber_cpus=subscriber_cpus,
          get_cpus=get_cpus,
          num_keys = num_keys,
          num_client_machines = num_client_machines,
          num_threads_per_client = num_threads_per_client,
          num_fibers_per_client_thread = num_fibers_per_client_thread,
          client_rate = client_rate,
          zipf_coefficient = zipf_coefficient,
      )

      for zipf_coefficient in [0]
      #for zipf_coefficient in [1, 0.8]

      # Keep load 24 clients per core
      for (num_server_threads,
           client_cpus,
           subscriber_cpus,
           get_cpus,
           num_keys,
           num_client_machines,
           num_threads_per_client,
           num_fibers_per_client_thread,
           client_rate) in [
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 7 * base_num_keys, 1, 1, 1, 25600),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 7 * base_num_keys, 10, 8, 1, 25600),

                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 2 * base_num_keys, 1, 5, 5, 102400),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 4 * base_num_keys, 2, 5, 5, 102400),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 8 * base_num_keys, 4, 5, 5, 102400),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,31,43,45,47,49,51,53,55,57,59,61,63", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 12 * base_num_keys, 6, 5, 5, 51200),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,31,43,45,47,49,51,53,55,57,59,61,63", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 14 * base_num_keys, 4, 8, 5, 51200),
                                       #(1, "3,5,7,9,35,37,39,41", "1,33,11,13,15,17,19,21,23,25,27,29,31,43,45,47,49,51,53,55,57,59,61,63", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 12 * base_num_keys, 6, 5, 5, 51200),
                                       #(1, "3,5,7,9,35,37,39,41", "1,33,11,13,15,17,19,21,23,25,27,29,31,43,45,47,49,51,53,55,57,59,61,63", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 14 * base_num_keys, 4, 8, 5, 51200),
                                       #(1, "5,7,9,37,39,41", "1,33,3,35,11,13,15,17,19,21,23,25,27,29,31,43,45,47,49,51,53,55,57,59,61,63", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 12 * base_num_keys, 6, 5, 5, 51200),
                                       #(1, "5,7,9,37,39,41", "1,33,3,35,11,13,15,17,19,21,23,25,27,29,31,43,45,47,49,51,53,55,57,59,61,63", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 14 * base_num_keys, 4, 8, 5, 51200),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 14 * base_num_keys, 8, 4, 5, 51200),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 16 * base_num_keys, 8, 5, 5, 51200),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 16 * base_num_keys, 5, 8, 5, 51200),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 20 * base_num_keys, 10, 5, 5, 51200),

                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", all_get_cpus, 16 * base_num_keys, 5, 8, 5, 51200),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 16 * base_num_keys, 1, 5, 5, 102400),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 16 * base_num_keys, 2, 5, 5, 51200),
                                       # generate data
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 2 * base_num_keys, 1, 5, 5, 102400),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 20 * base_num_keys, 10, 5, 5, 51200),
                                       #(1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 16 * base_num_keys, 8, 5, 5, 51200),
                                       (1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 12 * base_num_keys, 6, 5, 5, 51200),
                                       (1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 8 * base_num_keys, 4, 5, 5, 102400),
                                       (1, "1,3,5,7,9,33,35,37,39,41", "11,13,15,17,19,21,23,25,27,29,43,45,47,49,51,53,55,57,59,61", "0,2,4,6,8,10,12,14,32,34,36,38,40,42,44,46", 4 * base_num_keys, 2, 5, 5, 102400),
                                       # serialization perf debug
                                       #(1, "1,3,5,7,9,33,35,37,39,41,31,63,48,50,52,54,56,58,60,62,11,13,43,45,0,2,44,46,15,17,47,49,19,21,59,61,4,6,40,42,16,18,20,22,24,26,28,30,23,25,27,29,51,53,55,57", "", "8,10,12,14,32,34,36,38", 12 * base_num_keys, 6, 5, 5, 51200),
                                       #(4, 4 * 500000, 4, 8, 1),
                                       #(8, 8 * 500000, 2, 6, 8),
                                       #(16, 16 * 500000, 4, 6, 8),
                                       #(32, 32 * 500000, 8, 6, 8),
                                       #(64, 64 * 500000, 10, 8, 8),
                                       #(68, 68 * 500000, 10, 11, 8),
                                       #(72, 72 * 500000, 10, 11, 8),
                                       #(76, 76 * 500000, 10, 12, 8),
                                       #(78, 78 * 500000, 10, 12, 8),
                                       #(78, 78 * 500000, 10, 12, 8),
                                       #(80, 80 * 500000, 8, 12, 8),
                                       #(80, 80 * 500000, 10, 12, 8),
                                       ]

      for exp_type in ["client" + str(num_client_machines * num_threads_per_client * num_fibers_per_client_thread) + "_zipf" + str(zipf_coefficient) + "_keys" + str(num_keys)]
    ]

    # Run every experiment three times.
    parameters_list = [q for p in parameters_list for q in [p] * 3]

    # Run the suite.
    suite_dir = benchmark.SuiteDirectory(args.suite_directory, 'e1_and_e2')
    suite_dir.write_dict('args.json', vars(args))
    run_suite(suite_dir, parameters_list, clients(), ziplog_order_servers(), ziplog_storage_servers())

if __name__ == '__main__':
    main(parse_args())
