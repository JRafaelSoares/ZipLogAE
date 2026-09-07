ZIPKAT_PATH=/home/yh885/zipkat
ZIPLOG_ORDER_IP=192.168.99.16
ZIPLOG_ORDER_PORT=5813
python3 e1_e2.py \
		--ziplog_order_binary ${ZIPKAT_PATH}/order \
        --ziplog_order_port ${ZIPLOG_ORDER_PORT} \
        --ziplog_order_cpus "12" \
		--ziplog_storage_binary ${ZIPKAT_PATH}/storage \
		--client_binary ${ZIPKAT_PATH}/retwisClient     \
		--config_file_directory ${ZIPKAT_PATH}          \
		--key_file ${ZIPKAT_PATH}/keys                  \
		--suite_directory ${ZIPKAT_PATH}/yuju_benchmark
