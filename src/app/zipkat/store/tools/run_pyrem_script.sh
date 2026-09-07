MEERKAT_PATH=/home/yh885/meerkat
python3 e1_e2.py \
		--server_binary ${MEERKAT_PATH}/meerkat_server   \
		--client_binary ${MEERKAT_PATH}/retwisClient     \
		--config_file_directory ${MEERKAT_PATH}          \
		--key_file ${MEERKAT_PATH}/keys                  \
		--suite_directory ${MEERKAT_PATH}/yuju_benchmark
