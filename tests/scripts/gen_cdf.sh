rm -f czy && ./concurrent_hash_map_ycsb_resize czy /home/czy/benchmark/traces/pmdk-w-100-large.load /home/czy/benchmark/traces/pmdk-w-100-large.run 16
python gen_cdf.py latency_records.txt latency_cmap.csv
rm -f czy && ./concurrent_level_hash_ycsb_resize czy /home/czy/benchmark/traces/pmdk-w-100-large.load /home/czy/benchmark/traces/pmdk-w-100-large.run 16
python gen_cdf.py latency_records.txt latency_level.csv
rm -f czy && ./cceh_ycsb_resize czy /home/czy/benchmark/traces/pmdk-w-100-large.load /home/czy/benchmark/traces/pmdk-w-100-large.run 16
python gen_cdf.py latency_records.txt latency_cceh.csv
rm -f czy && ./clevel_hash_ycsb_resize czy /home/czy/benchmark/traces/pmdk-w-100-large.load /home/czy/benchmark/traces/pmdk-w-100-large.run 16
python gen_cdf.py latency_records.txt latency_clevel.csv
