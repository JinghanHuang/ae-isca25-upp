# ae-isca25-upp

### Running experiments for figure 7
```bash
chmod +x run_upp_experiments.sh
```
Then run experiments. 

For usps:
```bash
./run_upp_experiments.sh  usps
```
For baseline:
```bash
./run_upp_experiments.sh  baseline
```
### Generating Hash
Use `hash.py` to generate hash for data, query, and the percentiles used to generate the hash. 

Percentiles
```
python hash.py percentiles --input <input.tbl> --output <output_percentiles.npy> [--delimiter <delimiter>] [--size <hash_size>] [--chunksize <chunk_size>]
```
Data Hash
```
python hash.py data --input <input.tbl> --output-hash <output_hash.bin> [--output-len <output_len.bin>] [--percentile <percentile_file.npy>] [--delimiter <delimiter>] [--size <hash_size>]
```
Query Hash
```
python hash.py query --output-dir <output_directory> [--base-dir <percentile_base_dir>] [--size <hash_size>]
```
