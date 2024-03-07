import bz2
import csv
import lzma
import pathlib
import time
import zlib

import brotli
import msgpack
import tabulate
import zstandard as zstd

HIVE_24HR_DIR = "/home/junhao/samples/hive-24hr"

path_list = list(pathlib.Path(HIVE_24HR_DIR).rglob("*"))
path_string_list = [str(path) for path in path_list]

clp_config = {
    "input": {'paths_to_compress': path_string_list},
    "output": {}
}

encoded_data = msgpack.packb(clp_config)


def lzma_compression_benchmark(data, levels=range(10)):
    results = []
    for level in levels:
        start_time = time.time()
        compressed_data = lzma.compress(data, preset=level)
        end_time = time.time()

        compression_time = end_time - start_time
        compressed_size = len(compressed_data)

        results.append({
            "Compression Level": level,
            "Time (seconds)": compression_time,
            "Compressed Size (bytes)": compressed_size
        })

    return results


def zlib_compression_benchmark(data, levels=range(10)):
    results = []
    for level in levels:
        start_time = time.time()
        compressed_data = zlib.compress(data, level=level)
        end_time = time.time()

        compression_time = end_time - start_time
        compressed_size = len(compressed_data)

        results.append({
            "Compression Level": level,
            "Time (seconds)": compression_time,
            "Compressed Size (bytes)": compressed_size
        })

    return results


def bz2_compression_benchmark(data, levels=range(1, 10)):
    results = []
    for level in levels:
        start_time = time.time()
        compressed_data = bz2.compress(data, compresslevel=level)
        end_time = time.time()

        compression_time = end_time - start_time
        compressed_size = len(compressed_data)

        results.append({
            "Compression Level": level,
            "Time (seconds)": compression_time,
            "Compressed Size (bytes)": compressed_size
        })

    return results


def zstd_compression_benchmark(data, levels=range(1, 23)):
    results = []
    for level in levels:
        start_time = time.time()
        compressor = zstd.ZstdCompressor(level=level)
        compressed_data = compressor.compress(data)
        end_time = time.time()

        compression_time = end_time - start_time
        compressed_size = len(compressed_data)

        results.append({
            "Compression Level": level,
            "Time (seconds)": compression_time,
            "Compressed Size (bytes)": compressed_size
        })

    return results


def brotli_compression_benchmark(data, levels=range(0, 12)):  # Brotli levels range from 0 to 11
    results = []
    for level in levels:
        start_time = time.time()
        compressed_data = brotli.compress(data, quality=level)
        end_time = time.time()

        compression_time = end_time - start_time
        compressed_size = len(compressed_data)

        results.append({
            "Compression Level": level,
            "Time (seconds)": compression_time,
            "Compressed Size (bytes)": compressed_size
        })

    return results


def print_as_table(results, name):
    print(name)
    header = results[0].keys()
    rows = [x.values() for x in results]
    print(tabulate.tabulate(rows, header))
    print()

    with open(f'{name}.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        for r in rows:
            writer.writerow(r)


lzma_results = lzma_compression_benchmark(encoded_data)
print_as_table(lzma_results, name="LZMA")

gzip_results = zlib_compression_benchmark(encoded_data)
print_as_table(gzip_results, name="Gzip")

bz2_results = bz2_compression_benchmark(encoded_data)
print_as_table(bz2_results, name="BZ2")

zstd_results = zstd_compression_benchmark(encoded_data)
print_as_table(zstd_results, name="Zstandard")

brotli_results = brotli_compression_benchmark(encoded_data)
print_as_table(brotli_results, name="Brotli")

combined_results = []
for name, results in [("LZMA", lzma_results), ("Gzip", gzip_results),
                      ("BZ2", bz2_results), ("Zstandard", zstd_results),
                      ("Brotli", brotli_results)]:
    for result in results:
        combined_result = {"Algorithm": name}
        combined_result.update(result)
        combined_results.append(combined_result)

print_as_table(combined_results, "Combined")
