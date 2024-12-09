#ifndef BARECAT_MMAP_H
#define BARECAT_MMAP_H

#include <sqlite3.h>
#include <stddef.h>
#include <stdint.h>

struct BarecatMmapContext {
    sqlite3 *db;
    sqlite3_stmt *stmt_get_file;
    void **shard_mmaps;
    size_t num_shards;
    size_t *shard_sizes;
};

int barecat_mmap_crc32c_from_address(struct BarecatMmapContext *ctx, int shard, size_t offset, size_t size, uint32_t *crc_out);

int barecat_mmap_read_from_address(struct BarecatMmapContext *ctx, int shard, size_t offset, size_t size, void **buf_out);

int barecat_mmap_read(struct BarecatMmapContext *ctx, const char *path, void **buf, size_t *size);

int barecat_mmap_init(struct BarecatMmapContext *ctx, const char *db_path, const char **shard_paths, size_t num_shards);

int barecat_mmap_destroy(struct BarecatMmapContext *ctx);

#endif // BARECAT_MMAP_H