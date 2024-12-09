#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <glob.h>
#include <errno.h>
#include <sys/types.h>

#include "barecat_mmap.h"
#include "crc32c.h"


int barecat_mmap_read_from_address(struct BarecatMmapContext *ctx, int shard, size_t offset, size_t size, void **buf_out) {
    if (shard < 0 || shard >= ctx->num_shards) {
        return -EINVAL;
    }
    if (offset + size > ctx->shard_sizes[shard]) {
        return -EINVAL;
    }
    *buf_out = ctx->shard_mmaps[shard] + offset;
    return 0;
}


int barecat_mmap_read(struct BarecatMmapContext *ctx, const char *path, void **buf, size_t *size) {
    sqlite3_stmt *s = ctx->stmt_get_file;
    sqlite3_reset(s);
    sqlite3_bind_text(s, sqlite3_bind_parameter_index(s, ":path"), path, -1, SQLITE_STATIC);
    if (sqlite3_step(s) != SQLITE_ROW) {
        return -ENOENT;
    }
    int shard = sqlite3_column_int(s, 0);
    size_t offset = sqlite3_column_int64(s, 1);
    *size = (size_t) sqlite3_column_int64(s, 2);
    *buf = ctx->shard_mmaps[shard] + offset;
    return 0;
}

static int open_and_map_file(const char *shard_path, void **mmap_ptr, size_t *size) {
    int fd = open(shard_path, O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "Error opening file '%s': %s\n", shard_path, strerror(errno));
        return -1;
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        fprintf(stderr, "Error getting file stats for '%s': %s\n", shard_path, strerror(errno));
        close(fd);
        return -1;
    }
    void *mmap_result = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmap_result == MAP_FAILED) {
        fprintf(stderr, "Error mapping file '%s': %s\n", shard_path, strerror(errno));
        close(fd);
        return -1;
    }
    if (close(fd) == -1) {
        fprintf(stderr, "Error closing file '%s': %s\n", shard_path, strerror(errno));
        munmap(mmap_result, st.st_size);
        return -1;
    }
    *mmap_ptr = mmap_result;
    *size = st.st_size;
    return 0;
}

int barecat_mmap_init(struct BarecatMmapContext *ctx, const char *db_path, const char **shard_paths, size_t num_shards) {
    // Open the database
    ctx->db = NULL;
    ctx->stmt_get_file = NULL;
    ctx->shard_mmaps = NULL;
    ctx->shard_sizes = NULL;
    ctx->num_shards = 0;


    int rc = sqlite3_open_v2(db_path, &ctx->db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(ctx->db));
        sqlite3_close(ctx->db);
        return rc;
    }

    // Prepare the statement
    const char *query = "SELECT shard, offset, size, crc32c FROM files WHERE path = :path";
    rc = sqlite3_prepare_v2(ctx->db, query, -1, &ctx->stmt_get_file, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot prepare statement: %s\n", sqlite3_errmsg(ctx->db));
        sqlite3_close(ctx->db);
        return rc;
    }

    // Open the shards
    ctx->shard_mmaps = malloc(num_shards * sizeof(void*));
    ctx->shard_sizes = malloc(num_shards * sizeof(size_t));
    for (size_t i = 0; i < num_shards; i++) {
        rc = open_and_map_file(shard_paths[i], &ctx->shard_mmaps[i], &ctx->shard_sizes[i]);
        if (rc != 0) {
            fprintf(stderr, "Error opening and mapping file %s\n", shard_paths[i]);
            barecat_destroy(ctx);
            return rc;
        }
        //fprintf(stderr, "Mapped file %s\n", shard_paths[i]);
        ctx->num_shards++;
    }
    return 0;
}

int barecat_mmap_crc32c_from_address(struct BarecatMmapContext *ctx, int shard, size_t offset, size_t size, uint32_t *crc_out) {
    void *buf;
    int rc = barecat_mmap_read_from_address(ctx, shard, offset, size, &buf);
    if (rc != 0) {
        return rc;
    }
    *crc_out = crc32c(0, buf, size);
    return 0;
}


int barecat_mmap_destroy(struct BarecatMmapContext *ctx) {
    if (ctx->stmt_get_file) {
        sqlite3_finalize(ctx->stmt_get_file);
    }
    if (ctx->db) {
        sqlite3_close(ctx->db);
    }
    for (size_t i = 0; i < ctx->num_shards; i++) {
        munmap(ctx->shard_mmaps[i], ctx->shard_sizes[i]);
    }
    free(ctx->shard_mmaps);
    free(ctx->shard_sizes);
    return 0;
}
