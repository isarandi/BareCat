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

#include "barecat.h"


int barecat_read(struct BarecatContext *ctx, const char *path, void **buf_out, size_t *size_out) {
    sqlite3_stmt *s = ctx->stmt_get_file;
    sqlite3_reset(s);
    sqlite3_bind_text(s, sqlite3_bind_parameter_index(s, ":path"), path, -1, SQLITE_STATIC);
    if (sqlite3_step(s) != SQLITE_ROW) {
        return -ENOENT;
    }
    int shard = sqlite3_column_int(s, 0);
    size_t offset = sqlite3_column_int64(s, 1);
    size_t size = (size_t) sqlite3_column_int64(s, 2);
    if (size == 0) {
        *buf_out = NULL;
        *size_out = 0;
        return 0;
    }


    FILE *shard_file = ctx->shard_files[shard];
    if (fseek(shard_file, offset, SEEK_SET) != 0) {
        fprintf(stderr, "Error seeking in shard file: %s\n", strerror(errno));
        return -1;
    }

    void *buf = malloc(size);
    if (buf == NULL) {
        fprintf(stderr, "Error allocating %ld bytes of memory: %s\n", size, strerror(errno));
        return -1;
    }

    size_t num_read = fread(buf, 1, size, shard_file);
    if (num_read != size) {
        if (feof(shard_file)) {
            fprintf(stderr, "Error reading from shard %d, offset %ld, wanted %ld, read %ld: EOF\n", shard, offset, size, num_read);
        } else if (ferror(shard_file)) {
            fprintf(stderr, "Error reading from shard %d, read %ld: %s\n", shard, num_read, strerror(errno));
        }
        free(buf);
        return -1;
    }

    *buf_out = buf;
    *size_out = size;
    return 0;
}

int barecat_init(struct BarecatContext *ctx, const char *db_path, const char **shard_paths, size_t num_shards) {
    // Open the database
    ctx->db = NULL;
    ctx->stmt_get_file = NULL;
    ctx->shard_files = NULL;
    ctx->num_shards = 0;

    int rc = sqlite3_open_v2(db_path, &ctx->db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(ctx->db));
        int rc2 = sqlite3_close(ctx->db);
        return rc;
    }

    rc = sqlite3_exec(ctx->db, "BEGIN TRANSACTION", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot begin transaction database: %s\n", sqlite3_errmsg(ctx->db));
        int rc2 = sqlite3_close(ctx->db);
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
    ctx->shard_files = malloc(num_shards * sizeof(FILE*));
    if (ctx->shard_files == NULL) {
        fprintf(stderr, "Error allocating memory for shard files: %s\n", strerror(errno));
        barecat_destroy(ctx);
        return -1;
    }

    for (size_t i = 0; i < num_shards; i++) {
        ctx->shard_files[i] = fopen(shard_paths[i], "rb");
        if (ctx->shard_files[i] == NULL) {
            fprintf(stderr, "Error opening file %s: %s\n", shard_paths[i], strerror(errno));
            barecat_destroy(ctx);
            return -1;
        }
        ctx->num_shards++;
    }
    return 0;
}

int barecat_destroy(struct BarecatContext *ctx) {
    if (ctx->stmt_get_file) {
        sqlite3_finalize(ctx->stmt_get_file);
    }
    if (ctx->db) {
        sqlite3_close(ctx->db);
    }
    for (size_t i = 0; i < ctx->num_shards; i++) {
        fclose(ctx->shard_files[i]);
    }
    free(ctx->shard_files);
    return 0;
}
