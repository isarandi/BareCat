-- Description: Schema for the barecat database

PRAGMA recursive_triggers = ON;
PRAGMA foreign_keys = ON;

--####################################  Tables
CREATE TABLE files
(
    path     TEXT PRIMARY KEY NOT NULL,
    parent   TEXT GENERATED ALWAYS AS ( -- Parent directory is computed automatically
        rtrim(rtrim(path, replace(path, '/', '')), '/')
        ) VIRTUAL             NOT NULL REFERENCES dirs (path) ON DELETE RESTRICT,

    shard    INTEGER          NOT NULL,
    offset   INTEGER          NOT NULL,
    size     INTEGER DEFAULT 0,
    crc32c    INTEGER DEFAULT NULL,

    mode     INTEGER DEFAULT NULL,
    uid      INTEGER DEFAULT NULL,
    gid      INTEGER DEFAULT NULL,
    mtime_ns INTEGER DEFAULT NULL
);

CREATE TABLE dirs
(
    path           TEXT PRIMARY KEY NOT NULL,
    parent         TEXT GENERATED ALWAYS AS (
        CASE
            WHEN path = '' THEN NULL
            ELSE rtrim(rtrim(path, replace(path, '/', '')), '/')
            END
        ) VIRTUAL REFERENCES dirs (path) ON DELETE RESTRICT,

    num_subdirs    INTEGER DEFAULT 0, -- These are maintained by triggers
    num_files      INTEGER DEFAULT 0,
    num_files_tree INTEGER DEFAULT 0,
    size_tree      INTEGER DEFAULT 0,

    mode           INTEGER DEFAULT NULL,
    uid            INTEGER DEFAULT NULL,
    gid            INTEGER DEFAULT NULL,
    mtime_ns       INTEGER DEFAULT NULL
);

CREATE TABLE config -- For now, this table only holds the `shard_size_limit`
(
    key        TEXT PRIMARY KEY,
    value_text TEXT    DEFAULT NULL,
    value_int  INTEGER DEFAULT NULL
);

INSERT INTO config (key, value_int)
VALUES ('use_triggers', 1),
       ('shard_size_limit', CAST(power(2, 63) - 1 AS INTEGER)),
       ('schema_version_major', 0),
       ('schema_version_minor', 1);
-- Initial values
INSERT INTO dirs (path)
VALUES ('');
-- Root directory, mandatory

-- Indexes
CREATE INDEX idx_files_parent ON files (parent);
CREATE INDEX idx_dirs_parent ON dirs (parent);
CREATE INDEX idx_files_shard_offset ON files (shard, offset);

--####################################  Triggers
--  The idea is: we propagate changes up the tree with triggers, as this is cumbersome to do in
--  the Python code. There is no propagation downwards (for example when moving a dir, we do not
--  update all the children with triggers). This is because the Python code can do this more
--  quite easily. Furthermore, if we did it with triggers, the chain would start upward again
--  with a circular mess. So we only propagate upwards the tee.
--  We propagate two kinds of things:
--  1) statistics direct and aggregate file count and aggregate size
--  2) modification time of the parent directory
--  We don't update the modification time of the entity being inserted or modified,
--  this can be simply done in the Python code. If the app doesn't supply mtime, presumably it
--  doesn't care about it, so the overhead of triggering it makes no sense.

---- Files: add, del, move, resize
CREATE TRIGGER add_file -- Upsert the parent when adding a file
    AFTER INSERT
    ON files
    WHEN (SELECT value_int
          FROM config
          WHERE key = 'use_triggers') = 1
BEGIN
    -- Add the parent directory if it doesn't exist
    INSERT INTO dirs (path, num_files, num_files_tree, size_tree, mtime_ns)
    VALUES (NEW.parent, 1, 1, NEW.size,
            CAST((julianday('now') - 2440587.5) * 86400.0 * 1e9 AS INTEGER))
    -- If the parent directory already exists, update it
    ON CONFLICT(path) DO UPDATE SET num_files      = num_files + 1,
                                    num_files_tree = num_files_tree + 1,
                                    size_tree      = size_tree + excluded.size_tree,
                                    mtime_ns       = excluded.mtime_ns;
END;

CREATE TRIGGER del_file -- Update the parent when deleting a file
    AFTER DELETE
    ON files
    WHEN (SELECT value_int
          FROM config
          WHERE key = 'use_triggers') = 1
BEGIN
    UPDATE dirs
    SET num_files      = num_files - 1,
        num_files_tree = num_files_tree - 1,
        size_tree      = size_tree - OLD.size,
        mtime_ns       = CAST((julianday('now') - 2440587.5) * 86400.0 * 1e9 AS INTEGER)
    WHERE path = OLD.parent;
END;

CREATE TRIGGER move_file -- Update both parents when moving a file
    AFTER UPDATE OF path
    ON files
    WHEN NEW.parent != OLD.parent
        AND (SELECT value_int
             FROM config
             WHERE key = 'use_triggers') = 1
BEGIN
    UPDATE dirs
    SET num_files      = num_files + 1,
        num_files_tree = num_files_tree + 1,
        size_tree      = size_tree + NEW.size,
        mtime_ns       = CAST((julianday('now') - 2440587.5) * 86400.0 * 1e9 AS INTEGER)
    WHERE path = NEW.parent;
    UPDATE dirs
    SET num_files      = num_files - 1,
        num_files_tree = num_files_tree - 1,
        size_tree      = size_tree - OLD.size,
        mtime_ns       = CAST((julianday('now') - 2440587.5) * 86400.0 * 1e9 AS INTEGER)
    WHERE path = OLD.parent;
END;

CREATE TRIGGER resize_file -- When file size changes
    AFTER UPDATE OF size
    ON files
    WHEN NEW.parent == OLD.parent -- and the file was not moved
        AND (SELECT value_int
             FROM config
             WHERE key = 'use_triggers') = 1
BEGIN
    UPDATE dirs
    SET size_tree = size_tree + NEW.size - OLD.size
    WHERE path = OLD.parent;
END;

---- Directories: add, del, move, resize
CREATE TRIGGER add_subdir -- Upsert the parent when adding a directory
    AFTER INSERT
    ON dirs
    WHEN (SELECT value_int
          FROM config
          WHERE key = 'use_triggers') = 1
BEGIN
    INSERT INTO dirs (path, num_subdirs, size_tree, num_files_tree, mtime_ns)
    VALUES (NEW.parent, 1, NEW.size_tree, NEW.num_files_tree,
            CAST((julianday('now') - 2440587.5) * 86400.0 * 1e9 AS INTEGER))
    ON CONFLICT(path) DO UPDATE SET num_subdirs    = num_subdirs + 1,
                                    size_tree      = size_tree + excluded.size_tree,
                                    num_files_tree = num_files_tree + excluded.num_files_tree,
                                    mtime_ns= excluded.mtime_ns;
END;

CREATE TRIGGER del_subdir -- Update the parent when deleting a directory
    AFTER DELETE
    ON dirs
    WHEN (SELECT value_int
          FROM config
          WHERE key = 'use_triggers') = 1
BEGIN
    UPDATE dirs
    SET num_subdirs    = num_subdirs - 1,
        num_files      = num_files - OLD.num_files,
        size_tree      = size_tree - OLD.size_tree,
        num_files_tree = num_files_tree - OLD.num_files_tree,
        mtime_ns       = CAST((julianday('now') - 2440587.5) * 86400.0 * 1e9 AS INTEGER)
    WHERE path = OLD.parent;
END;

CREATE TRIGGER move_subdir -- Update both parents when moving a directory
    AFTER UPDATE OF path
    ON dirs
    WHEN NEW.parent != OLD.parent
        AND (SELECT value_int
             FROM config
             WHERE key = 'use_triggers') = 1
BEGIN
    UPDATE dirs
    SET num_subdirs    = num_subdirs - 1,
        num_files      = num_files - OLD.num_files,
        size_tree      = size_tree - OLD.size_tree,
        num_files_tree = num_files_tree - OLD.num_files_tree,
        mtime_ns       = CAST((julianday('now') - 2440587.5) * 86400.0 * 1e9 AS INTEGER)
    WHERE path = OLD.parent;
    UPDATE dirs
    SET num_subdirs    = num_subdirs + 1,
        num_files      = num_files + NEW.num_files,
        size_tree      = size_tree + NEW.size_tree,
        num_files_tree = num_files_tree + NEW.num_files_tree,
        mtime_ns       = CAST((julianday('now') - 2440587.5) * 86400.0 * 1e9 AS INTEGER)
    WHERE path = NEW.parent;
END;


CREATE TRIGGER resize_dir -- Update the parent when a directory changes size
    AFTER UPDATE OF size_tree, num_files_tree
    ON dirs
    WHEN NEW.parent = OLD.parent AND
         (NEW.size_tree != OLD.size_tree OR NEW.num_files_tree != OLD.num_files_tree)
        AND (SELECT value_int
             FROM config
             WHERE key = 'use_triggers') = 1
BEGIN
    UPDATE dirs
    SET size_tree      = size_tree + (NEW.size_tree - OLD.size_tree),
        num_files_tree = num_files_tree + (NEW.num_files_tree - OLD.num_files_tree)
    WHERE path = OLD.parent;
END;