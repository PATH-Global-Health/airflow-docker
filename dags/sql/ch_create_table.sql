CREATE TABLE IF NOT EXISTS data_source ( id String, title String, url String, description String ) ENGINE = MergeTree PRIMARY KEY (id)
CREATE TABLE IF NOT EXISTS orgunit ( uid String, name String, level UInt16, source_id String, lastupdated DateTime ) ENGINE = ReplacingMergeTree(lastupdated) ORDER BY (uid, name, level, source_id)