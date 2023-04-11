CREATE TABLE IF NOT EXISTS orgunit
(
    uid String,
    name String,
    level UInt16
)
ENGINE = MergeTree
PRIMARY KEY (level)