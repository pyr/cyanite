DROP KEYSPACE IF EXISTS cyanite_test;
CREATE KEYSPACE IF NOT EXISTS cyanite_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

USE cyanite_test;

CREATE TYPE IF NOT EXISTS metric_point (
  max double,
  mean double,
  min double,
  sum double
);

CREATE TYPE IF NOT EXISTS metric_resolution (
  precision int,
  period int
);

CREATE TYPE IF NOT EXISTS metric_id (
  path text,
  resolution frozen<metric_resolution>
);

CREATE TABLE IF NOT EXISTS metric (
  id frozen<metric_id>,
  time bigint,
  point frozen<metric_point>,
  PRIMARY KEY (id, time)
) WITH CLUSTERING ORDER BY (time ASC)
  AND compaction = {'class': 'DateTieredCompactionStrategy',  'min_threshold': '12', 'max_threshold': '32', 'max_sstable_age_days': '0.083', 'base_time_seconds': '50' }
  AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
  AND dclocal_read_repair_chance = 0.1
  AND default_time_to_live = 0
  AND gc_grace_seconds = 864000
  AND max_index_interval = 2048
  AND memtable_flush_period_in_ms = 0
  AND min_index_interval = 128
  AND read_repair_chance = 0.0
  AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE IF NOT EXISTS path (
    prefix text,
    path text,
    length int,
    PRIMARY KEY (prefix, path)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

CREATE CUSTOM INDEX IF NOT EXISTS on path(path) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'PREFIX'};
CREATE CUSTOM INDEX IF NOT EXISTS on path(length) USING 'org.apache.cassandra.index.sasi.SASIIndex';

CREATE TABLE IF NOT EXISTS segment (
    parent text,
    segment text,
    pos int,
    length int,
    leaf boolean,
    PRIMARY KEY (parent, segment)
) WITH CLUSTERING ORDER BY (segment ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

CREATE CUSTOM INDEX IF NOT EXISTS on segment(segment) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'PREFIX'};
CREATE CUSTOM INDEX IF NOT EXISTS on segment(pos) USING 'org.apache.cassandra.index.sasi.SASIIndex';