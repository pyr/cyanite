CREATE KEYSPACE metric WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

USE metric;

CREATE TYPE metric_point (
  max double,
  mean double,
  min double,
  sum double
);

CREATE TYPE metric_resolution (
  precision int,
  period int
);

CREATE TYPE metric_id (
  path text,
  resolution frozen<metric_resolution>
);

CREATE TABLE metric.metric (
  id frozen<metric_id>,
  time bigint,
  point frozen<metric_point>,
  PRIMARY KEY (id, time)
) WITH COMPACT STORAGE
  AND CLUSTERING ORDER BY (time ASC)
  AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
  AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
  AND dclocal_read_repair_chance = 0.1
  AND default_time_to_live = 0
  AND gc_grace_seconds = 864000
  AND max_index_interval = 2048
  AND memtable_flush_period_in_ms = 0
  AND min_index_interval = 128
  AND read_repair_chance = 0.0
  AND speculative_retry = '99.0PERCENTILE';
                                                                        
CREATE TYPE metric.segment_pos (
    pos int,
    segment text
);

CREATE TABLE metric.path (
    segment frozen<segment_pos>,
    path text,
    length int,
    PRIMARY KEY (segment, path)
) WITH CLUSTERING ORDER BY (path ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
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

CREATE TABLE metric.segment (
    pos int,
    segment text,
    PRIMARY KEY (pos, segment)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (segment ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
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
