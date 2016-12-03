DROP KEYSPACE IF EXISTS cyanite_test_with_tokenizer;
CREATE KEYSPACE IF NOT EXISTS cyanite_test_with_tokenizer WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

USE cyanite_test_with_tokenizer;

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

CREATE CUSTOM INDEX IF NOT EXISTS on segment(segment) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.SplittingTokenizer'};
CREATE CUSTOM INDEX IF NOT EXISTS on segment(pos) USING 'org.apache.cassandra.index.sasi.SASIIndex';