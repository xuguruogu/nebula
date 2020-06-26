/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/RocksEngineConfig.h"
#include "kvstore/EventListner.h"
#include "rocksdb/db.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "base/Configuration.h"

// [WAL]
DEFINE_bool(rocksdb_disable_wal,
            true,
            "Whether to disable the WAL in rocksdb");

DEFINE_bool(rocksdb_wal_sync,
            false,
            "Whether WAL writes are synchronized to disk or not");

// [DBOptions]
DEFINE_string(rocksdb_db_options,
              "{}",
              "json string of DBOptions, all keys and values are string");

// [CFOptions "default"]
DEFINE_string(rocksdb_column_family_options,
              "{}",
              "json string of ColumnFamilyOptions, all keys and values are string");

//  [TableOptions/BlockBasedTable "default"]
DEFINE_string(rocksdb_block_based_table_options,
              "{}",
              "json string of BlockBasedTableOptions, all keys and values are string");

DEFINE_int32(rocksdb_batch_size,
             4 * 1024,
             "default reserved bytes for one batch operation");
/*
 * For these un-supported string options as below, will need to specify them with gflag.
 */

// BlockBasedTable block_cache
DEFINE_int64(rocksdb_block_cache, 1024,
             "The default block cache size used in BlockBasedTable. The unit is MB");

DEFINE_bool(enable_partitioned_index_filter, false, "True for partitioned index filters");

DEFINE_int32(num_compaction_threads, 16, "Number of IO threads");

namespace nebula {
namespace kvstore {

static std::shared_ptr<rocksdb::Cache> __blockCache() {
    static std::mutex mutex;
    static std::shared_ptr<rocksdb::Cache> cache;
    std::unique_lock<std::mutex> l(mutex);
    if (!cache) {
        LOG(INFO) << "create block cache: " << FLAGS_rocksdb_block_cache << "MB";
        cache = rocksdb::NewLRUCache(FLAGS_rocksdb_block_cache * 1024 * 1024, 8);
    }
    return cache;
}

static std::shared_ptr<rocksdb::ConcurrentTaskLimiter> __compaction_thread_limiter() {
    static std::mutex mutex;
    static std::shared_ptr<rocksdb::ConcurrentTaskLimiter> compaction_thread_limiter;
    std::unique_lock<std::mutex> l(mutex);
    if (!compaction_thread_limiter) {
        LOG(INFO) << "create thread limiter: " << FLAGS_num_compaction_threads;
        compaction_thread_limiter.reset(rocksdb::NewConcurrentTaskLimiter("compaction", FLAGS_num_compaction_threads));
    }
    return compaction_thread_limiter;
}

rocksdb::Status initRocksdbOptions(rocksdb::Options &baseOpts) {
    rocksdb::Status s;
    rocksdb::DBOptions dbOpts;
    rocksdb::ColumnFamilyOptions cfOpts;
    rocksdb::BlockBasedTableOptions bbtOpts;

    std::unordered_map<std::string, std::string> dbOptsMap;
    if (!loadOptionsMap(dbOptsMap, FLAGS_rocksdb_db_options)) {
        LOG(ERROR) << ".....loadOptionsMap FLAGS_rocksdb_db_options error. ";
        return rocksdb::Status::InvalidArgument();
    }
    s = GetDBOptionsFromMap(rocksdb::DBOptions(), dbOptsMap, &dbOpts, true);
    if (!s.ok()) {
        LOG(ERROR) << ".....GetDBOptionsFromMap error. " << s.ToString();
        return s;
    }
    dbOpts.listeners.emplace_back(new EventListener());

    std::unordered_map<std::string, std::string> cfOptsMap;
    if (!loadOptionsMap(cfOptsMap, FLAGS_rocksdb_column_family_options)) {
        LOG(ERROR) << ".....loadOptionsMap FLAGS_rocksdb_column_family_options error. ";
        return rocksdb::Status::InvalidArgument();
    }
    s = GetColumnFamilyOptionsFromMap(rocksdb::ColumnFamilyOptions(), cfOptsMap, &cfOpts, true);
    if (!s.ok()) {
        LOG(ERROR) << ".....GetColumnFamilyOptionsFromMap error. " << s.ToString();
        return s;
    }

    baseOpts = rocksdb::Options(dbOpts, cfOpts);

    std::unordered_map<std::string, std::string> bbtOptsMap;
    if (!loadOptionsMap(bbtOptsMap, FLAGS_rocksdb_block_based_table_options)) {
        LOG(ERROR) << ".....loadOptionsMap FLAGS_rocksdb_block_based_table_options error. ";
        return rocksdb::Status::InvalidArgument();
    }
    s = GetBlockBasedTableOptionsFromMap(rocksdb::BlockBasedTableOptions(), bbtOptsMap,
                                         &bbtOpts, true);
    if (!s.ok()) {
        LOG(ERROR) << ".....GetBlockBasedTableOptionsFromMap error. " << s.ToString();
        return s;
    }

    bbtOpts.block_cache = __blockCache();
    bbtOpts.whole_key_filtering = false;
    bbtOpts.enable_index_compression = true;
    bbtOpts.format_version = 5;
    if (FLAGS_enable_partitioned_index_filter) {
        bbtOpts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        bbtOpts.partition_filters = true;
        bbtOpts.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
        bbtOpts.pin_l0_filter_and_index_blocks_in_cache = true;
        bbtOpts.pin_top_level_index_and_filter = true;
        bbtOpts.cache_index_and_filter_blocks = true;
        bbtOpts.cache_index_and_filter_blocks_with_high_priority = true;
    } else {
        bbtOpts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
    }

//    baseOpts.optimize_filters_for_hits = true;
//    baseOpts.table_cache_numshardbits = 8;
    baseOpts.table_factory.reset(NewBlockBasedTableFactory(bbtOpts));
    baseOpts.compression_per_level = std::vector<rocksdb::CompressionType> {
        rocksdb::CompressionType::kSnappyCompression,
        rocksdb::CompressionType::kSnappyCompression,
        rocksdb::CompressionType::kSnappyCompression,
        rocksdb::CompressionType::kSnappyCompression,
        rocksdb::CompressionType::kZSTD,
        rocksdb::CompressionType::kZSTD,
        rocksdb::CompressionType::kZSTD,
    };
    baseOpts.compaction_style = rocksdb::kCompactionStyleUniversal;
    baseOpts.compaction_options_universal.allow_trivial_move = true;
    baseOpts.compaction_options_universal.max_size_amplification_percent = 50;
    baseOpts.compaction_thread_limiter = __compaction_thread_limiter();
    baseOpts.dump_malloc_stats = true;
    baseOpts.report_bg_io_stats = true;
    baseOpts.memtable_insert_with_hint_prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(12));
    baseOpts.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(12));
    baseOpts.memtable_whole_key_filtering = false;
    baseOpts.create_if_missing = true;
    baseOpts.level_compaction_dynamic_level_bytes = true;

    return s;
}

bool loadOptionsMap(std::unordered_map<std::string, std::string> &map, const std::string& gflags) {
    Configuration conf;
    auto status = conf.parseFromString(gflags);
    if (!status.ok()) {
        return false;
    }
    conf.forEachItem([&map] (const std::string& key, const folly::dynamic& val) {
        LOG(INFO) << "Emplace rocksdb option " << key << "=" << val.asString();
        map.emplace(key, val.asString());
    });
    return true;
}

}  // namespace kvstore
}  // namespace nebula
