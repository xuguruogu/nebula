/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_COMPACTIONFILTER_H_
#define STORAGE_COMPACTIONFILTER_H_

#include "base/Base.h"
#include "utils/NebulaKeyUtils.h"
#include "dataman/RowReader.h"
#include "meta/NebulaSchemaProvider.h"
#include "kvstore/CompactionFilter.h"
#include "storage/CommonUtils.h"

DEFINE_bool(storage_kv_mode, false, "True for kv mode");
DEFINE_bool(storage_remove_future_data, false, "storage remove future data");

namespace nebula {
namespace storage {

class StorageCompactionFilter final : public kvstore::KVFilter {
public:
    StorageCompactionFilter(meta::SchemaManager* schemaMan,
                            meta::IndexManager* indexMan)
        : schemaMan_(schemaMan)
        , indexMan_(indexMan) {
        CHECK_NOTNULL(schemaMan_);
    }

    bool filter(GraphSpaceID spaceId,
                const folly::StringPiece& key,
                const folly::StringPiece& val) const override {
        if (FLAGS_storage_kv_mode) {
            // in kv mode, we don't delete any data
            return false;
        }

        if (NebulaKeyUtils::isDataKey(key)) {
            if (!schemaValid(spaceId, key)) {
                return true;
            }
            if (isInvalidReverseEdgeKey(key, val)) {
                return true;
            }
            if (!ttlValid(spaceId, key, val)) {
                VLOG(3) << "TTL invalid for key " << key;
                return true;
            }
            if (filterVersions(spaceId, key)) {
                VLOG(3) << "Extra versions has been filtered!";
                return true;
            }
        } else if (NebulaKeyUtils::isIndexKey(key)) {
            if (!indexValid(spaceId, key)) {
                VLOG(3) << "Index invalid for the key " << key;
                return true;
            }
        } else {
            VLOG(3) << "Skip the system key inside, key " << key;
        }
        return false;
    }

    bool schemaValid(GraphSpaceID spaceId, const folly::StringPiece& key) const {
        if (NebulaKeyUtils::isVertex(key)) {
            auto tagId = NebulaKeyUtils::getTagId(key);
            auto ret = schemaMan_->getLatestTagSchemaVersion(spaceId, tagId);
            if (ret.ok() && ret.value() == -1) {
                VLOG(3) << "Space " << spaceId << ", Tag " << tagId << " invalid";
                return false;
            }
        } else if (NebulaKeyUtils::isEdge(key)) {
            auto edgeType = NebulaKeyUtils::getEdgeType(key);
            if (edgeType < 0) {
                edgeType = -edgeType;
            }
            auto ret = schemaMan_->getLatestEdgeSchemaVersion(spaceId, edgeType);
            if (ret.ok() && ret.value() == -1) {
                VLOG(3) << "Space " << spaceId << ", EdgeType " << edgeType << " invalid";
                return false;
            }
        }
        return true;
    }

    bool isInvalidReverseEdgeKey(const folly::StringPiece& key,
                                 const folly::StringPiece& val) const {
        if (NebulaKeyUtils::isEdge(key)) {
            auto edgeType = NebulaKeyUtils::getEdgeType(key);
            return edgeType < 0 && val.empty();
        }
        return false;
    }

    bool ttlValid(GraphSpaceID spaceId, const folly::StringPiece& key,
                  const folly::StringPiece& val) const {
        if (NebulaKeyUtils::isVertex(key)) {
            auto tagId = NebulaKeyUtils::getTagId(key);
            auto schema = this->schemaMan_->getTagSchema(spaceId, tagId);
            if (!schema) {
                VLOG(3) << "Space " << spaceId << ", Tag " << tagId << " invalid";
                return false;
            }
            auto reader = nebula::RowReader::getTagPropReader(schemaMan_, val, spaceId, tagId);
            if (reader == nullptr) {
                VLOG(3) << "Remove the bad format vertex";
                return false;
            }
            return checkDataTtlValid(schema.get(), reader.get());
        } else if (NebulaKeyUtils::isEdge(key)) {
            auto edgeType = NebulaKeyUtils::getEdgeType(key);
            auto schema = this->schemaMan_->getEdgeSchema(spaceId, std::abs(edgeType));
            if (!schema) {
                VLOG(3) << "Space " << spaceId << ", EdgeType " << edgeType << " invalid";
                return false;
            }
            auto reader = nebula::RowReader::getEdgePropReader(schemaMan_, val,
                                                               spaceId, std::abs(edgeType));
            if (reader == nullptr) {
                VLOG(3) << "Remove the bad format edge!";
                return false;
            }
            return checkDataTtlValid(schema.get(), reader.get());
        }
        return true;
    }

    // TODO(panda) Optimize the method in the future
    bool checkDataTtlValid(const meta::SchemaProviderIf* schema, nebula::RowReader* reader) const {
        const auto* nschema = dynamic_cast<const meta::NebulaSchemaProvider*>(schema);
        if (nschema == NULL) {
            return true;
        }
        const auto schemaProp = nschema->getProp();
        int ttlDuration = 0;
        if (schemaProp.get_ttl_duration()) {
            ttlDuration = *schemaProp.get_ttl_duration();
        }
        std::string ttlCol;
        if (schemaProp.get_ttl_col()) {
            ttlCol = *schemaProp.get_ttl_col();
        }

        // Only support the specified ttl_col mode
        // Not specifying or non-positive ttl_duration behaves like ttl_duration = infinity
        if (ttlCol.empty() ||  ttlDuration <= 0) {
            return true;
        }

        return !nebula::storage::checkDataExpiredForTTL(schema, reader, ttlCol, ttlDuration);
    }

    bool filterVersions(GraphSpaceID spaceId, const folly::StringPiece& key) const {
        folly::StringPiece keyWithNoVersion = NebulaKeyUtils::keyWithNoVersion(key);
        int64_t version = NebulaKeyUtils::getVersionBigEndian(key);
        if (FLAGS_storage_remove_future_data &&
            std::numeric_limits<int64_t>::max() - version > time::WallClock::fastNowInMicroSec() + /* 1min */ 60 * 1000000 &&
            (NebulaKeyUtils::isVertex(key) || NebulaKeyUtils::isEdge(key))) {
            return true;
        }

        if (NebulaKeyUtils::isVertex(key)) {
            auto tagId = NebulaKeyUtils::getTagId(key);
            auto schema = this->schemaMan_->getTagSchema(spaceId, tagId);
            if (!schema) {
                VLOG(3) << "Space " << spaceId << ", Tag " << tagId << " invalid";
                return true;
            }
            auto nschema = dynamic_cast<const meta::NebulaSchemaProvider*>(schema.get());
            if (nschema == NULL) {
                VLOG(3) << "Space " << spaceId << ", Tag " << tagId << " invalid";
                return true;
            }
            const nebula::cpp2::MultiVersions& multi_versions = nschema->getMultiVersions();
            if (multi_versions.__isset.reserve_verions ||
                multi_versions.__isset.active_version ||
                multi_versions.__isset.max_version) {
                VLOG(5) << "Multi version try filter: Space " << spaceId << ", Tag " << tagId
                        << " part " << NebulaKeyUtils::getPart(key)
                        << " vId " << NebulaKeyUtils::getVertexId(key)
                        << " version " << version;
                if (
                    // match reserve versions.
                    (multi_versions.__isset.reserve_verions &&
                     (multi_versions.get_reserve_verions()->empty() ||
                      std::find(
                          multi_versions.get_reserve_verions()->begin(),
                          multi_versions.get_reserve_verions()->end(),
                          version
                      ) != multi_versions.get_reserve_verions()->end()))
                    // match active version.
                    || (multi_versions.__isset.active_version &&
                        version == *multi_versions.get_active_version())
                    // match max_version and keep min version.
                    || (multi_versions.__isset.max_version &&
                        version <= *multi_versions.get_max_version() &&
                        keyWithNoVersion != lastKeyWithNoVersion_)
                    ) {
                    // keep
                    lastKeyWithNoVersion_ = keyWithNoVersion.str();
                    return false;
                }
                return true;
            }
        } else if (NebulaKeyUtils::isEdge(key)) {
            auto edgeType = NebulaKeyUtils::getEdgeType(key);
            auto schema = this->schemaMan_->getEdgeSchema(spaceId, std::abs(edgeType));
            if (!schema) {
                VLOG(3) << "Space " << spaceId << ", EdgeType " << edgeType << " invalid";
                return true;
            }
            auto nschema = dynamic_cast<const meta::NebulaSchemaProvider*>(schema.get());
            if (nschema == NULL) {
                VLOG(3) << "Space " << spaceId << ", EdgeType " << edgeType << " invalid";
                return true;
            }
            const nebula::cpp2::MultiVersions& multi_versions = nschema->getMultiVersions();
            if (multi_versions.__isset.reserve_verions ||
                multi_versions.__isset.active_version ||
                multi_versions.__isset.max_version) {
                VLOG(5) << "Multi version try filter: Space " << spaceId << ", EdgeType " << edgeType
                        << " part " << NebulaKeyUtils::getPart(key)
                        << " srcId " << NebulaKeyUtils::getSrcId(key)
                        << " rank " << NebulaKeyUtils::getRank(key)
                        << " dstId " << NebulaKeyUtils::getDstId(key)
                        << " version " << version;
                if (
                    // match reserve versions.
                    (multi_versions.__isset.reserve_verions &&
                     (multi_versions.get_reserve_verions()->empty() ||
                      std::find(
                          multi_versions.get_reserve_verions()->begin(),
                          multi_versions.get_reserve_verions()->end(),
                          version
                      ) != multi_versions.get_reserve_verions()->end()))
                    // match active version.
                    || (multi_versions.__isset.active_version &&
                        version == *multi_versions.get_active_version())
                    // match max_version and keep min version.
                    || (multi_versions.__isset.max_version &&
                        version <= *multi_versions.get_max_version() &&
                        keyWithNoVersion != lastKeyWithNoVersion_)
                    ) {
                    // keep
                    lastKeyWithNoVersion_ = keyWithNoVersion.str();
                    return false;
                }
                return true;
            }
        }

        if (keyWithNoVersion == lastKeyWithNoVersion_) {
            // TODO(heng): we could support max-versions configuration in schema if needed.
            return true;
        }
        lastKeyWithNoVersion_ = keyWithNoVersion.str();
        return false;
    }

    bool indexValid(GraphSpaceID spaceId, const folly::StringPiece& key) const {
        auto indexId = NebulaKeyUtils::getIndexId(key);
        auto eRet = this->indexMan_->getEdgeIndex(spaceId, indexId);
        if (eRet.ok()) {
            return true;
        }
        auto tRet = this->indexMan_->getTagIndex(spaceId, indexId);
        if (tRet.ok()) {
            return true;
        }
        return !(eRet.status() == Status::IndexNotFound() &&
                 tRet.status() == Status::IndexNotFound());
    }

private:
    mutable std::string lastKeyWithNoVersion_;
    meta::SchemaManager* schemaMan_ = nullptr;
    meta::IndexManager* indexMan_ = nullptr;
};

class StorageCompactionFilterFactory final : public kvstore::KVCompactionFilterFactory {
public:
    StorageCompactionFilterFactory(meta::SchemaManager* schemaMan,
                                   meta::IndexManager* indexMan,
                                   GraphSpaceID spaceId):
        KVCompactionFilterFactory(spaceId),
        schemaMan_(schemaMan),
        indexMan_(indexMan) {}

    std::unique_ptr<kvstore::KVFilter> createKVFilter() override {
        return std::make_unique<StorageCompactionFilter>(schemaMan_, indexMan_);
    }

    const char* Name() const override {
        return "StorageCompactionFilterFactory";
    }

private:
    meta::SchemaManager* schemaMan_ = nullptr;
    meta::IndexManager* indexMan_ = nullptr;
};

class StorageCompactionFilterFactoryBuilder : public kvstore::CompactionFilterFactoryBuilder {
public:
    StorageCompactionFilterFactoryBuilder(meta::SchemaManager* schemaMan,
                                          meta::IndexManager* indexMan)
        : schemaMan_(schemaMan)
        , indexMan_(indexMan) {}

    virtual ~StorageCompactionFilterFactoryBuilder() = default;

    std::shared_ptr<kvstore::KVCompactionFilterFactory>
    buildCfFactory(GraphSpaceID spaceId) override {
        return std::make_shared<StorageCompactionFilterFactory>(schemaMan_,
                                                                indexMan_,
                                                                spaceId);
    }

private:
    meta::SchemaManager* schemaMan_ = nullptr;
    meta::IndexManager* indexMan_ = nullptr;
};


}   // namespace storage
}   // namespace nebula
#endif   // STORAGE_COMPACTIONFILTER_H_
