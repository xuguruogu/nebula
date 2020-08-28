/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/ScanExecutor.h"
#include "base/Base.h"
#include "time/WallClock.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace graph {

ScanExecutor::ScanExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "scan") {
    sentence_ = static_cast<ScanSentence *>(sentence);
}

Status ScanExecutor::prepare() {
    return Status::OK();
}

Status ScanExecutor::prepareClauses() {
    {
        auto status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            return status;
        }
    }
    auto space = ectx()->rctx()->session()->space();

    auto tag = sentence_->tag();

    auto tagStatus = ectx()->schemaManager()->toTagID(space, tag);
    if (!tagStatus.ok()) {
        LOG(ERROR) << "No schema found for " << tag;
        return Status::Error("No schema found for `%s'", tag.c_str());
    }
    auto tagId = tagStatus.value();
    return_columns_[tagId] = {};

    if (expCtx_ == nullptr) {
        expCtx_ = std::make_unique<ExpressionContext>();
    }

    {
        ValueList* list = sentence_->partitions();
        if (list) {
            for (auto& expr : list->values()) {
                expr->setContext(expCtx_.get());
                auto status = expr->prepare();
                if (!status.ok()) {
                    return status;
                }
            }
        }
    }
    {
        auto expr = sentence_->from();
        if (expr) {
            expr->setContext(expCtx_.get());
            auto status = expr->prepare();
            if (!status.ok()) {
                return status;
            }
        }
    }
    {
        auto expr = sentence_->latestSeconds();
        if (expr) {
            expr->setContext(expCtx_.get());
            auto status = expr->prepare();
            if (!status.ok()) {
                return status;
            }
        }
    }
    {
        auto expr = sentence_->limit();
        if (expr) {
            expr->setContext(expCtx_.get());
            auto status = expr->prepare();
            if (!status.ok()) {
                return status;
            }
        }
    }

    if (expCtx_->hasDstTagProp()
        || expCtx_->hasEdgeProp()
        || expCtx_->hasSrcTagProp()
        || expCtx_->hasVariableProp()) {
        return Status::Error("Unsupported expr.");
    }
    return Status::OK();
}

Status ScanExecutor::setup() {
    auto space = ectx()->rctx()->session()->space();
    Getters getters;
    auto func = [&] {
        {
            auto expr = sentence_->from();
            if (expr) {
                auto value = expr->eval(getters);
                if (!value.ok()) {
                    return value.status();
                }
                auto r = value.value();
                if (!Expression::isInt(r)) {
                    return Status::Error("should be of type integer");
                }
                auto vid = Expression::asInt(r);
                auto partStatus = ectx()->getStorageClient()->partId(space, vid);
                if (!partStatus.ok()) {
                    return partStatus.status();
                }
                auto partition = partStatus.value();
                cursor_ = NebulaKeyUtils::vertexPrefix(partition, vid);
                vertexPartition_ = partition;
                partitions_.emplace_back(partition);
            }
        }
        {
            auto expr = sentence_->latestSeconds();
            if (expr) {
                auto value = expr->eval(getters);
                if (!value.ok()) {
                    return value.status();
                }
                auto r = value.value();
                if (!Expression::isInt(r)) {
                    return Status::Error("should be of type integer");
                }
                int64_t latestSeconds = Expression::asInt(r);
                auto now = time::WallClock::fastNowInMicroSec();
                startTime_ = now - latestSeconds * 1000000L;
                endTime_ = now;
            } else {
                startTime_ = 0;
                endTime_ = std::numeric_limits<int64_t>::max();
            }
        }
        {
            auto expr = sentence_->limit();
            if (expr) {
                auto value = expr->eval(getters);
                if (!value.ok()) {
                    return value.status();
                }
                auto r = value.value();
                if (!Expression::isInt(r)) {
                    return Status::Error("should be of type integer");
                }
                limit_ = Expression::asInt(r);
            } else {
                limit_ = std::numeric_limits<int32_t>::max();
            }
        }
        {
            auto partnumStatus = ectx()->getStorageClient()->partsNum(space);
            if (!partnumStatus.ok()) {
                return partnumStatus.status();
            }
            auto partnum = partnumStatus.value();

            ValueList* list = sentence_->partitions();
            if (list) {
                for (auto& expr : list->values()) {
                    auto value = expr->eval(getters);
                    if (!value.ok()) {
                        return value.status();
                    }
                    auto r = value.value();
                    if (!Expression::isInt(r)) {
                        return Status::Error(
                            "should be of type integer");
                    }
                    auto partition = Expression::asInt(r);
                    if (partition > partnum) {
                        return Status::Error(
                            "partition should not larger than space partitions.");
                    }
                    if (vertexPartition_) {
                        if (vertexPartition_ != partition) {
                            return Status::Error(
                                "part [partition] not match from [vertex id's partition].");
                        }
                    } else {
                        partitions_.emplace_back(partition);
                    }
                }
            } else if (partitions_.empty()) {
                for (PartitionID partition = 1; partition <= partnum; partition++) {
                    partitions_.emplace_back(partition);
                }
            }
        }
        return Status::OK();
    };

    if (expCtx_->hasInputProp()) {
        if (inputs_ == nullptr) {
            return Status::Error("input is empty.");
        }

        auto rows = inputs_->getRows();
        if (!rows.ok()) {
            return rows.status();
        }
        if (rows.value().size() != 1) {
            return Status::Error("scan input rows size should be 1.");
        }

        auto visitor = [&] (const RowReader *reader) -> Status {
            auto schema = reader->getSchema().get();
            getters.getInputProp = [&] (const std::string &prop) -> OptVariantType {
                return Collector::getProp(schema, prop, reader);
            };
            return func();
        };
        return inputs_->applyTo(visitor);
    } else {
        return func();
    }
}

void ScanExecutor::execute() {
    {
        auto status = prepareClauses();
        if (!status.ok()) {
            LOG(ERROR) << "prepareClauses failed. " << status.toString();
            doError(status);
            return;
        }
        status = setup();
        if (!status.ok()) {
            LOG(ERROR) << "scan setup failed. " << status.toString();
            doError(status);
            return;
        }
    }

    auto space = ectx()->rctx()->session()->space();

    VLOG(1) << "ScanVertex: space:" << space << " partitions: " << folly::join(",", partitions_)
            << " startTime: " << startTime_ << " endTime: " << endTime_ << " limit: " << limit_ ;

    std::vector<folly::SemiFuture<StatusOr<storage::cpp2::ScanVertexResponse>>> results;
    results.reserve(partitions_.size());
    for (auto partition : partitions_) {
        auto future = ectx()->getStorageClient()->ScanVertex(
            space, partition, cursor_, return_columns_, true, limit_, startTime_, endTime_);
        results.emplace_back(std::move(future));
    }

    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (std::vector<folly::Try<StatusOr<storage::cpp2::ScanVertexResponse>>>&& t) {
        std::shared_ptr<ResultSchemaProvider> schema;
        {
            size_t num = 0;
            for (folly::Try<StatusOr<storage::cpp2::ScanVertexResponse>>& partitionTry : t) {
                CHECK(!partitionTry.hasException());
                StatusOr<storage::cpp2::ScanVertexResponse>& resultStatus = partitionTry.value();
                if (!resultStatus.ok()) {
                    LOG(ERROR) << "ScanVertex error " << resultStatus.status().toString();
                    doError(resultStatus.status());
                    return;
                }
                auto& result = resultStatus.value();
                if (!result.get_result().get_failed_codes().empty()) {
                    for (auto &error : result.get_result().failed_codes) {
                        LOG(INFO) << "ScanVertex failed, part: " << error.get_part_id()
                                  << " error code: " << static_cast<int>(error.get_code());
                    }
                    doError(Status::Error("ScanVertex failed."));
                    return;
                }
                if (result.get_vertex_schema().size() != 1) {
                    LOG(ERROR) << "ScanVertex return vertex_schema size != 1.";
                    doError(Status::Error("ScanVertex return vertex_schema size != 1."));
                    return;
                }
                schema = std::make_shared<ResultSchemaProvider>(
                    result.get_vertex_schema().begin()->second);
                num += result.get_vertex_data().size();
            }
            rows_.reserve(num);
        }

        colNames_.emplace_back("VertexID");
        schema_ = std::make_shared<SchemaWriter>();
        schema_->appendCol("VertexID", nebula::cpp2::SupportedType::VID);
        for (auto& iter : *schema) {
            colNames_.emplace_back(iter.getName());
            schema_->appendCol(iter.getName(), iter.getType().get_type());
        }
        colNames_.emplace_back("__version__");
        schema_->appendCol("__version__", nebula::cpp2::SupportedType::INT);
        colNames_.emplace_back("__ts__");
        schema_->appendCol("__ts__", nebula::cpp2::SupportedType::TIMESTAMP);

        for (folly::Try<StatusOr<storage::cpp2::ScanVertexResponse>>& partitionTry : t) {
            StatusOr<storage::cpp2::ScanVertexResponse>& resultStatus = partitionTry.value();
            auto& result = resultStatus.value();
            for (auto& data : result.get_vertex_data()) {
                cpp2::RowValue row;
                row.columns.reserve(colNames_.size());

                cpp2::ColumnValue vid;
                vid.set_id(data.get_vertexId());
                row.columns.emplace_back(vid);

                auto reader = RowReader::getRowReader(data.get_value(), schema);
                for (unsigned i = 0; i < schema->getNumFields(); i++) {
                    cpp2::ColumnValue col;
                    switch (schema->getFieldType(i).get_type()) {
                        case nebula::cpp2::SupportedType::BOOL:
                        {
                            bool v;
                            auto ret = reader->getBool(i, v);
                            if (ret != ResultType::SUCCEEDED) {
                                LOG(ERROR) << "ScanVertex return unsupported type.";
                                doError(Status::Error("ScanVertex return unsupported type."));
                                return;
                            }
                            col.set_bool_val(v);
                        }
                            break;
                        case nebula::cpp2::SupportedType::INT:
                        {
                            long v;
                            auto ret = reader->getInt(i, v);
                            if (ret != ResultType::SUCCEEDED) {
                                LOG(ERROR) << "ScanVertex return unsupported type.";
                                doError(Status::Error("ScanVertex return unsupported type."));
                                return;
                            }
                            col.set_integer(v);
                        }
                            break;
                        case nebula::cpp2::SupportedType::TIMESTAMP:
                        {
                            long v;
                            auto ret = reader->getInt(i, v);
                            if (ret != ResultType::SUCCEEDED) {
                                LOG(ERROR) << "ScanVertex return unsupported type.";
                                doError(Status::Error("ScanVertex return unsupported type."));
                                return;
                            }
                            col.set_timestamp(v);
                        }
                            break;
                        case nebula::cpp2::SupportedType::VID:
                        {
                            VertexID v;
                            auto ret = reader->getVid(i, v);
                            if (ret != ResultType::SUCCEEDED) {
                                LOG(ERROR) << "ScanVertex return unsupported type.";
                                doError(Status::Error("ScanVertex return unsupported type."));
                                return;
                            }
                            col.set_id(v);
                        }
                            break;
                        case nebula::cpp2::SupportedType::FLOAT:
                        {
                            float v;
                            auto ret = reader->getFloat(i, v);
                            if (ret != ResultType::SUCCEEDED) {
                                LOG(ERROR) << "ScanVertex return unsupported type.";
                                doError(Status::Error("ScanVertex return unsupported type."));
                                return;
                            }
                            col.set_single_precision(v);
                        }
                            break;
                        case nebula::cpp2::SupportedType::DOUBLE:
                        {
                            double v;
                            auto ret = reader->getDouble(i, v);
                            if (ret != ResultType::SUCCEEDED) {
                                LOG(ERROR) << "ScanVertex return unsupported type.";
                                doError(Status::Error("ScanVertex return unsupported type."));
                                return;
                            }
                            col.set_double_precision(v);
                        }
                            break;
                        case nebula::cpp2::SupportedType::STRING:
                        {
                            folly::StringPiece v;
                            auto ret = reader->getString(i, v);
                            if (ret != ResultType::SUCCEEDED) {
                                LOG(ERROR) << "ScanVertex return unsupported type.";
                                doError(Status::Error("ScanVertex return unsupported type."));
                                return;
                            }
                            col.set_str(v);
                        }
                            break;
                        case nebula::cpp2::SupportedType::YEAR:
                        case nebula::cpp2::SupportedType::YEARMONTH:
                        case nebula::cpp2::SupportedType::DATE:
                        case nebula::cpp2::SupportedType::DATETIME:
                        case nebula::cpp2::SupportedType::PATH:
                        case nebula::cpp2::SupportedType::UNKNOWN:
                            LOG(ERROR) << "ScanVertex return unsupported type.";
                            doError(Status::Error("ScanVertex return unsupported type."));
                            break;
                    }
                    row.columns.emplace_back(col);
                }
                int64_t version = data.get_version();
                {
                    cpp2::ColumnValue col;
                    col.set_integer(version);
                    row.columns.emplace_back(col);
                }
                {
                    cpp2::ColumnValue col;
                    col.set_timestamp((std::numeric_limits<int64_t>::max() - version) / 1000000);
                    row.columns.emplace_back(col);
                }
                rows_.emplace_back(std::move(row));
            }
        }

        if (onResult_) {
            auto status = setupInterimResult();
            if (!status.ok()) {
                DCHECK(onError_);
                onError_(std::move(status).status());
                return;
            }
            onResult_(std::move(status).value());
        }

        doFinish(Executor::ProcessControl::kNext);
    };
    auto error = [this](auto &&e) {
        LOG(ERROR) << "Exception when handle scan: " << e.what();
        doError(Status::Error("Exeception when handle scan: %s.", e.what().c_str()));
    };

    folly::collectAll(results).via(runner).thenValue(cb).thenError(error);
}

StatusOr<std::unique_ptr<InterimResult>> ScanExecutor::setupInterimResult() {
    auto result = std::make_unique<InterimResult>(std::move(colNames_));
    if (rows_.empty()) {
        return result;
    }

    auto rsWriter = std::make_unique<RowSetWriter>(schema_);
    using Type = cpp2::ColumnValue::Type;
    for (auto &row : rows_) {
        RowWriter writer(schema_);
        auto columns = row.get_columns();
        for (auto &column : columns) {
            switch (column.getType()) {
                case cpp2::ColumnValue::Type::id:
                    writer << column.get_id();
                    break;
                case Type::integer:
                    writer << column.get_integer();
                    break;
                case Type::double_precision:
                    writer << column.get_double_precision();
                    break;
                case Type::bool_val:
                    writer << column.get_bool_val();
                    break;
                case Type::str:
                    writer << column.get_str();
                    break;
                case cpp2::ColumnValue::Type::timestamp:
                    writer << column.get_timestamp();
                    break;
                default:
                    LOG(ERROR) << "Not Support type: " << column.getType();
                    return Status::Error("Not Support type: %d", static_cast<int>(column.getType()));
            }
        }
        rsWriter->addRow(writer);
    }

    if (rsWriter != nullptr) {
        result->setInterim(std::move(rsWriter));
    }
    return result;
}

void ScanExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp.set_column_names(std::move(colNames_));

    if (rows_.empty()) {
        return;
    }
    resp.set_rows(std::move(rows_));
}
}   // namespace graph
}   // namespace nebula
