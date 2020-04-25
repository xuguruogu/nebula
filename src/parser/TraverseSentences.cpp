/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "parser/TraverseSentences.h"

namespace nebula {

std::string GoSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "GO ";
    if (stepClause_ != nullptr) {
        buf += stepClause_->toString();
    }
    if (fromClause_ != nullptr) {
        buf += " ";
        buf += fromClause_->toString();
    }
    if (overClause_ != nullptr) {
        buf += " ";
        buf += overClause_->toString();
    }
    if (whereClause_ != nullptr) {
        buf += " ";
        buf += whereClause_->toString();
    }
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }
    if (sentenses_.empty()) {
        if (orderBySentense_ && limitSentense_) {
            buf += " | ";
            buf += orderBySentense_->toString();
            buf += " | ";
            buf += limitSentense_->toString();
        }
    } else {
        for (auto& s : sentenses_) {
            buf += " | ";
            buf += s->toString();
        }
    }
    return buf;
}


const OrderByClause* GoSentence::layerOrderByClause() const {
    return orderBySentense_->orderBy();
}

const LimitClause* GoSentence::layerLimitClause() const {
    return limitSentense_->limit();
}

void GoSentence::optimizeWith(std::unique_ptr<OrderBySentence> order_by_sentense, std::unique_ptr<LimitSentence> limit_sentense) {
    orderBySentense_ = std::move(order_by_sentense);
    limitSentense_ = std::move(limit_sentense);
    kind_ = Kind::kGoWholePushDown;
}

void GoSentence::optimizeWith(std::vector<std::unique_ptr<Sentence>> sentenses) {
    sentenses_ = std::move(sentenses);
    kind_ = Kind::kGoWholePushDownV2;
}

bool GoSentence::canWholePushDown() const {
    // where
    if (whereClause_ && !whereClause_->filter()->canWholePushDown()) {
        return false;
    }

    // yield
    if (yieldClause_) {
        for (auto col : yieldClause_->columns()) {
            auto expr = col->expr();
            if (!expr->canWholePushDown()) {
                return false;
            }
        }
    }
    return true;
}

std::string MatchSentence::toString() const {
    return "MATCH sentence";
}

std::string LookupSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "LOOKUP ON ";
    buf += *from_;
    if (whereClause_ != nullptr) {
        buf += " ";
        buf += whereClause_->toString();
    }
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }
    return buf;
}

std::string UseSentence::toString() const {
    return "USE " + *space_;
}

std::string SetSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "(";
    buf += left_->toString();
    switch (op_) {
        case UNION:
            buf += " UNION ";
            break;
        case INTERSECT:
            buf += " INTERSECT ";
            break;
        case MINUS:
            buf += " MINUS ";
            break;
    }
    buf += right_->toString();
    buf += ")";
    return buf;
}

std::unique_ptr<Sentence> SetSentence::optimize(bool pushDown) {
    auto left = left_->optimize(pushDown);
    auto right = right_->optimize(pushDown);
    if (left) {
        left_ = std::move(left);
    }
    if (right) {
        right_ = std::move(right);
    }
    return nullptr;
}

static bool __contains_go(PipedSentence* piped_sentence) {
    if (piped_sentence->left()->kind() == Sentence::Kind::kGo ||
        piped_sentence->right()->kind() == Sentence::Kind::kGo) {
        return true;
    }

    if (piped_sentence->left()->kind() == Sentence::Kind::kPipe) {
        return __contains_go(static_cast<PipedSentence*>(piped_sentence->left()));
    }

    return false;
}

static bool __is_start_with_go(PipedSentence* piped_sentence) {
    if (piped_sentence->right()->kind() == Sentence::Kind::kGo) {
        return false;
    }
    if (piped_sentence->left()->kind() == Sentence::Kind::kGo) {
        return true;
    }
    if (piped_sentence->left()->kind() == Sentence::Kind::kPipe) {
        return __is_start_with_go(static_cast<PipedSentence*>(piped_sentence->left()));
    }

    return false;
}


static std::unique_ptr<Sentence> __optimize_start_with_go(
    PipedSentence* piped_sentence, std::vector<std::unique_ptr<Sentence>>&& sentences) {
    CHECK(__is_start_with_go(piped_sentence)) << "should be start with go.";
    sentences.insert(sentences.begin(), std::move(piped_sentence->right_));
    if (piped_sentence->left()->kind() == Sentence::Kind::kGo) {
        static_cast<GoSentence*>(piped_sentence->left())->optimizeWith(std::move(sentences));
        return std::move(piped_sentence->left_);
    }

    CHECK(piped_sentence->left()->kind() == Sentence::Kind::kPipe) << "should be pipe sentence.";
    return __optimize_start_with_go(static_cast<PipedSentence*>(piped_sentence->left()), std::move(sentences));
}

static std::unique_ptr<Sentence> __optimize_pipe_with_go(
    PipedSentence* piped_sentence, std::vector<std::unique_ptr<Sentence>>&& sentences) {
    CHECK(!__is_start_with_go(piped_sentence)) << "should be pipe with go.";
    CHECK(piped_sentence->right()->kind() != Sentence::Kind::kGo) << "";
    sentences.insert(sentences.begin(), std::move(piped_sentence->right_));
    CHECK(piped_sentence->left()->kind() == Sentence::Kind::kPipe) << "should be pipe sentence.";
    auto left = static_cast<PipedSentence*>(piped_sentence->left());
    if (left->right()->kind() == Sentence::Kind::kGo) {
        static_cast<GoSentence*>(left->right())->optimizeWith(std::move(sentences));
        return std::move(piped_sentence->left_);
    }

    return __optimize_pipe_with_go(left, std::move(sentences));
}

static std::unique_ptr<Sentence> __optimize_go(PipedSentence* piped_sentence) {
    CHECK(__contains_go(piped_sentence)) << "";
    std::vector<std::unique_ptr<Sentence>> sentences;
    if (__is_start_with_go(piped_sentence)) {
        return __optimize_start_with_go(piped_sentence, std::move(sentences));
    } else {
        return __optimize_pipe_with_go(piped_sentence, std::move(sentences));
    }
}

static const GoSentence* __locate_optimize_go(Sentence* sentence) {
    switch (sentence->kind()) {
        case Sentence::Kind::kGo:
            return static_cast<const GoSentence*>(sentence);
        case Sentence::Kind::kPipe:
            break;
        default:
            return nullptr;
    }

  auto piped_sentence = static_cast<const PipedSentence*>(sentence);
    switch (piped_sentence->right()->kind()) {
        case Sentence::Kind::kGo:
            return static_cast<const GoSentence*>(piped_sentence->right());
        case Sentence::Kind::KGroupBy:
        case Sentence::Kind::kOrderBy:
        case Sentence::Kind::kYield:
            break;
        default:
            return nullptr;
    }

    switch (piped_sentence->left()->kind()) {
        case Sentence::Kind::kGo:
            return static_cast<const GoSentence*>(piped_sentence->left());
        case Sentence::Kind::kPipe:
            return __locate_optimize_go(static_cast<PipedSentence*>(piped_sentence->left()));
        default:
            return nullptr;
    }
}


std::unique_ptr<Sentence> PipedSentence::optimize(bool pushDown) {
    if (pushDown) {
//        // go ... | order by ... | limit ...
//        if (left()->kind() == Sentence::Kind::kPipe &&
//            right()->kind() == Sentence::Kind::kLimit &&
//            static_cast<PipedSentence*>(left())->left()->kind() == Sentence::Kind::kGo &&
//            static_cast<PipedSentence*>(left())->right()->kind() == Sentence::Kind::kOrderBy) {
//            auto p_go_sentense = static_cast<GoSentence*>(static_cast<PipedSentence*>(left())->left_.get());
////        auto p_order_by_sentense = static_cast<OrderBySentence*>(static_cast<PipedSentence*>(left())->right_.get());
////        auto p_limit_sentense = static_cast<LimitSentence*>(right_.get());
//
//            if (p_go_sentense->overClause()->direction() == OverClause::Direction::kForward &&
//                p_go_sentense->canWholePushDown() &&
//                (p_go_sentense->stepClause() == nullptr || p_go_sentense->stepClause()->steps() == 1)) {
//
//                std::unique_ptr<GoSentence> go_sentense(static_cast<GoSentence*>(static_cast<PipedSentence*>(left())->left_.release()));
//                std::unique_ptr<OrderBySentence> order_by_sentense(static_cast<OrderBySentence*>(static_cast<PipedSentence*>(left())->right_.release()));
//                std::unique_ptr<LimitSentence> limit_sentense(static_cast<LimitSentence*>(right_.release()));
//
//                go_sentense->optimizeWith(std::move(order_by_sentense), std::move(limit_sentense));
//                return std::move(go_sentense);
//            }
//        }

//        // ... | go ... | order by ... | limit ...
//        if (left()->kind() == Sentence::Kind::kPipe &&
//            right()->kind() == Sentence::Kind::kLimit &&
//            static_cast<PipedSentence*>(left())->left()->kind() == Sentence::Kind::kPipe &&
//            static_cast<PipedSentence*>(left())->right()->kind() == Sentence::Kind::kOrderBy &&
//            static_cast<PipedSentence*>(static_cast<PipedSentence*>(left())->left())->right()->kind() == Sentence::Kind::kGo) {
//            auto p_go_sentense = static_cast<GoSentence*>(static_cast<PipedSentence*>(static_cast<PipedSentence*>(left())->left())->right());
////        auto p_order_by_sentense = static_cast<OrderBySentence*>(static_cast<PipedSentence*>(left())->right_.get());
////        auto p_limit_sentense = static_cast<LimitSentence*>(right_.get());
//
//            if (p_go_sentense->overClause()->direction() == OverClause::Direction::kForward &&
//                p_go_sentense->canWholePushDown() &&
//                (p_go_sentense->stepClause() == nullptr || p_go_sentense->stepClause()->steps() == 1)) {
//                std::unique_ptr<OrderBySentence> order_by_sentense(static_cast<OrderBySentence*>(static_cast<PipedSentence*>(left())->right_.release()));
//                std::unique_ptr<LimitSentence> limit_sentense(static_cast<LimitSentence*>(right_.release()));
//                p_go_sentense->optimizeWith(std::move(order_by_sentense), std::move(limit_sentense));
//                return std::move(static_cast<PipedSentence*>(left())->left_);
//            }
//        }

        // [... | ] go ... [ | order by | yield | group by ] | limit ...
        {
            if (right()->kind() == Sentence::Kind::kLimit) {
                auto p_go_sentense = __locate_optimize_go(left());
                if (p_go_sentense &&
                    p_go_sentense->overClause()->direction() == OverClause::Direction::kForward &&
                    p_go_sentense->canWholePushDown() &&
                    (p_go_sentense->stepClause() == nullptr || p_go_sentense->stepClause()->steps() == 1)) {
                    return __optimize_go(this);
                }
            }
        }
    }

    // order by ... | limit ...

    // ... | order by ... | limit ...

    auto left = left_->optimize(pushDown);
    auto right = right_->optimize(pushDown);
    if (left) {
        left_ = std::move(left);
    }
    if (right) {
        right_ = std::move(right);
    }

    return nullptr;
}

std::string PipedSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += left_->toString();
    buf += " | ";
    buf += right_->toString();
    return buf;
}

std::string PushdownSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += " pushdown( ";
    buf += sentence_->toString();
    buf += ") ";
    return buf;
}

std::string AssignmentSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "$";
    buf += *variable_;
    buf += " = ";
    buf += sentence_->toString();
    return buf;
}

std::unique_ptr<Sentence> AssignmentSentence::optimize(bool pushDown) {
    auto sentence = sentence_->optimize(pushDown);
    if (sentence) {
        sentence_ = std::move(sentence);
    }
    return nullptr;
}

std::string OrderBySentence::toString() const {
    return orderByClause_->toString();
}

std::string FetchVerticesSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FETCH PROP ON ";
    buf += *tag_;
    buf += " ";
    if (isRef()) {
        buf += vidRef_->toString();
    } else {
        buf += vidList_->toString();
    }
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }
    return buf;
}

std::string EdgeKey::toString() const {
    return folly::stringPrintf("%s->%s@%ld,",
            srcid_->toString().c_str(), dstid_->toString().c_str(), rank_);
}

std::string EdgeKeys::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &key : keys_) {
        buf += key->toString();
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }

    return buf;
}

std::string* EdgeKeyRef::srcid() {
    if (isInputExpr_) {
        auto *iSrcExpr = static_cast<InputPropertyExpression*>(srcid_.get());
        auto *colname = iSrcExpr->prop();
        return colname;
    } else {
        auto *vSrcExpr = static_cast<VariablePropertyExpression*>(srcid_.get());
        auto *var = vSrcExpr->alias();
        auto *colname = vSrcExpr->prop();
        uniqVar_.emplace(*var);
        return colname;
    }
}

std::string* EdgeKeyRef::dstid() {
    if (isInputExpr_) {
        auto *iDstExpr = static_cast<InputPropertyExpression*>(dstid_.get());
        auto *colname = iDstExpr->prop();
        return colname;
    } else {
        auto *vDstExpr = static_cast<VariablePropertyExpression*>(dstid_.get());
        auto *var = vDstExpr->alias();
        auto *colname = vDstExpr->prop();
        uniqVar_.emplace(*var);
        return colname;
    }
}

std::string* EdgeKeyRef::rank() {
    if (rank_ == nullptr) {
        return nullptr;
    }

    if (isInputExpr_) {
        auto *iRankExpr = static_cast<InputPropertyExpression*>(rank_.get());
        auto *colname = iRankExpr->prop();
        return colname;
    } else {
        auto *vRankExpr = static_cast<VariablePropertyExpression*>(rank_.get());
        auto *var = vRankExpr->alias();
        auto *colname = vRankExpr->prop();
        uniqVar_.emplace(*var);
        return colname;
    }
}

StatusOr<std::string> EdgeKeyRef::varname() const {
    std::string result = "";
    if (isInputExpr_) {
        return result;
    }

    if (uniqVar_.size() != 1) {
        return Status::SyntaxError("Near %s, Only support single data source.", toString().c_str());
    }

    for (auto &var : uniqVar_) {
        result = std::move(var);
    }

    return result;
}

std::string EdgeKeyRef::toString() const {
    std::string buf;
    buf.reserve(256);
    if (srcid_ != nullptr) {
        buf += srcid_->toString();
    }
    if (dstid_ != nullptr) {
        buf += "->";
        buf += dstid_->toString();
    }
    if (rank_ != nullptr) {
        buf += "@";
        buf += rank_->toString();
    }
    return buf;
}

std::string FetchEdgesSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FETCH PROP ON ";
    buf += *edge_;
    buf += " ";
    if (isRef()) {
        buf += keyRef_->toString();
    } else {
        buf += edgeKeys_->toString();
    }
    return buf;
}

std::string GroupBySentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "GROUP BY";
    if (groupClause_ != nullptr) {
        buf += " ";
        buf += groupClause_->toString();
    }
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }
    return buf;
}

std::string FindPathSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FIND ";
    if (isShortest_) {
        buf += "SHORTEST PATH ";
    } else {
        buf += "ALL PATH ";
    }

    if (from_ != nullptr) {
        buf += from_->toString();
        buf += " ";
    }
    if (to_ != nullptr) {
        buf += to_->toString();
        buf += " ";
    }
    if (over_ != nullptr) {
        buf += over_->toString();
        buf += " ";
    }
    if (step_ != nullptr) {
        buf += step_->toString();
        buf += " ";
    }
    if (where_ != nullptr) {
        buf += where_->toString();
        buf += " ";
    }
    return buf;
}

std::string LimitSentence::toString() const {
    return limitClause_->toString();
}

std::string YieldSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += yieldClause_->toString();
    if (whereClause_ != nullptr) {
        buf += " ";
        buf += whereClause_->toString();
    }
    return buf;
}
}   // namespace nebula
