/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "parser/SequentialSentences.h"

namespace nebula {

std::string SequentialSentences::toString() const {
    std::string buf;
    buf.reserve(1024);
    auto i = 0UL;
    buf += sentences_[i++]->toString();
    for ( ; i < sentences_.size(); i++) {
        buf += "; ";
        buf += sentences_[i]->toString();
    }
    return buf;
}

void SequentialSentences::optimize(bool pushDown) {
    for (auto& sentense : sentences_) {
        auto s = sentense->optimize(pushDown);
        if (s) {
            sentense = std::move(s);
        }
    }
}

}   // namespace nebula
