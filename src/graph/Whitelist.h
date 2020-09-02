//
// Created by trippli on 2020/9/2.
//

#ifndef NEBULA_GRAPH_WHITELIST_H
#define NEBULA_GRAPH_WHITELIST_H

#include "base/Base.h"
#include "folly/IPAddressV4.h"

DECLARE_string(whitelist);

namespace nebula {
namespace graph {

void loadWhitelist();

bool checkWhitelist(const folly::IPAddressV4& ip);

}
}


#endif   // NEBULA_GRAPH_WHITELIST_H
