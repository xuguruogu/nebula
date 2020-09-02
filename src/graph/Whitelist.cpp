//
// Created by trippli on 2020/9/2.
//

#include "Whitelist.h"

DEFINE_string(whitelist, "", "ip whitelist: 127.0.0.1,127.0.0.2");


namespace nebula {
namespace graph {

static std::unordered_set<uint32_t> gWhitelist;

void loadWhitelist() {
    gWhitelist.clear();
    std::vector<std::string> ips;
    folly::split(",", FLAGS_whitelist, ips, true);
    for (auto& ip : ips) {
        LOG(INFO) << "whitelist ip: " << ip;
        gWhitelist.insert(folly::IPAddressV4::toLong(ip));
    }
}

bool checkWhitelist(const folly::IPAddressV4& ip) {
    return gWhitelist.empty() || gWhitelist.find(ip.toLong()) != gWhitelist.end();
}

}
}