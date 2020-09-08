//
// Created by trippli on 2020/9/9.
//

#ifndef NEBULA_GRAPH_TO_STRING_H
#define NEBULA_GRAPH_TO_STRING_H

#include <set>
#include <array>
#include <deque>
#include <vector>
#include <string>
#include <memory>
#include <sstream>
#include <unordered_set>

namespace nebula {
namespace to_string_detail {

template <typename Iterator>
static inline std::string join(std::string delimiter, Iterator begin, Iterator end) {
    std::ostringstream oss;
    while (begin != end) {
        oss << *begin;
        ++begin;
        if (begin != end) {
            oss << delimiter;
        }
    }
    return oss.str();
}

template <typename PrintableRange>
static inline std::string join(std::string delimiter, const PrintableRange& items) {
    return join(delimiter, items.begin(), items.end());
}

}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::unordered_set<T>& items) {
    os << "{" << to_string_detail::join(", ", items) << "}";
    return os;
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::set<T>& items) {
    os << "{" << to_string_detail::join(", ", items) << "}";
    return os;
}

template <typename T, size_t N>
std::ostream& operator<<(std::ostream& os, const std::array<T, N>& items) {
    os << "{" << to_string_detail::join(", ", items) << "}";
    return os;
}

template <typename Printable>
std::ostream& operator<<(std::ostream& os, const std::vector<Printable>& items) {
    os << "{" << to_string_detail::join(", ", items) << "}";
    return os;
}

template <typename Printable>
std::ostream& operator<<(std::ostream& os, const std::deque<Printable>& items) {
    os << "{" << to_string_detail::join(", ", items) << "}";
    return os;
}

template <typename Printable>
std::ostream& operator<<(std::ostream& os, const std::shared_ptr<Printable>& items) {
    os << *items;
    return os;
}
}

#endif   // NEBULA_GRAPH_TO_STRING_H
