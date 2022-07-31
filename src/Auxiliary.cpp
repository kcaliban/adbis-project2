//
// Created by fk on 28.07.22.
//

#include "Auxiliary.h"
#include <random>

std::string GetRandomString() {
    static auto& chrs = "0123456789"
                        "abcdefghijklmnopqrstuvwxyz"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    thread_local static std::mt19937 rg{std::random_device{}()};
    thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);

    std::string s;

    size_t length = 30;
    s.reserve(length);

    while(--length)
        s += chrs[pick(rg)];

    return s;
}