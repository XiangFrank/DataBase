//
// Created by Han Xinyue on 2019/10/20.
//

#ifndef PROJECT_SWAPQUERY_H
#define PROJECT_SWAPQUERY_H
#include "../Query.h"

class SwapQuery : public ComplexQuery {
    static constexpr const char *qname = "SWAP";
    Table::FieldIndex fieldId;

public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    void thread_execute(int begin, int end);
};
#endif //PROJECT_SWAPQUERY_H
