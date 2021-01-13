//
// Created by Han Xinyue on 2019/10/20.
//

#ifndef PROJECT_SUBQUERY_H
#define PROJECT_SUBQUERY_H
#include "../Query.h"

class SubQuery : public ComplexQuery {
    static constexpr const char *qname = "SUB";
    Table::FieldIndex fieldId;

public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    void thread_execute(int begin, int end);
};

#endif //PROJECT_SUBQUERY_H
