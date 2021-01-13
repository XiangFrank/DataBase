//
// Created by Han Xinyue on 2019/10/20.
//

#ifndef PROJECT_SELECTQUERY_H
#define PROJECT_SELECTQUERY_H

#include "../Query.h"
class SelectQuery : public ComplexQuery {
    static constexpr const char *qname = "SELECT";
    Table::FieldIndex fieldId;

public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    void thread_execute(int begin, int end);
};

#endif //PROJECT_SELECTQUERY_H
