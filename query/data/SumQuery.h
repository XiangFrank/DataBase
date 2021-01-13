//
// Created by Han Xinyue on 2019/10/19.
//

#ifndef PROJECT_SUMQUERY_H
#define PROJECT_SUMQUERY_H

#include "../Query.h"

class SumQuery : public ComplexQuery {
    static constexpr const char *qname = "SUM";
    Table::FieldIndex fieldId;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    void thread_execute(int begin, int end);
};

#endif //PROJECT_SUMQUERY_H
