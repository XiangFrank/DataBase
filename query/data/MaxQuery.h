//
// Created by Han Xinyue on 2019/10/20.
//

#ifndef PROJECT_MAXQUERY_H
#define PROJECT_MAXQUERY_H
#include "../Query.h"

class MaxQuery : public ComplexQuery {
    static constexpr const char *qname = "MAX";
    Table::FieldIndex fieldId;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    void thread_execute(int begin, int end);
};
#endif //PROJECT_MAXQUERY_H
