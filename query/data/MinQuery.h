//
// Created by Han Xinyue on 2019/10/20.
//

#ifndef PROJECT_MINQUERY_H
#define PROJECT_MINQUERY_H

#include "../Query.h"

class MinQuery : public ComplexQuery {
    static constexpr const char *qname = "MIN";
    Table::FieldIndex fieldId;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    void thread_execute(int begin, int end);
};

#endif //PROJECT_MINQUERY_H
