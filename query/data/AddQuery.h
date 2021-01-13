//
// Created by Han Xinyue on 2019/10/20.
//

#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H
#include "../Query.h"

class AddQuery : public ComplexQuery {
    static constexpr const char *qname = "ADD";
    Table::FieldIndex fieldId;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    void thread_execute(int begin, int end);
};

#endif //PROJECT_ADDQUERY_H
