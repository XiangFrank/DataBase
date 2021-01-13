//
// Created by Han Xinyue on 2019/10/20.
//

#ifndef PROJECT_DELETEQUERY_H
#define PROJECT_DELETEQUERY_H
#include "../Query.h"

class DeleteQuery : public ComplexQuery {
    static constexpr const char *qname = "DELETE";

public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;

    void thread_execute(int begin, int end);
};


#endif //PROJECT_DELETEQUERY_H
