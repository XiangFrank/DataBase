//
// Created by Han Xinyue on 2019/10/22.
//

#ifndef PROJECT_COPYTABLEQUERY_H
#define PROJECT_COPYTABLEQUERY_H
#include "../Query.h"

class CopyTableQuery : public Query {
    static constexpr const char *qname = "COPY";
    const std::string OldTableName;
    const std::string TableName;
public:

    explicit CopyTableQuery(std::string OldTableName, std::string TableName) :
            Query(std::move(OldTableName)), OldTableName(std::move(TableName)) {}

    QueryResult::Ptr execute() override;

    std::string toString() override;

};
#endif //PROJECT_COPYTABLEQUERY_H
