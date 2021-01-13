//
// Created by Han Xinyue on 2019/10/22.
//

#include "CopyTableQuery.h"
#include "../../db/Database.h"

constexpr const char *CopyTableQuery::qname;

QueryResult::Ptr CopyTableQuery::execute() {
    using namespace std;
    Database &db = Database::getInstance();
    try {
        db.CopyTable(this->targetTable, this->OldTableName);
        return make_unique<SuccessMsgResult>(qname, targetTable);
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, e.what());
    }
}

std::string CopyTableQuery::toString() {
    return "QUERY = Copy TABLE, TABLE = \"" + this->OldTableName + "\"";
}
