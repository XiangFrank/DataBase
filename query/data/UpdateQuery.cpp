//
// Created by liu on 18-10-25.
//

#include "UpdateQuery.h"
#include "../../db/Database.h"

constexpr const char *UpdateQuery::qname;

static pthread_mutex_t update_lock;

static int CNT;

struct arguement{
    UpdateQuery* query;
    int begin;
    int end;
};

static void* thread_test(void* arg){
    struct arguement arg_new = *(struct arguement*) arg;
    arg_new.query->thread_execute(arg_new.begin, arg_new.end);
    return NULL;
}

QueryResult::Ptr UpdateQuery::execute() {
    using namespace std;
    CNT = 0;
    pthread_mutex_init(&update_lock, 0);
    if (this->operands.size() != 2)
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    //Table::SizeType counter = 0;
    pthread_t thread_list[4];
    struct arguement* arg;
    arg = (struct arguement*) malloc(4* sizeof(struct arguement));
    try {
        int size = db[this->targetTable].size();
        int batch_size = size/4;
        for(int i = 0; i < 4; i++){
            if(i == 3) {
                arg[i].query = this;
                arg[i].begin = i*batch_size;
                arg[i].end = size;
                pthread_create(&thread_list[i], NULL, thread_test, &arg[i]);
            }
            else{
                arg[i].query = this;
                arg[i].begin = i*batch_size;
                arg[i].end = (i+1)*batch_size;
                pthread_create(&thread_list[i], NULL, thread_test, &arg[i]);
            }
        }
        for(int i = 0; i < 4; i++){
            pthread_join(thread_list[i], NULL);
        }
    /*try {
        auto &table = db[this->targetTable];
        if (this->operands[0] == "KEY") {
            this->keyValue = this->operands[1];
        } else {
            this->fieldId = table.getFieldIndex(this->operands[0]);
            this->fieldValue = (Table::ValueType) strtol(this->operands[1].c_str(), nullptr, 10);
        }
        auto result = initCondition(table);
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    if (this->keyValue.empty()) {
                        (*it)[this->fieldId] = this->fieldValue;
                    } else {
                        it->setKey(this->keyValue);
                    }
                    ++counter;
                }
            }
        }*/
        delete[] arg;
        return make_unique<RecordCountResult>(CNT);
    } catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }
}

std::string UpdateQuery::toString() {
    return "QUERY = UPDATE " + this->targetTable + "\"";
}

void UpdateQuery::thread_execute(int begin, int end) {
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    auto &table = db[this->targetTable];
    Table::FieldIndex fd = this->fieldId;
    Table::ValueType value = this->fieldValue;
    Table::KeyType keyvalue;
    if (this->operands[0] == "KEY") {
        //this->keyValue = this->operands[1];
        keyvalue = this->operands[1];
    } else {
        fd = table.getFieldIndex(this->operands[0]);
        //this->fieldId = table.getFieldIndex(this->operands[0]);
        value = (Table::ValueType) strtol(this->operands[1].c_str(), nullptr, 10);
        //this->fieldValue = (Table::ValueType) strtol(this->operands[1].c_str(), nullptr, 10);
    }
    auto result = initCondition(table);
    if (result.second) {
        for (auto it = table.begin() + begin; it != table.begin() + end; ++it) {
            if (this->evalCondition(*it)) {
                if (keyvalue.empty()) {
                    (*it)[fd] = value;
                } else {
                    it->setKey(keyvalue);
                }
                ++counter;
            }
        }
    }
    pthread_mutex_lock(&update_lock);
    CNT += counter;
    pthread_mutex_unlock(&update_lock);
}
