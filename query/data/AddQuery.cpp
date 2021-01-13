//
// Created by Han Xinyue on 2019/10/20.
//
#include "AddQuery.h"
#include "../../db/Database.h"

constexpr const char *AddQuery::qname;

static pthread_mutex_t add_lock;

static int CNT;

struct arguement{
    AddQuery* query;
    int begin;
    int end;
};

static void* thread_test(void* arg){
    struct arguement arg_new = *(struct arguement*) arg;
    arg_new.query->thread_execute(arg_new.begin, arg_new.end);
    return NULL;
}

QueryResult::Ptr AddQuery::execute() {
    using namespace std;
    CNT = 0;
    pthread_mutex_init(&add_lock, 0);
    if (this->operands.size() < 2)
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
        auto result = initCondition(table);

        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    if (operands.size() == 2 && this->operands[0] == this->operands[1]){
                        ++counter;
                    }else{
                        int sum = 0;
                        for (unsigned int i = 0; i < operands.size()-1; i++){
                            this->fieldId = table.getFieldIndex(this->operands[i]);
                            int value = (*it)[this->fieldId];
                            sum += value;
                        }
                        this->fieldId = table.getFieldIndex(this->operands[operands.size()-1]);
                        (*it)[this->fieldId] = sum;
                        ++counter;
                    }
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

std::string AddQuery::toString() {
    return "QUERY = ADD " + this->targetTable + "\"";
}

void AddQuery::thread_execute(int begin, int end) {
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    auto &table = db[this->targetTable];
    auto result = initCondition(table);
    if (result.second) {
        for (auto it = table.begin() + begin; it != table.begin() + end; ++it) {
            if (this->evalCondition(*it)) {
                if (operands.size() == 2 && this->operands[0] == this->operands[1]) {
                    ++counter;
                } else {
                    int sum = 0;
                    for (unsigned int i = 0; i < operands.size() - 1; i++) {
                        Table::FieldIndex fd;
                        fd = table.getFieldIndex(this->operands[i]);
                        //this->fieldId = table.getFieldIndex(this->operands[i]);
                        int value = (*it)[fd];
                        sum += value;
                    }
                    Table::FieldIndex fd;
                    //this->fieldId = table.getFieldIndex(this->operands[operands.size() - 1]);
                    fd = table.getFieldIndex(this->operands[operands.size() - 1]);
                    (*it)[fd] = sum;
                    ++counter;
                }
            }
        }
    }
    pthread_mutex_lock(&add_lock);
    CNT += counter;
    pthread_mutex_unlock(&add_lock);
}
