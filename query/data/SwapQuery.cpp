//
// Created by Han Xinyue on 2019/10/20.
//

#include "SwapQuery.h"
#include "../../db/Database.h"

constexpr const char *SwapQuery::qname;

static pthread_mutex_t swap_lock;

static int CNT;

struct arguement{
    SwapQuery* query;
    int begin;
    int end;
};

static void* thread_test(void* arg){
    struct arguement arg_new = *(struct arguement*) arg;
    arg_new.query->thread_execute(arg_new.begin, arg_new.end);
    return NULL;
}


QueryResult::Ptr SwapQuery::execute() {
    using namespace std;
    pthread_mutex_init(&swap_lock, 0);
    CNT = 0;
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
        /*if (operands.size() == 2 && this->operands[0] == this->operands[1])
            return make_unique<RecordCountResult>(CNT);*/
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
        // if add field A to field A, then return with counter=0
        if (operands.size() == 2 && this->operands[0] == this->operands[1])
            return make_unique<RecordCountResult>(counter);

        auto &table = db[this->targetTable];
        auto result = initCondition(table);

        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    this->fieldId = table.getFieldIndex(this->operands[0]);
                    int value1 = (*it)[this->fieldId];
                    this->fieldId = table.getFieldIndex(this->operands[1]);
                    int value2 = (*it)[this->fieldId];
                    (*it)[this->fieldId] = value1;
                    this->fieldId = table.getFieldIndex(this->operands[0]);
                    (*it)[this->fieldId] = value2;
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

std::string SwapQuery::toString() {
    return "QUERY = SWAP " + this->targetTable + "\"";
}

void SwapQuery::thread_execute(int begin, int end){
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    auto &table = db[this->targetTable];
    auto result = initCondition(table);

    if (result.second) {
        for (auto it = table.begin() + begin; it != table.begin() + end; ++it) {
            if (this->evalCondition(*it)) {
                Table::FieldIndex fd;
                fd = table.getFieldIndex(this->operands[0]);
                //this->fieldId = table.getFieldIndex(this->operands[0]);
                int value1 = (*it)[fd];
                fd = table.getFieldIndex(this->operands[1]);
                //this->fieldId = table.getFieldIndex(this->operands[1]);
                int value2 = (*it)[fd];
                (*it)[fd] = value1;
                fd = table.getFieldIndex(this->operands[0]);
                (*it)[fd] = value2;
                ++counter;
            }
        }
    }
    pthread_mutex_lock(&swap_lock);
    CNT += counter;
    pthread_mutex_unlock(&swap_lock);
}

