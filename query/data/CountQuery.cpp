//
// Created by Han Xinyue on 2019/10/20.
//

#include "CountQuery.h"
#include "../../db/Database.h"
#include <iostream>

constexpr const char *CountQuery::qname;

static pthread_mutex_t total_lock;

static int total;

struct arguement{
    CountQuery* query;
    int begin;
    int end;
};

static void* thread_test(void* arg){
    struct arguement arg_new = *(struct arguement*) arg;
    arg_new.query->thread_execute(arg_new.begin, arg_new.end);
    return NULL;
}

QueryResult::Ptr CountQuery::execute() {
    total = 0;
    pthread_mutex_init(&total_lock, 0);
    pthread_t thread_list[4];
    using namespace std;
    if (this->operands.size() != 0)
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    //int counter = 0;
    struct arguement* arg;
    arg = (struct arguement*) malloc(4* sizeof(struct arguement));
    try {
        auto& table = db[this->targetTable];
        if (table.checkByIndex("r3306_copy")){
            //std::cout<<this->toString() << "here !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" <<endl;
        }
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
        int result = total;
        total = 0;
        delete[] arg;
        return make_unique<AnswerQueryResult>(result);
        /*auto &table = db[this->targetTable];
        auto result = initCondition(table);
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it))
                    ++counter;
            }
        }
        return make_unique<AnswerQueryResult>(counter);*/
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

std::string CountQuery::toString() {
    return "QUERY = COUNT " + this->targetTable + "\"";
}

void CountQuery::thread_execute(int begin, int end) {
    Database &db = Database::getInstance();
    int counter = 0;
    auto &table = db[this->targetTable];
    auto result = initCondition(table);
    if (result.second) {
        for (auto it = table.begin()+begin; it != table.begin()+end; ++it) {
            if (this->evalCondition(*it)) {
                //pthread_mutex_lock(&total_lock);
                ++counter;
                //std::cout<<(*it).key() << std::endl;
                //pthread_mutex_unlock(&total_lock);
            }

        }
    }
    pthread_mutex_lock(&total_lock);
    total += counter;
    pthread_mutex_unlock(&total_lock);
}

