//
// Created by Han Xinyue on 2019/10/19.
//
#include "SumQuery.h"
#include "../../db/Database.h"

constexpr const char *SumQuery::qname;

static pthread_mutex_t sum_lock;

static std::vector<int> results;

struct arguement{
    SumQuery* query;
    int begin;
    int end;
};

static void* thread_test(void* arg){
    struct arguement arg_new = *(struct arguement*) arg;
    arg_new.query->thread_execute(arg_new.begin, arg_new.end);
    return NULL;
}

QueryResult::Ptr SumQuery::execute() {
    using namespace std;
    results.clear();
    for (unsigned int i = 0; i < this->operands.size(); i++){
        results.push_back(0);
    }
    pthread_mutex_init(&sum_lock, 0);
    if (this->operands.size() < 1)
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    //std::vector<int> results;
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
        /*for (unsigned int i = 0; i < this->operands.size(); i++){
            int sum = 0;
            auto &table = db[this->targetTable];
            this->fieldId = table.getFieldIndex(this->operands[i]);
                //this->fieldValue = (Table::ValueType) strtol(this->operands[i].c_str(), nullptr, 10);
            auto result = initCondition(table);
            if (result.second) {
                for (auto it = table.begin(); it != table.end(); ++it) {
                    if (this->evalCondition(*it))
                        sum += (*it)[this->fieldId];
                    }
                }
            results.push_back(sum);
            }*/
        delete[] arg;
        return make_unique<AnswerQueryResult>(results);
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

std::string SumQuery::toString() {
    return "QUERY = SUM " + this->targetTable + "\"";
}

void SumQuery::thread_execute(int begin, int end) {
    Database &db = Database::getInstance();
    int* sum = (int*) malloc(this->operands.size()* sizeof(int));
    for (unsigned int i = 0; i < this->operands.size(); i++){
        sum[i] = 0;
    }
    auto &table = db[this->targetTable];
    //this->fieldId = table.getFieldIndex(this->operands[i]);
    //this->fieldValue = (Table::ValueType) strtol(this->operands[i].c_str(), nullptr, 10);
    auto result = initCondition(table);
    if (result.second) {
        for (auto it = table.begin()+begin; it != table.begin()+end; ++it) {
            if (this->evalCondition(*it))
                for (unsigned int i = 0; i < this->operands.size(); i++) {
                    Table::FieldIndex fd;
                    //this->fieldId = table.getFieldIndex(this->operands[i]);
                    fd = table.getFieldIndex(this->operands[i]);
                    sum[i] += (*it)[fd];
                }
        }
    }
    pthread_mutex_lock(&sum_lock);
    for (unsigned int i = 0; i < this->operands.size(); i++) {
        results[i] += sum[i];
    }
    pthread_mutex_unlock(&sum_lock);
}

