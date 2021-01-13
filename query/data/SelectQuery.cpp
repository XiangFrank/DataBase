//
// Created by Han Xinyue on 2019/10/20.
//

#include "SelectQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include <set>
#include <iostream>


#include <algorithm>

constexpr const char *SelectQuery::qname;

pthread_mutex_t Select_lock;

struct classCompPair_s {
    bool operator() (const std::pair<std::string,std::string>& pair_1, const std::pair<std::string, std::string>& pair_2) const
    {    return (pair_1.first < pair_2.first); }
};

static std::set<std::pair<std::string, std::string>, classCompPair_s> Set_s;

struct arguement{
    SelectQuery* query;
    int begin;
    int end;
};

static void* thread_test(void* arg){
    struct arguement arg_new = *(struct arguement*) arg;
    arg_new.query->thread_execute(arg_new.begin, arg_new.end);
    return NULL;
}

QueryResult::Ptr SelectQuery::execute() {
    using namespace std;
    Set_s.clear();
    pthread_mutex_init(&Select_lock, 0);
    if (this->operands.size() < 1)
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    std::vector<string> results;
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
            std::string record;
            auto &table = db[this->targetTable];
            auto result = initCondition(table);
            if (result.second) {
                for (auto it = table.begin(); it != table.end(); ++it) {
                    if (this->evalCondition(*it)){
                        std::string rd = (*it).key();
                        for (unsigned int i = 1; i < this->operands.size(); i++) {
                            this->fieldId = table.getFieldIndex(this->operands[i]);
                            rd += " ";
                            rd += to_string((*it)[this->fieldId]);
                        }
                        results.push_back(rd);
                    }
                }
        }
        for (unsigned int i = 0; i < results.size() - 1; i++){
            for (unsigned int j = i + 1; j < results.size(); j++)
                if (results[i] > results[j])
                    swap(results[i], results[j]);
        }*/
        auto it = Set_s.begin();
        for(; it != Set_s.end(); it++){
            results.push_back((*it).second);
        }
        return make_unique<SelectQueryResult>(results);
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


std::string SelectQuery::toString() {
    return "QUERY = SELECT " + this->targetTable + "\"";
}

void SelectQuery::thread_execute(int begin, int end) {
    Database &db = Database::getInstance();
    std::string record;
    auto &table = db[this->targetTable];
    auto result = initCondition(table);
    if (result.second) {
        for (auto it = table.begin()+begin; it != table.begin()+end; ++it) {
            if (this->evalCondition(*it)){
                std::string rd = (*it).key();
                for (unsigned int i = 1; i < this->operands.size(); i++) {
                    auto fd = table.getFieldIndex(this->operands[i]);
                    //this->fieldId = table.getFieldIndex(this->operands[i]);
                    rd += " ";
                    rd += to_string((*it)[fd]);
                }
                //std::cout<<rd<<std::endl;
                //results.push_back(rd);
                pthread_mutex_lock(&Select_lock);
                Set_s.insert(std::pair<std::string, std::string>((*it).key(),rd));
                pthread_mutex_unlock(&Select_lock);
            }
        }
    }
}