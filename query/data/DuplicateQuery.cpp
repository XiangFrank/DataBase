//
// Created by Han Xinyue on 2019/10/21.
//

#include <iostream>
#include <cstring>
#include "DuplicateQuery.h"
#include "../../db/Database.h"
#include "InsertQuery.h"
#include <set>

struct classCompPair {
    bool operator() (const std::pair<int,std::vector<Table::ValueType> >& pair_1, const std::pair<int, std::vector<Table::ValueType >>& pair_2) const
    {    return (pair_1.first < pair_2.first); }
};

struct classCompPair_k {
    bool operator() (const std::pair<int,Table::KeyType> & pair_1, const std::pair<int, Table::KeyType >& pair_2) const
    {    return (pair_1.first < pair_2.first); }
};


constexpr const char *DuplicateQuery::qname;

static std::set<std::pair<int, std::vector<Table::ValueType> >, classCompPair> Set_v;

static std::set<std::pair<int, Table::KeyType>, classCompPair_k> Set_k;

static pthread_mutex_t set_lock;

static int CNT;

struct arguement{
    DuplicateQuery* query;
    int begin;
    int end;
};

/*static void* thread_test(void* arg){
    struct arguement arg_new = *(struct arguement*) arg;
    arg_new.query->thread_execute(arg_new.begin, arg_new.end);
    return NULL;
}*/

QueryResult::Ptr DuplicateQuery::execute() {
    using namespace std;
    Set_k.clear();
    Set_v.clear();
    CNT = 0;
    pthread_mutex_init(&set_lock, 0);
    if (this->operands.size() > 0)
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    //pthread_t thread_list[4];
    //struct arguement* arg;
    //arg = (struct arguement*) malloc(4* sizeof(struct arguement));
    /*try {
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
        }*/
    try {
             auto &table = db[this->targetTable];
             if (table.checkByIndex("r3306_copy")){
                 std::cerr<<this->toString() <<endl;
             }
             auto result = initCondition(table);
             vector<Table::KeyType> keys;
             vector<vector<Table::ValueType> > data;
             //set<Table::KeyType> key_na;
             //key_na.clear();
             if (result.second) {
                 for (auto it = table.begin(); it != table.end(); it++) {
                     if (this->evalCondition(*it)){
                         Table::KeyType key = (*it).key().append("_copy");
                         //if (table[key] != NULL)
                         /*if (key == "r3306_copy"){
                             printf("%d\n",1);
                             if (table.checkByIndex("r3306_copy")){
                                 printf("%d\n",1);
                             }

                         }*/
                         if (table.checkByIndex(key))
                             //key += "_copy";
                             continue;
                         std::vector<Table::ValueType> tuple;
                         Table::SizeType fieldCount = table.field().size();
                         tuple.reserve(fieldCount - 1);
                         for (Table::SizeType i = 0; i < fieldCount; i++) {
                             Table::ValueType value = (*it)[i];
                             tuple.emplace_back(value);
                         }
                         /*if(key_na.find(key) == key_na.end()) {
                             key_na.insert(key);
                             keys.emplace_back(key);
                             data.emplace_back(tuple);
                             counter++;
                         }*/
                         keys.emplace_back(key);
                         data.emplace_back(tuple);
                         counter++;
                     }
                 }
                 CNT = counter;
                 for (unsigned int i = 0; i < counter; i++) {
                     /*if(table.checkByIndex(keys[i])) {
                         CNT--;
                     }*/
                         /*if(keys[i] == "r3306_copy"){
                             printf("%d\n",1);
                         }*/
                         /*if(table.checkByIndex("r3306_copy")){
                             printf("%d\n",1);
                         }*/
                     table.insertByIndex(keys[i], std::move(data[i]));
                 }
             }
        /*auto &table = db[this->targetTable];
        auto it_k = Set_k.begin();
        auto it_v = Set_v.begin();
        for(; it_k != Set_k.end(); it_k++, it_v++){
            vector<Table::ValueType> tuple = (*it_v).second;
            table.insertByIndex((*it_k).second, std::move(tuple));
        }*/
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

std::string DuplicateQuery::toString() {
    return "QUERY = DUPLICATE " + this->targetTable + "\"";
}

void DuplicateQuery::thread_execute(int begin, int end){
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    auto &table = db[this->targetTable];
    auto result = initCondition(table);
    std::vector<Table::KeyType> keys;
    std::vector<std::vector<Table::ValueType> > data;
    int cnt = 0;
    if (result.second) {
        for (auto it = table.begin()+begin; it != table.begin()+end; ++it, cnt++) {
            if (this->evalCondition(*it)) {
                auto temp = (*it).key();
                Table::KeyType key = (*it).key() + "_copy";
                if (table[key] != nullptr)
                //if (table.checkByIndex(key))
                    continue;
                    //key += "_copy";
                std::vector<Table::ValueType> tuple;
                Table::SizeType fieldCount = table.field().size();
                tuple.reserve(fieldCount - 1);
                for (Table::SizeType i = 0; i < fieldCount; i++) {
                    Table::ValueType value = (*it)[i];
                    tuple.emplace_back(value);
                }
                pthread_mutex_lock(&set_lock);
                //auto i = Set_k.begin();
                /*for(; i != Set_k.end(); i++){
                    if((*i).second == key) break;
                }*/
                Set_k.insert(std::pair<int, Table::KeyType>(cnt + begin, key));
                Set_v.insert(std::pair<int, std::vector<Table::ValueType >>(cnt + begin, tuple));
                CNT++;
                /*if(i == Set_k.end()) {
                    Set_k.insert(std::pair<int, Table::KeyType>(cnt + begin, key));
                    Set_v.insert(std::pair<int, std::vector<Table::ValueType >>(cnt + begin, tuple));
                    CNT++;
                }*/
                pthread_mutex_unlock(&set_lock);
                //keys.emplace_back(key);
                //data.emplace_back(tuple);
                counter++;
            }
        }
        //for (unsigned int i = 0; i < counter; i++)
           // table.insertByIndex(keys[i], std::move(data[i]));
    }
}

