//
// Created by Han Xinyue on 2019/10/20.
//

#include "DeleteQuery.h"
#include "../../db/Database.h"
#include <algorithm>

constexpr const char *DeleteQuery::qname;

//static pthread_mutex_t delete_lock;

struct arguement{
    DeleteQuery* query;
    int begin;
    int end;
};

/*static void* thread_test(void* arg){
    struct arguement arg_new = *(struct arguement*) arg;
    arg_new.query->thread_execute(arg_new.begin, arg_new.end);
    return NULL;
}*/


QueryResult::Ptr DeleteQuery::execute() {
    using namespace std;
    if (this->operands.size() != 0)
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
        auto result = initCondition(table);
        //vector<Table::Iterator> delete_items;
        if (result.second) {
            auto it = table.begin();
            while (it != table.end()) {
                if (this->evalCondition(*it)) {
                    it->setMark(1);
                    table.deleteKeyMap(it->key());
                    //it = table.erase(it);
                    //(*it).setKey("Xiang");
                    //delete_items.push_back(it);
                    it++;
                    counter++;
                } else{
                    it->setMark(0);
                    it->Remap(counter);
                    it++;
                }
            }
            /*for (unsigned int i = 0; i < delete_items.size(); i++)
                table.erase(delete_items[i]);*/
            //auto it_temp = std::remove(table.begin(), table.end(), 1);
            //counter = table.erase_x(table.begin(), table.end());
        }
        //auto &table = db[this->targetTable];
        //counter = table.erase_x(table.begin(), table.end());
        counter = table.erase_mark();
        return make_unique<RecordCountResult>(counter);
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

std::string DeleteQuery::toString() {
    return "QUERY = DELETE " + this->targetTable + "\"";
}

void DeleteQuery::thread_execute(int begin, int end){
    Database &db = Database::getInstance();
    auto &table = db[this->targetTable];
    auto result = initCondition(table);
    //std::vector<Table::Iterator> delete_items;
    if (result.second) {
        //auto it = table.begin();
        //while (it != table.end()) {
        for(auto it = table.begin()+begin; it < table.begin()+end; it++){
            if (this->evalCondition(*it)) {
                //it = table.erase(it);
                (*it).setMark(1);
                //delete_items.push_back(it);
                //counter++;
            }
            else{
                it->setMark(0);
            }
        }
    }
}
