#ifndef SQLITE_EXTRACT_H
#define SQLITE_EXTRACT_H

#include <string>
#include <map>
#include <vector>

#include <plog/Log.h>


#include <sqlite3.h>

// TODO: proper resource handling for sqlite3 used in here (many missing finalizes etc.)

class SQLiteExtract {
public:
    SQLiteExtract() : db(nullptr) {

    }

    ~SQLiteExtract() {
        if(db) {
            sqlite3_close(db);
        }
    }

    bool loadDB(std::string path) {
        int exit = sqlite3_open(path.c_str(), &db);

        if (exit) {
            PLOG_ERROR << "Error open DB " << sqlite3_errmsg(db);
            // TODO: check docs - anything else to do here with db?
            db = nullptr;
            return (false);
        }

        return true;
    };

    bool isLoaded() {
        return db != nullptr;
    }

    // returns a map mapping objects ocel_type to their slug name
    std::map<std::string, std::string> get_object_map_type() {
        std::map<std::string, std::string> result;
        // ocel_type, ocel_type_map
        std::string sql = "SELECT * FROM object_map_type;";

        sqlite3_stmt* stmt;

        // compile sql statement to binary
        if(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) != SQLITE_OK) {
            PLOG_ERROR << "while compiling sql: " <<  sqlite3_errmsg(db);
            sqlite3_close(db);
            sqlite3_finalize(stmt);
            return  {};
        }

        int ret_code = 0;
        //PLOG_DEBUG << "object_type_map:";
        while((ret_code = sqlite3_step(stmt)) == SQLITE_ROW) {
            std::string ocel_type((char*)sqlite3_column_text(stmt, 0));
            std::string ocel_type_map((char*)sqlite3_column_text(stmt, 1));
            //PLOG_DEBUG << ocel_type << " / " << ocel_type_map;

            result[ocel_type] = ocel_type_map;

        }

        if(ret_code != SQLITE_DONE) {
            //this error handling could be done better, but it works
            PLOG_ERROR << "while performing sql:" << sqlite3_errmsg(db);
            PLOG_ERROR << "ret_code = " << ret_code;
        }

        // TODO: check if callback approach here https://www.geeksforgeeks.org/sql-using-c-c-and-sqlite/
        // is more feasible

        return result;
    }

    // returns a map mapping objects ocel_type to their slug name
    std::map<std::string, std::string> get_event_map_type() {
        std::map<std::string, std::string> result;
        // ocel_type, ocel_type_map
        std::string sql = "SELECT * FROM event_map_type;";

        sqlite3_stmt* stmt;

        // compile sql statement to binary
        if(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) != SQLITE_OK) {
            PLOG_ERROR << "while compiling sql: " <<  sqlite3_errmsg(db);
            sqlite3_close(db);
            sqlite3_finalize(stmt);
            return  {};
        }

        int ret_code = 0;
        //PLOG_DEBUG << "object_type_map:";
        while((ret_code = sqlite3_step(stmt)) == SQLITE_ROW) {
            std::string ocel_type((char*)sqlite3_column_text(stmt, 0));
            std::string ocel_type_map((char*)sqlite3_column_text(stmt, 1));
            //PLOG_DEBUG << ocel_type << " / " << ocel_type_map;

            result[ocel_type] = ocel_type_map;

        }

        if(ret_code != SQLITE_DONE) {
            //this error handling could be done better, but it works
            PLOG_ERROR << "while performing sql:" << sqlite3_errmsg(db);
            PLOG_ERROR << "ret_code = " << ret_code;
        }

        return result;
    }


    //TODO probably easier on memory - but potentially less performant? - to operate with a callback on row here
    std::vector<std::pair<std::string,std::string>> getObjects() {
        std::vector<std::pair<std::string,std::string>> result;
        // ocel_type, ocel_type_map
        std::string sql = "SELECT * FROM object;";

        sqlite3_stmt* stmt;

        // compile sql statement to binary
        if(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) != SQLITE_OK) {
            PLOG_ERROR << "while compiling sql: " <<  sqlite3_errmsg(db);
            sqlite3_close(db);
            sqlite3_finalize(stmt);
            return  {};
        }

        int ret_code = 0;
        //PLOG_DEBUG << "Objects:";
        while((ret_code = sqlite3_step(stmt)) == SQLITE_ROW) {
            std::string ocel_id((char*)sqlite3_column_text(stmt, 0));
            std::string ocel_type((char*)sqlite3_column_text(stmt, 1));
            //PLOG_DEBUG << ocel_id << " / " << ocel_type;
            result.push_back({ocel_id, ocel_type});
        }

        if(ret_code != SQLITE_DONE) {
            //this error handling could be done better, but it works
            PLOG_ERROR << "while performing sql:" << sqlite3_errmsg(db);
            PLOG_ERROR << "ret_code = " << ret_code;
        }

        // TODO: check if callback approach here https://www.geeksforgeeks.org/sql-using-c-c-and-sqlite/
        // is more feasible

        return result;
    };

    std::vector<std::pair<std::string,std::string>> getEvents() {
        std::vector<std::pair<std::string,std::string>> result;
        // ocel_type, ocel_type_map
        std::string sql = "SELECT * FROM event;";

        sqlite3_stmt* stmt;

        // compile sql statement to binary
        if(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) != SQLITE_OK) {
            PLOG_ERROR << "while compiling sql: " <<  sqlite3_errmsg(db);
            sqlite3_close(db);
            sqlite3_finalize(stmt);
            return  {};
        }

        int ret_code = 0;
        //PLOG_DEBUG << "Objects:";
        while((ret_code = sqlite3_step(stmt)) == SQLITE_ROW) {
            std::string ocel_id((char*)sqlite3_column_text(stmt, 0));
            std::string ocel_type((char*)sqlite3_column_text(stmt, 1));
            //PLOG_DEBUG << ocel_id << " / " << ocel_type;
            result.push_back({ocel_id, ocel_type});
        }

        if(ret_code != SQLITE_DONE) {
            //this error handling could be done better, but it works
            PLOG_ERROR << "while performing sql:" << sqlite3_errmsg(db);
            PLOG_ERROR << "ret_code = " << ret_code;
        }

        // TODO: check if callback approach here https://www.geeksforgeeks.org/sql-using-c-c-and-sqlite/
        // is more feasible

        return result;
    };

    std::vector<
        std::tuple<std::string,std::string,std::string>
        > getObjectObjectRelations() {
        std::vector<std::tuple<std::string,std::string,std::string>> result;
        // ocel_type, ocel_type_map
        std::string sql = "SELECT * FROM object_object;";

        sqlite3_stmt* stmt;

        // compile sql statement to binary
        if(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) != SQLITE_OK) {
            PLOG_ERROR << "while compiling sql: " <<  sqlite3_errmsg(db);
            sqlite3_close(db);
            sqlite3_finalize(stmt);
            return  {};
        }

        int ret_code = 0;
        //PLOG_DEBUG << "Object Object Relations:";
        while((ret_code = sqlite3_step(stmt)) == SQLITE_ROW) {
            std::string ocel_source_id((char*)sqlite3_column_text(stmt, 0));
            std::string ocel_target_id((char*)sqlite3_column_text(stmt, 1));
            std::string ocel_qualifier((char*)sqlite3_column_text(stmt, 2));
            //PLOG_DEBUG << ocel_source_id << " / " << ocel_target_id << " / " << ocel_qualifier;
            result.push_back({ocel_source_id, ocel_target_id, ocel_qualifier});
        }

        if(ret_code != SQLITE_DONE) {
            //this error handling could be done better, but it works
            PLOG_ERROR << "while performing sql:" << sqlite3_errmsg(db);
            PLOG_ERROR << "ret_code = " << ret_code;
        }

        // TODO: check if callback approach here https://www.geeksforgeeks.org/sql-using-c-c-and-sqlite/
        // is more feasible

        return result;
    };

    /**
     * @brief getEventObjectRelations
     * @return vector of (event_id, object_id, ocel_qualifier)
     */
    std::vector<
        std::tuple<std::string,std::string,std::string>
        > getEventObjectRelations() {
        std::vector<std::tuple<std::string,std::string,std::string>> result;
        // ocel_type, ocel_type_map
        std::string sql = "SELECT * FROM event_object;";

        sqlite3_stmt* stmt;

        // compile sql statement to binary
        if(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) != SQLITE_OK) {
            PLOG_ERROR << "while compiling sql: " <<  sqlite3_errmsg(db);
            sqlite3_close(db);
            sqlite3_finalize(stmt);
            return  {};
        }

        int ret_code = 0;
        //PLOG_DEBUG << "Object Object Relations:";
        while((ret_code = sqlite3_step(stmt)) == SQLITE_ROW) {
            std::string ocel_event_id((char*)sqlite3_column_text(stmt, 0));
            std::string ocel_object_id((char*)sqlite3_column_text(stmt, 1));
            std::string ocel_qualifier((char*)sqlite3_column_text(stmt, 2));
            //PLOG_DEBUG << ocel_source_id << " / " << ocel_target_id << " / " << ocel_qualifier;
            result.push_back({ocel_event_id, ocel_object_id, ocel_qualifier});
        }

        if(ret_code != SQLITE_DONE) {
            //this error handling could be done better, but it works
            PLOG_ERROR << "while performing sql:" << sqlite3_errmsg(db);
            PLOG_ERROR << "ret_code = " << ret_code;
        }

        // TODO: check if callback approach here https://www.geeksforgeeks.org/sql-using-c-c-and-sqlite/
        // is more feasible

        return result;
    };

    // returns a map relating ocel id to event time (which is a string in database)
    std::map<std::string, std::string> getEventTimes() {
        std::map<std::string, std::string> result;

        const auto event_map = this->get_event_map_type();

        for (auto it = event_map.begin(); it != event_map.end(); it++)
        {
            const auto key = it->first;
            const auto value = it->second;
            std::stringstream qstream;
            qstream << "SELECT * FROM event_" << value << ";";

            const std::string sql = qstream.str();

            sqlite3_stmt* stmt;

            // compile sql statement to binary
            if(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL) != SQLITE_OK) {
                PLOG_ERROR << "while compiling sql: " <<  sqlite3_errmsg(db);
                sqlite3_close(db);
                sqlite3_finalize(stmt);
                return  {};
            }

            int ret_code = 0;
            //PLOG_DEBUG << "Objects:";
            while((ret_code = sqlite3_step(stmt)) == SQLITE_ROW) {
                std::string ocel_id((char*)sqlite3_column_text(stmt, 0));
                std::string ocel_date((char*)sqlite3_column_text(stmt, 1));
                //PLOG_DEBUG << ocel_id << " / " << ocel_type;
                result[ocel_id] = ocel_date;
            }

            if(ret_code != SQLITE_DONE) {
                //this error handling could be done better, but it works
                PLOG_ERROR << "while performing sql:" << sqlite3_errmsg(db);
                PLOG_ERROR << "ret_code = " << ret_code;
            }

            sqlite3_finalize(stmt);
        }

        return result;
    };

private:
    sqlite3* db;



};


#endif // SQLITE_EXTRACT_H
