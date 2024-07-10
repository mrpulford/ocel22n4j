#include <string>

#include <plog/Init.h>
#include <plog/Appenders/ColorConsoleAppender.h>
#include <plog/Formatters/TxtFormatter.h>
#include <plog/Log.h>

#include <boost/program_options.hpp>

#include "sqlite_extract.h"
#include "n4j_insert.h"

int main(int argc, char ** argv) {
    static plog::ColorConsoleAppender<plog::TxtFormatter> consoleAppender;

#ifdef NDEBUG
    plog::init<1>(plog::info, &consoleAppender);
#else
    plog::init<1>(plog::verbose, &consoleAppender);
#endif
    plog::init(plog::verbose).addAppender(plog::get<1>());

    PLOG_INFO << "sqlite neo4j etl tool";

    try {
        boost::program_options::options_description desc("Allowed options");
        desc.add_options()
            ("help", "produce help message")
            ("input-file", boost::program_options::value<std::string>(),"ocel file")
            ("user", boost::program_options::value<std::string>(), "db user")
            ("pass", boost::program_options::value<std::string>(), "db pass")
            ("db", boost::program_options::value<std::string>(), "db name")
            ("host", boost::program_options::value<std::string>(), "db host")
            ;


        boost::program_options::variables_map vm;

        const auto parsed = boost::program_options::command_line_parser(argc, argv).options(desc).run();
        boost::program_options::store(parsed, vm);
        boost::program_options::notify(vm);

        if (vm.count("help")) {
            PLOG_INFO << desc;
            return 0;
        }

        std::string input_file;
        if(vm.count("input-file")) {
            input_file = vm["input-file"].as< std::string>();
        }
        else {
            PLOG_ERROR << "No ocel sqlite file was given.";
            PLOG_INFO << desc;
            return 0;
        }



        std::string dbuser{""};
        if(vm.count("user")) {
            dbuser = vm["user"].as< std::string>();
        }
        else {
            PLOG_ERROR << "No user was given.";
            PLOG_INFO << desc;
            return 0;
        }

        std::string dbpass{""};
        if(vm.count("pass")) {
            dbpass = vm["pass"].as< std::string>();
        }
        else {
            PLOG_ERROR << "No password was given.";
            PLOG_INFO << desc;
            return 0;
        }

        std::string dbhost{"localhost:7474"};
        if(vm.count("host")) {
            dbhost = vm["host"].as<std::string>();
        }
        else {
            PLOG_INFO << "No host was given. Defaulting to " << dbhost;
        }


        std::string dbname{"neo4j"};
        if(vm.count("db")) {
            dbname = vm["db"].as<std::string>();
        }
        else {
            PLOG_INFO << "No db name was given. Defaulting to " << dbname;
        }

        size_t batch_size = 1000;

        PLOG_INFO << "Inserting to neo4j";
        N4JInsert n4jinserter(dbhost, dbname, dbuser, dbpass);

        SQLiteExtract sqle;
        sqle.loadDB(input_file);

        const auto objects = sqle.getObjects();

        PLOG_INFO << "No. of objects: " << objects.size();

        std::vector<N4JInsert::query> batch;
        batch.reserve(batch_size);

        size_t obj_nr = 0;
        for(const auto & object_data : objects) {
            PLOG_DEBUG << "o: " << obj_nr;
            obj_nr++;
            std::stringstream statement;
            statement << "MERGE (n:Object:`" << object_data.second << "` {ocel_id: $id})";
            N4JInsert::query q {statement.str(),
                               {{"id", object_data.first}}};

            batch.push_back(q);

            if(batch.size() == batch_size) {
                n4jinserter.send_query(batch);
                batch.clear();
            }
        }

        n4jinserter.send_query(batch);
        batch.clear();


        const auto events = sqle.getEvents();
        auto event_times = sqle.getEventTimes();

        PLOG_INFO << "No. of events: " << events.size();

        size_t e_nr = 0;
        for(const auto & event_data : events) {
            // dirrrrrrty hack to make this a compliant time string
            std::string timestr = event_times[event_data.first];
            timestr[10] = 'T';

            PLOG_DEBUG << "e: " << e_nr;
            e_nr++;
            std::stringstream statement;
            statement << "MERGE (n:Event:`" << event_data.second << "` {ocel_id: $id, ocel_time: datetime($time)})";

            N4JInsert::query q {statement.str(),{
                                                    {"id", event_data.first},
                                                    {"time", timestr}
                                                }};

            batch.push_back(q);

            if(batch.size() == batch_size) {
                n4jinserter.send_query(batch);
                batch.clear();
            }
        }

        n4jinserter.send_query(batch);
        batch.clear();


        // TODO: add ocel_time from event_{ocel_map_type} to each event (tables are ocel_id, ocel_time)

        // TODO: add relations from event_object: (ocel_event_id, ocel_object_id, ocel_qualifier)
        const auto eors = sqle.getEventObjectRelations();

        PLOG_INFO << "No. of Event to Object Relationships: " << eors.size();

        size_t eo_nr = 0;
        for(const auto & eor : eors) {
            PLOG_DEBUG << "eo: " << eo_nr;
            eo_nr++;
            std::stringstream statement;
            statement << "MATCH (src:Event {ocel_id: $event_id}) ";
            statement << " WITH src ";
            statement << "MATCH (dst:Object {ocel_id: $object_id}) ";
            statement << " MERGE (src)-[r:`" << std::get<2>(eor) << "`]-(dst) ";
            statement << " RETURN type(r)";

            N4JInsert::query q {statement.str(), {
                                                    {"event_id", std::get<0>(eor)},
                                                    {"object_id", std::get<1>(eor)}
                                                }};
            batch.push_back(q);

            if(batch.size() == batch_size) {
                n4jinserter.send_query(batch);
                batch.clear();
            }

        }


        n4jinserter.send_query(batch);
        batch.clear();


        // add relations from object_object: (ocel_source_id, ocel_target_id, ocel_qualifier) -> directional seems correct?
        const auto oors = sqle.getObjectObjectRelations();

        PLOG_INFO << "No. of Object to Object Relationships: " << oors.size();

        size_t oo_nr = 0;
        for(const auto & oor : oors) {
            PLOG_DEBUG << "oo: " << oo_nr;
            oo_nr++;
            std::stringstream statement;
            statement << "MATCH (src:Object {ocel_id: $src_id}) ";
            statement << " WITH src ";
            statement << "MATCH (dst:Object {ocel_id: $dst_id}) ";
            statement << " MERGE (src)-[r:`" << std::get<2>(oor) << "`]->(dst) ";
            statement << " RETURN type(r)";

            N4JInsert::query q {statement.str(), {
                                                    {"src_id", std::get<0>(oor)},
                                                    {"dst_id", std::get<1>(oor)}
                                                }};
            batch.push_back(q);

            if(batch.size() == batch_size) {
                n4jinserter.send_query(batch);
                batch.clear();
            }
        }

        n4jinserter.send_query(batch);
        batch.clear();



    }
    catch(std::exception& e) {
        PLOG_ERROR << "error: " << e.what() << "\n";
        return 1;
    }
    catch(...) {
        PLOG_ERROR << "Exception of unknown type!\n";
    }



}

