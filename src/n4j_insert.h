#ifndef N4J_INSERT_H
#define N4J_INSERT_H

#include <vector>
#include <string>
#include <utility>

#include <plog/Log.h>

#include <curlpp/cURLpp.hpp>
#include <curlpp/Easy.hpp>
#include <curlpp/Options.hpp>
#include <curlpp/Exception.hpp>

namespace {


const std::string call_begin {R"EOS(
{
    "statements": [
            )EOS"};

const std::string call_end {R"EOS(
    ]
}
            )EOS"};

}


class N4JInsert {
private:
    std::string url;
    std::string db;
    std::string user;
    std::string pass;


    bool silent;

public:
    N4JInsert(std::string url, std::string db, std::string user, std::string pass, bool silent = true)
        : url(url), db(db), user(user), pass(pass), silent(silent)
    {
    }

    struct query {
        std::string query_statement;
        std::vector<std::pair<std::string,std::string>> parameters;
    };

    void send_query(const std::vector<query>& queries) {
        std::stringstream statementstream;
        statementstream << call_begin;

        for(size_t idx = 0; idx < queries.size(); idx++) {
            const auto& query_statement = queries[idx].query_statement;
            const auto& parameters = queries[idx].parameters;
            statementstream <<  "{\n";
            statementstream <<  "     \"statement\":";

            statementstream << "\"" << query_statement << "\"";

            if(parameters.empty()) {
                statementstream << "\n";
            }
            else {
                statementstream << ",\n";

                statementstream <<  "\"parameters\": { \n";

                const auto pmsize_minus_one = parameters.size() -1;
                for(size_t i = 0; i < parameters.size(); i++) {
                    const auto& parameter = parameters[i];
                    statementstream << "\"" << parameter.first << "\":" << "\"" << parameter.second <<  "\"";

                    // omit comma in last statement
                    if(i != pmsize_minus_one) {
                        statementstream << ",\n";
                    }
                    else {
                        statementstream << "\n";
                    }
                }

                statementstream << "} \n";
            }
            statementstream << "}";

            const auto last = (idx == queries.size() -1);

            if(not last) {
                statementstream << ",";
            }

            statementstream << "\n";

        }

        statementstream << call_end;

        //PLOG_DEBUG << "Statement: ";
        //PLOG_DEBUG << statementstream.str();
        send_req(statementstream.str());
    }

private:

    void send_req(const std::string&& statement) {
        curlpp::Cleanup cleaner;
        curlpp::Easy request;

        const std::string transaction_url = url + "/db/" + db + "/tx/commit";

        request.setOpt(new curlpp::options::Url(transaction_url));

        // if(not silent) request.setOpt(new curlpp::options::Verbose(true));

        std::list<std::string> header;
        header.push_back("Content-Type: application/json");

        request.setOpt(new curlpp::options::HttpHeader(header));


        const std::string auth_blank{user + ":" + pass};
        // it seems this function already encodes user:pass in base64
        request.setOpt(new curlpp::options::UserPwd(auth_blank));

        // this should silence the commandline but will result in invalid writes. probably lifetime issue.
        // if(silent) {
        //     std::ostringstream response;
        //     request.setOpt(new curlpp::options::WriteStream(&response));
        // }



        request.setOpt(new curlpp::options::PostFields(statement));
        request.setOpt(new curlpp::options::PostFieldSize(statement.length()));



        request.perform();
    }
};


#endif // N4J_INSERT_H
