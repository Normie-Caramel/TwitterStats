#include <fstream>
#include <iostream>
#include <unordered_map>
#include <map>
#include <string>
#include <vector>
#include <algorithm>

#include <mpi.h>

#include "rapidjson/reader.h"
#include "rapidjson/document.h"
#include "rapidjson/filereadstream.h"

#include "boost/algorithm/string.hpp"
#include "boost/format.hpp"
#include "boost/archive/text_oarchive.hpp"
#include "boost/archive/text_iarchive.hpp"
#include "boost/serialization/map.hpp"
#include "boost/serialization/unordered_map.hpp"

using namespace std;
using namespace rapidjson;

typedef map<string, int> CityStats;
typedef unordered_map<string, unordered_map<string, int>> UserStats;

CityStats GCC_STATS = {{"1gsyd", 0}, {"2gmel", 0}, {"3gbri", 0}, {"4gade", 0},
                       {"5gper", 0}, {"6ghob", 0}, {"7gdar", 0}, {"8acte", 0},
                       {"9oter", 0}};
char JSON_OBJECT_START[] = "  {";
int JSON_HEADER_LENGTH = strlen(JSON_OBJECT_START);

struct DataHandler : public BaseReaderHandler<UTF8<>, DataHandler> {

    DataHandler(Document* suburbs): city_stats_{GCC_STATS}, state_{kExpectOthers}, suburbs_{suburbs} {}

    bool Null() {return true;};
    bool Bool(bool b) {return true;};
    bool Int(int i) {return true;};
    bool Uint(unsigned i) {return true;};
    bool Int64(int64_t i) {return true;};
    bool Uint64(uint64_t i) {return true;};
    bool Double(double d) {return true;};
    bool RawNumber(const Ch* str, SizeType length, bool copy) {return true;};
    bool String(const Ch* str, SizeType length, bool copy) {
        switch(state_) {
            case kExpectCityName:
            {
                string city_ = string(str, length);
                city_ = city_.substr(0, city_.find(','));
                boost::algorithm::to_lower(city_);
                auto c_city_ = city_.c_str();
                if(suburbs_->HasMember(c_city_)) {
                    auto index = (*suburbs_)[c_city_].GetObject()["gcc"].GetString();
                    if(city_stats_.find(index) != city_stats_.end()){
                        city_stats_[index] += 1;
                        user_stats_[curr_user_]["count"] += 1;
                        user_stats_[curr_user_][index] += 1;
                    }
                }
                break;
            }
            case kExpectUserID:
            {
                curr_user_ = string(str, length);
                break;
            }
            default:
            {
                return true;
            }
        }
        state_ = kExpectOthers;
        return true;
    };
    bool StartObject() {return true;};
    bool Key(const Ch* str, SizeType length, bool copy) {
        if(strcmp(str, "author_id") == 0) {
            state_ = kExpectUserID;
        } else if (strcmp(str, "full_name") == 0) {
            state_ = kExpectCityName;
        }
        return true;
    };
    bool EndObject(SizeType memberCount) {return true;};
    bool StartArray() {return true;};
    bool EndArray(SizeType elementCount) {return true;};

    enum State {
        kExpectUserID,
        kExpectCityName,
        kExpectOthers,
    } state_;
    Document* suburbs_;
    CityStats city_stats_;
    UserStats user_stats_;
    string curr_user_;
};

int main(int argc, char* argv[]) {

    // Get access to MPI service
    MPI_Init(&argc, &argv);

    int world_size, my_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    string twitter_path = "D:/2023 S1/COMP90024_Cluster_and_Cloud_Computing/Assignment_1/data/smallTwitter.json";
    string suburb_path = "D:/2023 S1/COMP90024_Cluster_and_Cloud_Computing/Assignment_1/data/sal.json";

    // open and save sal.json as a json object
    FILE* fp = fopen(suburb_path.c_str(), "rb");
    if(!fp) {
        cerr << "Error: failed to open the file " + suburb_path << endl;
        return 1;
    }
    char read_buffer[65536];
    FileReadStream is(fp, read_buffer, sizeof(read_buffer));
    Document suburbs;
    suburbs.ParseStream(is);
    fclose(fp);

    // Open the twitter file
    ifstream twitter_file(twitter_path);
    if(!twitter_file.is_open()) {
        cerr << "Error: failed to open the file " + twitter_path << endl;
        return 1;
    }

    // Get the size of input file
    twitter_file.seekg(0, ios::end);
    long long file_size = twitter_file.tellg();

    // Calculate the chunk size
    long long chunk_size = file_size / world_size;

    // Split the json file at valid boundary
    // Identify the start position (close)
    long long start_pos = chunk_size * my_rank;

    twitter_file.seekg(start_pos);
    char content[1024];
    while(twitter_file.getline(content, 1024)) {
        if(strcmp(content, JSON_OBJECT_START) == 0) break;
    }
    start_pos = twitter_file.tellg();
    start_pos -= JSON_HEADER_LENGTH + 1;

    // Identify the end position (open)
    long long end_pos = file_size;
    if(world_size > 1) {
        if(my_rank > 0) {
            MPI_Send(&start_pos, 1, MPI_INT, my_rank-1, 0, MPI_COMM_WORLD);
        }
        if(my_rank < world_size-1) {
            MPI_Recv(&end_pos, 1, MPI_INT, my_rank+1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
    }
    twitter_file.close();

    // Initialize the parser
    FILE* tw = fopen(twitter_path.c_str(), "rb");
    fseek(tw, start_pos, SEEK_SET);
    char buffer[65536];
    FileReadStream isr(tw, buffer, sizeof(buffer));
    buffer[0] = '[';
    Reader json_reader;
    DataHandler data_handler(&suburbs);
    json_reader.IterativeParseInit();
    long long total = end_pos - start_pos;

    // Parse the file stream token by token
    while(isr.Tell() < total - 10) {
        json_reader.IterativeParseNext<kParseDefaultFlags>(isr, data_handler);
    }
    fclose(tw);

    CityStats city_stats(std::move(data_handler.city_stats_));
    UserStats user_stats(std::move(data_handler.user_stats_));

    double start = MPI_Wtime();
    // Concat the stats results
    if(my_rank != 0) {
        // Set up archive
        ostringstream oss;
        boost::archive::text_oarchive oa(oss);

        // Send city stats to rank 0 process
        oa << city_stats;
        string serialized_city_stats = oss.str();
        MPI_Send(&serialized_city_stats[0], serialized_city_stats.size() + 1, MPI_CHAR, 0, 1, MPI_COMM_WORLD);

    } else {
        for(int i{1}; i<world_size; i++) {

            // Receive city stats from rank i process
            MPI_Status status;
            MPI_Probe(i, 1, MPI_COMM_WORLD, &status);
            int serialized_size;
            MPI_Get_count(&status, MPI_CHAR, &serialized_size);
            char serialized_city_data[serialized_size];
            MPI_Recv(&serialized_city_data, serialized_size, MPI_CHAR, i, 1, MPI_COMM_WORLD, &status);

            // Set up inverse archive for city stats
            istringstream iss_city(serialized_city_data);
            boost::archive::text_iarchive ia_city(iss_city);

            // inversely translate city stats from rank i node
            CityStats received_city_map;
            ia_city >> received_city_map;

            // Concat city stats map
            for(auto& e : received_city_map) {
                city_stats[e.first] += e.second;
            }
        }
    }

    if(my_rank != 0) {
        // Set up archive
        ostringstream oss;
        boost::archive::text_oarchive oa(oss);

        // Send user stats to rank 0 process
        oa << user_stats;
        string serialized_user_stats = oss.str();
        MPI_Send(&serialized_user_stats[0], serialized_user_stats.size() + 1, MPI_CHAR, 0, 2, MPI_COMM_WORLD);

    } else {
        for(int i{1}; i<world_size; i++) {

            // Receive user stats from rank i process
            MPI_Status status;
            MPI_Probe(i, 2, MPI_COMM_WORLD, &status);
            int serialized_size;
            MPI_Get_count(&status, MPI_CHAR, &serialized_size);
            char serialized_user_data[serialized_size];
            MPI_Recv(&serialized_user_data, serialized_size, MPI_CHAR, i, 2, MPI_COMM_WORLD, &status);

            // Set up inverse archive for user stats
            istringstream iss_user(serialized_user_data);
            boost::archive::text_iarchive ia_user(iss_user);

            // inversely translate user stats from rank i node
            UserStats received_user_map;
            ia_user >> received_user_map;

            // Concat user stats map
            for(auto& e : received_user_map) {
                for(auto& p : e.second) {
                    user_stats[e.first][p.first] += p.second;
                }
            }
        }
    }

    // Prompt the output
    if(my_rank == 0) {
        // print the great capital cities sorted by tweets posted
        cout << boost::format("Greater Capital City%|35t|Number of Tweets Made") << endl;
        for(auto e : city_stats) {
            cout << boost::format("%1%%|35t|%2%\n") % e.first % e.second;
        }
        cout << endl;

        // print the top 10 users posting the most tweets in great capital cities.
        cout << boost::format("Rank%|10t|Author Id%|35t|Number of Tweets Made") << endl;
        vector<pair<string, unordered_map<string, int>>> user_count(user_stats.begin(), user_stats.end());
        sort(user_count.rbegin(), user_count.rend(),
             [](auto const &a, auto const &b) {return a.second.at("count") < b.second.at("count");});
        for(int i{}; i<10; i++) {
            cout << boost::format("#%1%%|10t|%2%%|35t|%3%\n") % i % user_count[i].first % user_count[i].second["count"];
        }
        cout << endl;

        // print the top 10 users posting tweets in most great capital cities.
        cout << boost::format("Rank%|10t|Author Id%|35t|Number of Unique City Location and #Tweets") << endl;
        sort(user_count.rbegin(), user_count.rend(), [](auto const &a, auto const &b) {
            if(a.second.size() < b.second.size()) return true;
            if(a.second.size() == b.second.size()) return a.second.at("count") < b.second.at("count");
            return false;
        });
        for(int i{}; i<10; i++) {
            cout << boost::format("#%1%%|10t|%2%%|35t|%3%") % i % user_count[i].first % (user_count[i].second.size()-1);
            cout << boost::format("(#%1% tweets - ") % user_count[i].second["count"];
            vector<pair<string, int>> city_count(user_count[i].second.begin(), user_count[i].second.end());
            sort(city_count.rbegin(), city_count.rend(),
                 [](auto const &a, auto const &b) {return a.second < b.second;});
            ostringstream oss;
            for(auto& city : city_count) {
                if(city.first != "count") oss << city.second << city.first.substr(1) << ", ";
            }
            auto output = oss.str();
            cout << output.substr(0, output.size()-2) << ")" << endl;
        }
    }

    double end = MPI_Wtime();

    if(my_rank == 0) cout << end - start << endl;

    // Close MPI service
    MPI_Finalize();
}