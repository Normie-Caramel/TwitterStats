#include <fstream>
#include <iostream>
#include <unordered_map>
#include <set>
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

typedef unordered_multimap<string, string> Suburbs;
typedef map<string, int> CityStats;
typedef unordered_map<string, unordered_map<string, int>> UserStats;
typedef vector<pair<string, unordered_map<string, int>>> UserProfile;

const set<string> GCC {"1gsyd", "2gmel", "3gbri", "4gade", "5gper", "6ghob", "7gdar", "8acte", "9oter"};
const unordered_map<string, string> LOC {
        {"1gsyd", "New South Wales"}, {"2gmel", "Victoria"}, {"3gbri", "Queensland"},
        {"4gade", "South Australia"}, {"5gper", "Western Australia"}, {"6ghob", "Tasmania"},
        {"7gdar", "Northern Territory"}, {"8acte", "Australian Capital Territory"}, {"9oter", ""}};

struct DataHandler : public BaseReaderHandler<UTF8<>, DataHandler> {

    DataHandler(CityStats& city_stats, UserStats& user_stats, Suburbs& suburbs):
        city_stats_{city_stats}, user_stats_{user_stats}, suburbs_{suburbs}, state_{kExpectOthers} {}

    bool String(const Ch* str, SizeType length, bool copy) {
        switch(state_) {
            case kExpectCityName: {
                string ori_city{str, length};
                string city = ori_city.substr(0, ori_city.find(','));
                city = city.substr(0, city.find(" ("));
                city = city.substr(0, city.find(" -"));
                boost::algorithm::to_lower(city);
                if(suburbs_.count(city) == 1) {
                    auto index = suburbs_.find(city)->second;
                    city_stats_[index] += 1;
                    user_stats_[curr_user_]["gcc_count"] += 1;
                    user_stats_[curr_user_][index] += 1;
                } else if(suburbs_.count(city) > 1) {
                    auto range = suburbs_.equal_range(city);
                    for(auto itr = range.first; itr != range.second; itr++) {
                        if(ori_city.find(LOC.at(itr->second)) != string::npos) {
                            auto index = itr->second;
                            city_stats_[index] += 1;
                            user_stats_[curr_user_]["gcc_count"] += 1;
                            user_stats_[curr_user_][index] += 1;
                            break;
                        }
                    }
                }
                state_ = kExpectOthers;
                break;
            }
            case kExpectUserID: {
                curr_user_ = string(str, length);
                user_stats_[curr_user_]["count"] += 1;
                state_ = kExpectOthers;
                break;
            }
        }
        return true;
    }

    bool Key(const Ch* str, SizeType length, bool copy) {
        if(length == 9) {
            if (strcmp(str, "author_id") == 0) {
                state_ = kExpectUserID;
            } else if (strcmp(str, "full_name") == 0) {
                state_ = kExpectCityName;
            }
        }
        return true;
    }

    static bool Default() {return true;}

    enum State {kExpectUserID, kExpectCityName, kExpectOthers} state_;
    Suburbs& suburbs_;
    CityStats& city_stats_;
    UserStats& user_stats_;
    string curr_user_;
};

inline Suburbs get_suburbs(const char* path) {
    FILE* fp = fopen(path, "rb");
    if(!fp) {
        cerr << "Error: failed to open the file " + string(path) << endl;
        exit(1);
    }
    char buffer[65536];
    FileReadStream is(fp, buffer, sizeof(buffer));
    Document document;
    document.ParseStream(is);
    Suburbs suburbs;
    for (auto& e : document.GetObject()) {
        string value = e.value.GetObject()["gcc"].GetString();
        if(GCC.find(value) != GCC.end()) {
            string key = e.name.GetString();
            key = key.substr(0, key.find(" ("));
            suburbs.insert({key, value});
        }
    }
    fclose(fp);
    return suburbs;
}

inline long long get_start_pos(const char* path, long long& file_size, int world_size, int rank) {
    ifstream twt_file(path);
    if(!twt_file.is_open()) {
        cerr << "Error: failed to open the file " + string(path) << endl;
        exit(1);
    };
    twt_file.seekg(0, ios::end);
    file_size = twt_file.tellg();
    if(rank == 0) return 0;
    long long chunk_size = file_size / world_size;
    long long start_pos = chunk_size * rank;
    twt_file.seekg(start_pos, ios::beg);
    char buffer[1024];
    while(twt_file.getline(buffer, 1024)) {
        if(strcmp(buffer, "  },") == 0) break;
    }
    start_pos = twt_file.tellg();
    twt_file.close();
    return start_pos;
}

inline long long get_chunk_size(long long start_pos, long long file_size, int world_size, int rank) {
    long long end_pos = file_size;
    if(world_size > 1) {
        if(rank > 0) MPI_Send(&start_pos, 1, MPI_LONG_LONG, rank-1, 0, MPI_COMM_WORLD);
        if(rank < world_size - 1) MPI_Recv(&end_pos, 1, MPI_LONG_LONG, rank+1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    return end_pos - start_pos;
}

inline void file_stats(const char* path, long long chunk_size, long long start_pos, DataHandler& dh) {
    FILE* twt_file = fopen(path, "rb");
    fseeko64(twt_file, start_pos, SEEK_SET);
    char buffer[1024 * 64];
    FileReadStream isr(twt_file, buffer, sizeof(buffer)); 
    buffer[0] = '[';
    Reader json_reader;
    json_reader.IterativeParseInit();
    while(isr.Tell() < chunk_size) {
        json_reader.IterativeParseNext<kParseIterativeFlag>(isr, dh);
    }
    fclose(twt_file);
}

template<typename Stats, typename F>
inline void merge_stats(Stats& stats, int world_size, int rank, int flag, F func) {
    if(rank != 0) {
        ostringstream oss;
        boost::archive::text_oarchive oa(oss);
        oa << stats;
        string serialized_stats = oss.str();
        size_t total = serialized_stats.size() + 1;
        if(total > numeric_limits<int>::max()) {
            cerr << "Stats size overflow!" << endl;
            exit(1);
        }
        int count = static_cast<int>(total);
        MPI_Send(&serialized_stats[0], count, MPI_CHAR, 0, flag, MPI_COMM_WORLD);
    } else {
        for(int i{1}; i<world_size; i++) {
            MPI_Status status;
            MPI_Probe(i, flag, MPI_COMM_WORLD, &status);
            int serialized_size;
            MPI_Get_count(&status, MPI_CHAR, &serialized_size);
            char serialized_data[serialized_size];
            MPI_Recv(&serialized_data, serialized_size, MPI_CHAR, i, flag, MPI_COMM_WORLD, &status);

            istringstream iss(serialized_data);
            boost::archive::text_iarchive ia(iss);

            Stats received_stats;
            ia >> received_stats;

            func(stats, received_stats);
        }
    }
}

inline void prompt_gcc_tweet_nums(CityStats& city_stats) {
    cout << boost::format("\nGreater Capital City%|35t|Number of Tweets Made") << endl;
    for(auto const& e : city_stats) {
        cout << boost::format("%1%%|35t|%2%\n") % e.first % e.second;
    }
    cout << endl;
}

inline void prompt_usr_most_tweets(UserProfile& user_count) {
    cout << boost::format("Rank%|10t|Author Id%|35t|Number of Tweets Made") << endl;
    sort(user_count.rbegin(), user_count.rend(),
         [](auto const& a, auto const& b) {return a.second.at("count") < b.second.at("count");});
    for(int i{}; i<10; i++) {
        cout << boost::format("#%1%%|10t|%2%%|35t|%3%\n") % (i+1) % user_count[i].first % user_count[i].second["count"];
    }
    cout << endl;
}

inline void prompt_usr_most_gcc(UserProfile& user_count) {
    cout << boost::format("Rank%|10t|Author Id%|35t|Number of Unique City Location and #Tweets") << endl;
    sort(user_count.rbegin(), user_count.rend(), [](auto& a, auto& b) {
        if(a.second.size() < b.second.size()) return true;
        if(a.second.size() == b.second.size()) return a.second["gcc_count"] < b.second["gcc_count"];
        return false;
    });
    for(int i{}; i<10; i++) {
        cout << boost::format("#%1%%|10t|%2%%|35t|%3%") % (i+1) % user_count[i].first % (user_count[i].second.size()-2);
        cout << boost::format("(#%1% tweets - ") % user_count[i].second["gcc_count"];
        ostringstream oss;
        for(auto& city : GCC) {
            if(user_count[i].second[city] != 0) oss << user_count[i].second[city] << city.substr(1) << ", ";
        }
        auto output = oss.str();
        cout << output.substr(0, output.size()-2) << ")" << endl;
    }
}

int main(int argc, char* argv[]) {

    // Get access to MPI service
    MPI_Init(&argc, &argv);

    double start = MPI_Wtime();

    int world_size, my_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    const char* twt_path = argv[1];
    const char* sal_path = argv[2];

    auto suburbs = get_suburbs(sal_path);

    long long file_size;
    long long start_pos = get_start_pos(twt_path, file_size, world_size, my_rank);
    long long chunk_size = get_chunk_size(start_pos, file_size, world_size, my_rank);

    CityStats city_stats;
    UserStats user_stats;
    for(auto const& city : GCC) city_stats[city] = 0;
    DataHandler data_handler(city_stats, user_stats, suburbs);

    file_stats(twt_path, chunk_size, start_pos, data_handler);

    auto merge_city = [](CityStats& des, CityStats& ori) {
        for(const auto& e : ori) des[e.first] += e.second;
    };
    auto merge_user = [](UserStats& des, UserStats& ori) {
        for(const auto& e : ori) {
            for (const auto& p : e.second) {
                des[e.first][p.first] += p.second;
            }
        }
    };

    merge_stats(city_stats, world_size, my_rank, 1, merge_city);
    merge_stats(user_stats, world_size, my_rank, 2, merge_user);

    // Prompt the output
    if(my_rank == 0) {
        UserProfile user_count(user_stats.begin(), user_stats.end());
        prompt_gcc_tweet_nums(city_stats);
        prompt_usr_most_tweets(user_count);
        prompt_usr_most_gcc(user_count);
    }

    double end = MPI_Wtime();
    if (my_rank == 0) cout << "\nExecution time: " << end - start << "s\n" << endl;

    // Close MPI service
    MPI_Finalize();
}