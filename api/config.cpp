#include "config.h"
#include "address.h"
#include <third_party/json.hpp>
#include <set>
#include <regex>
#include <fstream>
#include <sstream>
#include <memory>

using json = nlohmann::json;
using std::regex;

namespace ziplog
{
    namespace api
    {
        /*
            Parses "IP:PORT" address strings. Accepts IPv4 (0.0.0.0-255.255.255.255)
            and "localhost". Ports must be in dynamic range 49152-65535.
            Examples: "127.0.0.1:50001", "localhost:55000", "192.168.1.1:51234"
            @throws ConfigParseError for invalid format, IP, or port range
        */
        Address parse_address(const string &addr)
        {
            // find presence of colon
            size_t colonPos = addr.find_last_of(':');
            if (colonPos == string::npos)
                throw ConfigParseError("Config Address is not a valid IP address, missing colon");

            // split on semicolon
            string ipAddr = addr.substr(0, colonPos);
            string portStr = addr.substr(colonPos + 1);
            int port;

            if (ipAddr.empty() || portStr.empty())
                throw ConfigParseError("Config Address or Port is empty");

            // validate addr is ipv4 within range 0-255
            regex ipPattern(
                R"(^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$)");
            if (ipAddr == "localhost")
                ipAddr = "127.0.0.1";
            if (!regex_match(ipAddr, ipPattern))
                throw ConfigParseError("Invalid IP address: " + ipAddr);

            // validate port number is within range 1-65535
            try
            {
                port = stoi(portStr);
            }
            catch (...)
            {
                throw ConfigParseError("Invalid port: " + portStr);
            }
            if (port < 1 || port > 65535)
                throw ConfigParseError("Port " + portStr + " not in range (1-65535)");

            // return parsed tcp information
            return Address(ipAddr, port);
        }

        ZiplogConfig parse_config_JSON(const string &json_str)
        {
            ZiplogConfig config;
            std::set<string> seen; // all seen addresses (for tracking duplicates between proxies/servers/subscribers)

            try
            {
                json j = json::parse(json_str);

                // store 'f'
                if (!j.contains("f") || !j["f"].is_number_integer())
                {
                    throw ConfigParseError("Missing or invalid 'f' field");
                }
                config.f = j["f"];

                // store max retries
                if (!j.contains("max_retries") || !j["max_retries"].is_number_integer())
                {
                    throw ConfigParseError("Missing or invalid 'max_retries' field");
                }
                config.max_retries = j["max_retries"];

                // store max epoch history
                if (!j.contains("max_epoch_history") || !j["max_epoch_history"].is_number_integer())
                {
                    config.max_epoch_history = MAX_EPOCH_HISTORY;
                }
                else
                {
                    config.max_epoch_history = j["max_epoch_history"];
                }

                // store epoch duration
                if (!j.contains("epoch_duration") || !j["epoch_duration"].is_number_integer())
                {
                    config.epoch_duration = EPOCH_DURATION;
                }
                else
                {
                    config.epoch_duration = j["epoch_duration"];
                }

                // validate/store zipper address
                if (!j.contains("zipper"))
                {
                    throw ConfigParseError("Missing or invalid 'zipper' field");
                }
                config.zipper = parse_address(j["zipper"]);

                // helper to extract and validate arrays
                auto extractArray = [&](const string &field) -> vector<Address>
                {
                    if (!j.contains(field) || !j[field].is_array())
                    {
                        throw ConfigParseError("Missing or invalid '" + field + "' field");
                    }

                    vector<Address> result;
                    for (const auto &item : j[field])
                    {
                        if (!item.is_string())
                        {
                            throw ConfigParseError("Non-string item in " + field + " array");
                        }
                        // verify valid address
                        string addr = item;
                        if (seen.count(addr))
                            throw ConfigParseError("Duplicate address");
                        seen.insert(addr);
                        Address val = parse_address(addr);
                        result.push_back(val);
                    }
                    return result;
                };

                // update config (does not check for duplicates or 2f + 1 servers
                config.proxies = extractArray("proxies");
                config.servers = extractArray("servers");
                config.subscribers = extractArray("subscribers");

                // if (config.proxies.size() < 1) throw ConfigParseError("Missing proxies");
                // if (config.subscribers.size() < 1) throw ConfigParseError("Missing subscribers");
                if (config.servers.size() < static_cast<size_t>(2 * config.f + 1))
                {
                    throw ConfigParseError("Insufficient servers: need at least " +
                                           std::to_string(2 * config.f + 1) +
                                           " servers for f=" + std::to_string(config.f));
                }
            }
            catch (const ConfigParseError &e)
            {
                // re-throw the specific error
                throw;
            }
            catch (const json::parse_error &e)
            {
                throw ConfigParseError("JSON syntax error at byte " + std::to_string(e.byte) + ": " + e.what());
            }
            catch (const json::exception &e)
            {
                throw ConfigParseError("JSON processing error: " + string(e.what()));
            }
            catch (const std::exception &e)
            {
                throw ConfigParseError("Unexpected error during config parsing: " + string(e.what()));
            }
            return config;
        }

        ZiplogConfig parse_config(const string &filename)
        {
            std::ifstream file(filename); // https://cplusplus.com/reference/fstream/ifstream/ifstream/
            if (!file.is_open())
                throw ConfigParseError("Can't open file: " + filename);

            std::stringstream buffer;
            buffer << file.rdbuf(); // https://cplusplus.com/reference/ios/ios/rdbuf/
            file.close();
            return parse_config_JSON(buffer.str());
        }
    }
}