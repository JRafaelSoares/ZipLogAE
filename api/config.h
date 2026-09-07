#pragma once
#include "types.h"
#include "address.h"
#include "node_config.h"
#include <stdexcept>
#include <memory>

using std::shared_ptr;

namespace ziplog
{
    namespace api
    {

        class ConfigParseError : public std::runtime_error
        {
        public:
            ConfigParseError(const string &msg) : std::runtime_error(msg) {}
        };

        // like master config (for testing). nodes have type specific configs with the necessary information
        struct ZiplogConfig
        {
            int f;
            int max_retries;
            int max_epoch_history;
            uint64_t epoch_duration;
            Address zipper;
            //        vector<pair<string, int>> proxies;
            //        vector<pair<string, int>> servers;
            //        vector<pair<string, int>> subscribers;
            vector<Address> proxies;
            vector<Address> servers;
            vector<Address> subscribers;

            // helper methods
            size_t quorum() const
            {
                return f + 1;
            }

            size_t num_proxies() const
            {
                return proxies.size();
            }

            size_t num_servers() const
            {
                return servers.size();
            }

            size_t num_subscribers() const
            {
                return subscribers.size();
            }

            bool isValidProxy(NodeId id)
            {
                if (id >= static_cast<NodeId>(num_proxies()))
                    return false;
                return true;
            }

            bool isValidServer(NodeId id)
            {
                if (id >= static_cast<NodeId>(num_servers()))
                    return false;
                return true;
            }

            bool isValidSubscriber(NodeId id)
            {
                if (id >= static_cast<NodeId>(num_subscribers()))
                    return false;
                return true;
            }

            // helpers to create type specific configs
            ZipperConfig make_zipper_config()
            {
                ZipperConfig cfg;
                cfg.id = 0; // would be the shard id (would overlap with node id)?
                cfg.shard = 0;
                cfg.address = zipper;
                cfg.f = f;
                cfg.max_retries = max_retries;
                cfg.epoch_duration = epoch_duration;
                cfg.proxies = proxies;
                cfg.servers = servers;
                cfg.subscribers = subscribers;
                return cfg;
            }

            ProxyConfig make_proxy_config(NodeId proxy_id)
            {
                if (!isValidProxy(proxy_id))
                {
                    throw std::invalid_argument("Invalid proxy ID");
                }

                ProxyConfig cfg;
                cfg.id = proxy_id;
                cfg.shard = 0;
                cfg.address = proxies[proxy_id];
                cfg.f = f;
                cfg.max_retries = max_retries;
                cfg.max_epoch_history = max_epoch_history;
                cfg.epoch_duration = epoch_duration;
                cfg.zipper = zipper;
                cfg.servers = servers;
                return cfg;
            }

            ServerConfig make_server_config(NodeId server_id)
            {
                if (!isValidServer(server_id))
                {
                    throw std::invalid_argument("Invalid server ID");
                }

                ServerConfig cfg;
                cfg.id = server_id;
                cfg.shard = 0;
                cfg.address = servers[server_id];
                cfg.f = f;
                cfg.max_retries = max_retries;
                cfg.epoch_duration = epoch_duration;
                cfg.zipper = zipper;
                cfg.proxies = proxies;
                cfg.subscribers = subscribers;

                // add other servers
                for (size_t i = 0; i < servers.size(); i++)
                {
                    if (i == server_id)
                        continue;
                    cfg.other_servers.push_back(servers[i]);
                }
                return cfg;
            }

            SubscriberConfig make_subscriber_config(NodeId subscriber_id)
            {
                if (!isValidSubscriber(subscriber_id))
                {
                    throw std::invalid_argument("Invalid server ID");
                }

                SubscriberConfig cfg;
                cfg.id = subscriber_id;
                cfg.shard = 0;
                cfg.address = subscribers[subscriber_id];
                cfg.f = f;
                cfg.max_retries = max_retries;
                cfg.zipper = zipper;
                cfg.servers = servers;
                return cfg;
            }
        };

        /*
            @brief: Validates string representing ipv4 address ("127.0.0.1:8001") and returns host and port.
            @param addr: String representing ipv4 address to be parsed/validated.
            @return: Pair of string and int if valid input.
            @throws: ConfigParseError on unsuccessful parsing (look at comments in config.cpp for more).
        */
        Address parse_address(const string &addr);

        /*
            @brief: Takes file path of file containing JSON object and parses the file to create a ZiplogConfig.
            @param filename: Path of file.
            @return: ZiplogConfig object.
            @throws: ConfigParseError if file cannot be read or JSON structure is invalid.
        */
        ZiplogConfig parse_config(const string &filename);

        /*
            @brief: Takes raw string representing JSON object and parses into a ZiplogConfig.
            @param json_str: String JSON object representing ziplog config.
            @return: ZiplogConfig object.
            @throws: ConfigParseError if JSON structure is invalid.
        */
        ZiplogConfig parse_config_JSON(const string &json_str);
    }
}