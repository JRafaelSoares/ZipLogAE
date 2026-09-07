#include <memory>

namespace ziplog
{
    namespace api
    {

        struct NodeConfig
        {
            NodeId id;
            ShardId shard;
            Address address; // node's own address
            int f;
            int max_retries;
        };

        struct ZipperConfig : NodeConfig
        {
            Timestamp epoch_duration;
            vector<Address> proxies;     // needed to distribute sequence numbers
            vector<Address> servers;     // need for recovery protocol
            vector<Address> subscribers; // ? not necessary

            bool isValidProxy(NodeId id)
            {
                if (id >= proxies.size())
                    return false;
                return true;
            }

            bool isValidServer(NodeId id)
            {
                if (id >= servers.size())
                    return false;
                return true;
            }

            bool isValidSubscriber(NodeId id)
            {
                if (id >= subscribers.size())
                    return false;
                return true;
            }
        };

        struct ProxyConfig : NodeConfig
        {
            Timestamp epoch_duration;
            int max_epoch_history;
            Address zipper;
            vector<Address> servers; // need servers to replicate
        };

        struct ServerConfig : NodeConfig
        {
            Timestamp epoch_duration;
            Address zipper;
            vector<Address> proxies;
            vector<Address> subscribers;   // need subscribers to broadcast
            vector<Address> other_servers; // for recovery protocol

            bool isValidProxy(NodeId id)
            {
                if (id >= proxies.size())
                    return false;
                return true;
            }
        };

        struct SubscriberConfig : NodeConfig
        {
            Address zipper;
            vector<Address> servers; // when joining

            bool isValidServer(NodeId id)
            {
                if (id >= servers.size())
                    return false;
                return true;
            }
        };
    }
}