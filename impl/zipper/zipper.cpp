#include "zipper/zipper.h"
#include <algorithm>
#include <future>

using namespace ziplog::api;
using std::async;
using std::future;

namespace ziplog::impl
{

    Zipper::Zipper(const ZipperConfig &cfg)
        : BaseNode<ZipperConfig>(cfg), slot_allocator_(registry_), reconfig_manager_(registry_, ReconfigCallbacks{[this]()
                                                                                                                  { return quorum(); },
                                                                                                                  [this]()
                                                                                                                  { return shard(); },
                                                                                                                  [this]()
                                                                                                                  { return config_.servers; },
                                                                                                                  [this](const Address &a)
                                                                                                                  { return connection_pool_.get_connection(a); },
                                                                                                                  [this](const Address &a)
                                                                                                                  { connection_pool_.close_connection(a); }}),
          epoch_timer_(EpochDurationUnit(cfg.epoch_duration), [this]()
                       { allocate_slots(); }, [this]()
                       { introduce_proxies(); })
    {
        // register initial proxies
        for (NodeId i = 0; i < cfg.proxies.size(); i++)
        {
            registry_.add_proxy(i, cfg.proxies[i]);
        }

        start_listening();
        epoch_timer_.start();
    }

    Zipper::~Zipper()
    {
        ZLOG("Zipper shutdown() called");
        epoch_timer_.stop();
        ZLOG("Zipper timer stoped");
        BaseNode::shutdown();
        ZLOG("Zipper basenode shutdown");
        connection_pool_.close_all();
        ZLOG("Zipper shutdown() complete");
    }

    void Zipper::shutdown()
    {
        BaseNode::shutdown();
    }

    void Zipper::handle_connection(int proxy_socket)
    {
        while (running())
        {
            Message req;
            if (!NetworkUtils::recv_message(proxy_socket, req))
            {
                close(proxy_socket);
                return;
            }

            if (req.type == ZIP_REQUEST)
                update_slot_estimate(req);
            if (req.type == REPORT)
                reconfig_manager_.handle_report(req);
            if (req.type == FREEZE_RESPONSE)
                reconfig_manager_.handle_freeze_response(req);
            if (req.type == REGISTER_PROXY)
                add_proxy(req, true);
            if (req.type == REJOIN_PROXY)
                add_proxy(req, false);
            if (req.type == REGISTER_SUBSCRIBER)
                introduce_subscriber(req);
        }
        close(proxy_socket);
    }

    void Zipper::update_slot_estimate(const Message &req)
    {
        auto start = high_resolution_clock::now();
        if (req.shard_id != shard() || !config_.isValidProxy(req.sender_id))
        {
            ZLOG("[zipper] unknown proxy");
            return;
        }
        if (req.get_num_requests())
        {
            ZLOG("[zipper] estimate from proxy " << req.sender_id
                                                 << ": " << req.get_num_requests());
        }
        slot_allocator_.update_estimate(req.sender_id, req.get_num_requests());
        auto end = high_resolution_clock::now();
        auto dur = duration_cast<EpochDurationUnit>(end - start);
        // cout << "Zipper update slot estimate() : " << dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
    }

    void Zipper::allocate_slots()
    {
        auto start = high_resolution_clock::now();
        // cout << "zipper allocate_slots() called" << endl;

        auto allocations = slot_allocator_.compute_allocations(
            epoch_timer_.next_epoch(), config_.epoch_duration);

        auto t1 = high_resolution_clock::now();

        if (allocations.size())
        {
            // cout << "zipper allocate_slots() got values" << endl;
            auto dur = duration_cast<EpochDurationUnit>(t1 - start);
            cout << "Zipper compute allocations(): " << dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
        }

        for (const auto &[proxy_id, values] : allocations)
        {
            ZLOG("zipper proxy " << proxy_id << " has slots");
            thread([this, proxy_id, values]()
                   { deliver_slot_allocation(proxy_id, values); })
                .detach();
        }
        auto end = high_resolution_clock::now();
        auto dur = duration_cast<EpochDurationUnit>(t1 - start);

        // cout << "zipper allocate_slots() exited" << endl;
    }

    void Zipper::deliver_slot_allocation(NodeId proxy_id, const vector<SequenceNumber> &values)
    {
        auto start = high_resolution_clock::now();
        ZLOG("zipper sending slots out");

        Message resp;
        resp.type = ZIP_RESPONSE;
        resp.shard_id = shard();
        resp.sender_id = proxy_id;
        resp.set_num_requests(static_cast<SequenceNumber>(values.size() / 2));
        resp.set_assigned_sequences(values);

        // send to proxy
        Address proxy;
        {
            lock_guard<mutex> lock(registry_.mutex());
            proxy = registry_.get(proxy_id).address;
        }

        int sock = connection_pool_.get_connection(proxy);
        // cout << "[zipper] sending out slots to proxy " << proxy_id << " at " << std::to_string(now()) << endl;
        if (sock >= 0 && !NetworkUtils::send_message(sock, resp))
        {
            connection_pool_.close_connection(proxy);
            ZLOG("[zipper] failed to send to proxy");
        }
        else
        {
            ZLOG("[zipper] sent to proxy");
            // cout << "[zipper] done sending slots to proxy " << proxy_id << " at " << std::to_string(now()) << endl;
        }

        // forward to all servers
        for (const Address &server : config_.servers)
        {
            int sock = connection_pool_.get_connection(server);
            if (sock < 0)
                continue;
            if (NetworkUtils::send_message(sock, resp))
            {
                ZLOG("[zipper] sent to server");
            }
            else
            {
                ZLOG("[zipper] failed to send to server");
            }
        }

        ZLOG("zipper done sending slots out");
        auto end = high_resolution_clock::now();
        auto dur = duration_cast<EpochDurationUnit>(end - start);
        cout << "Zipper deliver slot allocation(): " << dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
    }

    void Zipper::add_proxy(const Message &msg, bool is_new)
    {
        string addr_info(msg.data.begin(), msg.data.end());
        size_t colon = addr_info.find(':');
        string ip = addr_info.substr(0, colon);
        int port = stoi(addr_info.substr(colon + 1));
        NodeId proxy_id = msg.sender_id;

        if (!is_new)
        {
            registry_.rejoin_proxy(proxy_id);
        }

        // broadcast to servers
        Message join;
        join.type = INCLUDE_PROXY;
        join.shard_id = shard();
        join.sender_id = proxy_id;
        join.data = Command(addr_info.begin(), addr_info.end());

        vector<future<bool>> futures;
        for (size_t i = 0; i < num_servers(); i++)
        {
            Address server = config_.servers[i];
            futures.push_back(async(std::launch::async, [this, server, join, i]()
                                    {
                int sock = connection_pool_.get_connection(server);
                if (sock < 0) return false;
                if (!NetworkUtils::send_message(sock, join)) return false;
                Message ack;
                return NetworkUtils::recv_message(sock, ack); }));
        }

        size_t ok = 0;
        for (auto &f : futures)
            if (f.get())
                ok++;

        if (ok >= quorum())
        {
            lock_guard<mutex> lock(mu_);
            joining_proxies_.push_back({ip, port});
        }
    }

    void Zipper::introduce_proxies()
    {
        deque<pair<string, int>> to_add;
        {
            lock_guard<mutex> lock(mu_);
            swap(to_add, joining_proxies_);
        }

        vector<future<void>> futures;
        for (const auto &[ip, port] : to_add)
        {
            futures.push_back(async(std::launch::async, [this, ip, port]()
                                    {
                NodeId new_id;
                {
                    ZLOG("[introduce proxies] waiting for lock...");
                    lock_guard<mutex> lock(registry_.mutex());
                    ZLOG("[introduce proxies] got lock");
                    new_id = config_.proxies.size();
                    config_.proxies.push_back({ip, port});
                }
                registry_.add_proxy(new_id, {ip, port});

                Message intro;
                intro.type     = INCLUDE_PROXY;
                intro.shard_id = shard();

                int sock = connection_pool_.get_connection({ip, port});
                if (sock < 0) return;
                NetworkUtils::send_message(sock, intro); }));
        }
        for (auto &f : futures)
            f.wait();
    }

    void Zipper::introduce_subscriber(const Message &msg)
    {
        string addr_info(msg.data.begin(), msg.data.end());
        size_t colon = addr_info.find(':');
        string ip = addr_info.substr(0, colon);
        int port = stoi(addr_info.substr(colon + 1));

        Message join;
        join.type = INCLUDE_SUBSCRIBER;
        join.shard_id = shard();
        join.sender_id = msg.sender_id;
        join.data = Command(addr_info.begin(), addr_info.end());

        vector<future<bool>> futures;
        for (size_t i = 0; i < num_servers(); i++)
        {
            Address server = config_.servers[i];
            futures.push_back(async(std::launch::async, [this, server, join, i]()
                                    {
                int sock = connection_pool_.get_connection(server);
                if (sock < 0) return false;
                if (!NetworkUtils::send_message(sock, join)) return false;
                Message ack;
                if (!NetworkUtils::recv_message(sock, ack)) {
                    ZLOG("[zipper] no ack for subscriber join from server " << i);
                    return false;
                }
                return true; }));
        }

        size_t ok = 0;
        for (auto &f : futures)
            if (f.get())
                ok++;
        if (ok < quorum())
            return;

        {
            lock_guard<mutex> lock(mu_);
            config_.subscribers.push_back({ip, port});
        }

        Message intro;
        intro.type = INCLUDE_SUBSCRIBER;
        intro.shard_id = shard();

        int sock = connection_pool_.get_connection({ip, port});
        if (sock < 0)
            return;
        NetworkUtils::send_message(sock, intro);
    }
} // namespace ziplog::impl