#include "zipper/reconfig_manager.h"
#include <future>
#include <iostream>

using namespace ziplog::api;

namespace ziplog::impl
{

    ReconfigManager::ReconfigManager(ProxyRegistry &registry, ReconfigCallbacks callbacks)
        : registry_(registry), cb_(std::move(callbacks)) {}

    void ReconfigManager::handle_report(const Message &msg)
    {
        NodeId failed_proxy = msg.get_failed_proxy();
        ZLOG("[reconfig] received report for proxy " << failed_proxy);

        ZLOG("[handle_report] waiting for lock...");
        std::lock_guard<std::mutex> lock(registry_.mutex());
        ZLOG("[handle_report] got lock");
        if (!registry_.exists_unlocked(failed_proxy))
            return;

        ProxyState &state = registry_.get(failed_proxy);

        // already handling this proxy
        if (state.status != ProxyStatus::ACTIVE)
            return;

        state.reporters.insert(msg.sender_id);
        if (state.reporters.size() < cb_.get_quorum())
            return;

        // quorum reached - begin freeze
        state.status = ProxyStatus::RECONFIGURING;

        // unlock before network calls
        // (re-lock not needed, send_freeze takes its own lock)
        // use a detached thread to avoid holding lock during IO
        NodeId fp = failed_proxy;
        std::thread([this, fp]()
                    { send_freeze(fp, true); })
            .detach();
    }

    void ReconfigManager::handle_freeze_response(const Message &msg)
    {
        NodeId failed_proxy = msg.get_failed_proxy();

        ZLOG("[handle freeze respnose] waiting for lock...");
        std::unique_lock<std::mutex> lock(registry_.mutex());
        ZLOG("[handle freeze response] got lock");

        if (!registry_.exists_unlocked(failed_proxy))
            return;

        ProxyState &state = registry_.get(failed_proxy);

        // stale round
        if (msg.get_round() != state.freeze_round)
            return;

        state.freeze_responders.insert(msg.sender_id);
        state.last_sequences.insert(msg.get_sequence_number());

        if (state.freeze_responders.size() < cb_.get_quorum())
            return;

        bool consensus = (state.last_sequences.size() == 1);
        SequenceNumber last_seq = *state.last_sequences.begin();
        NodeId fp = failed_proxy;
        int round = state.freeze_round;
        lock.unlock();

        if (consensus)
        {
            send_freeze_complete(fp, last_seq);
        }
        else
        {
            ZLOG("[reconfig] no consensus on round " << round << ", retrying");
            send_freeze(fp, false);
        }
    }

    void ReconfigManager::send_freeze(NodeId failed_proxy, bool first_round)
    {
        {
            ZLOG("[send freeze] waiting for lock...");
            std::lock_guard<std::mutex> lock(registry_.mutex());
            ZLOG("[send freeze] got lock");
            ProxyState &state = registry_.get(failed_proxy);
            state.freeze_round = first_round ? 1 : state.freeze_round + 1;
            state.freeze_responders.clear();
            state.last_sequences.clear();
        }

        Message freeze;
        freeze.type = FREEZE;
        freeze.shard_id = cb_.get_shard();
        freeze.set_failed_proxy(failed_proxy);

        {
            ZLOG("[send freeze 1] waiting for lock...");
            std::lock_guard<std::mutex> lock(registry_.mutex());
            freeze.set_round(registry_.get(failed_proxy).freeze_round);
            ZLOG("[send freeze 1] got lock");
        }

        broadcast_to_servers(freeze);
        ZLOG("[reconfig] broadcasted FREEZE for proxy " << failed_proxy);
    }

    void ReconfigManager::send_freeze_complete(NodeId failed_proxy, SequenceNumber last_seq)
    {
        std::vector<SequenceNumber> allocated;
        {
            ZLOG("[send freeze complete] waiting for lock...");
            std::lock_guard<std::mutex> lock(registry_.mutex());
            ZLOG("[send freeze complete] got lock");
            ProxyState &state = registry_.get(failed_proxy);
            state.status = ProxyStatus::BLOCKED;
            allocated = state.allocated_sequences;
        }

        // send SKIP for any sequence allocated but never used
        for (SequenceNumber seq : allocated)
        {
            if (seq > last_seq)
            {
                Message skip;
                skip.type = SKIP;
                skip.shard_id = cb_.get_shard();
                skip.sender_id = failed_proxy;
                skip.set_sequence_number(seq);

                std::vector<std::future<void>> futures;
                for (const Address &server : cb_.get_servers())
                {
                    futures.push_back(std::async(std::launch::async, [&, server]()
                                                 {
                    int sock = cb_.get_connection(server);
                    if (sock < 0) return;
                    if (!NetworkUtils::send_message(sock, skip)) {
                        cb_.close_connection(server);
                        return;
                    }
                    Message ack;
                    NetworkUtils::recv_message(sock, ack); }));
                }
                for (auto &f : futures)
                    f.wait();
            }
        }

        // broadcast FREEZE_COMPLETE
        Message fc;
        fc.type = FREEZE_COMPLETE;
        fc.shard_id = cb_.get_shard();
        fc.set_failed_proxy(failed_proxy);
        fc.set_sequence_number(last_seq);
        broadcast_to_servers(fc);

        ZLOG("[reconfig] freeze complete for proxy " << failed_proxy);
    }

    void ReconfigManager::broadcast_to_servers(const Message &msg)
    {
        std::vector<std::future<void>> futures;
        for (const Address &server : cb_.get_servers())
        {
            futures.push_back(std::async(std::launch::async, [&, server]()
                                         {
            int sock = cb_.get_connection(server);
            if (sock < 0) return;
            if (!NetworkUtils::send_message(sock, msg))
                cb_.close_connection(server); }));
        }
        for (auto &f : futures)
            f.wait();
    }

} // namespace ziplog::impl