#pragma once
#include "network_utils.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    class ConnectionPool {
    private:
        unordered_map<Address, int> active_connections_;  // address struct -> socket
        mutex mu_;

    public:
        int get_connection(const Address& addr) {
            lock_guard<mutex> lock(mu_);

            auto it = active_connections_.find(addr);
            if (it != active_connections_.end()) {
                int sock = it->second;  // return sock

                if (socket_alive(sock)) {
                    return sock;
                }
                close(sock);
                active_connections_.erase(it);
            }

            // create new persistent connection
            int sock = NetworkUtils::connect_to_address_persistent(addr.ip, addr.port);
            if (sock< 0) return -1;

            active_connections_[addr] = sock;
            cout << "connection to " << addr.ip << std::to_string(addr.port) << " on socket " << sock << endl;
            return sock;
        }

        // overload to allow coompilation during migration
        int get_connection(const string& ip, int port) {
            return get_connection(Address(ip, port));
        }

        void close_connection(const Address& addr) {
            lock_guard<mutex> lock(mu_);
            auto it = active_connections_.find(addr);
            if (it != active_connections_.end()) {
                close(it->second);
                active_connections_.erase(it);
            }
        }

        void close_all() {
            lock_guard<mutex> lock(mu_);
            for (auto& [addr, sock] : active_connections_) {
                close(sock);
            }
            active_connections_.clear();
        }

        ~ConnectionPool() {
            close_all();
        }

    private:
        bool socket_alive(int sock) {
            char buf;
            int result = recv(sock, &buf, 1, MSG_PEEK | MSG_DONTWAIT);
            if (result == 0) return false;  // connection closed
            if (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                return false;  // error
            }
            return true;
        }
    };
}}