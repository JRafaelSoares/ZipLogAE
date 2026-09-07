#pragma once
#include "config.h"
#include "connection_pool.h"

using namespace ziplog::api;

namespace ziplog
{
    namespace impl
    {

        class Client
        {
        private:
            Address proxy_;
            ConnectionPool connection_pool_;
            NetworkUtils::ReadBuffer rb_;

        public:
            Client(Address proxy);
            ~Client();

            bool append(const Command &data);

            bool append(const string &data)
            {
                return append(Command(data.begin(), data.end()));
            }

            bool bulk_append(const vector<Command> &commands);

            void update_proxy(Address new_proxy);
        };
    }
}