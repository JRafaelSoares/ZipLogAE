#include "common.h"
#include "config.h"
#include "zipper/zipper.h"
#include "client/client.h"
#include "proxy/proxy.h"
#include "server/server.h"
#include "subscriber/subscriber.h"
#include <csignal>

#define SUCCESS 0
#define ERROR -1

using namespace ziplog::api;
using namespace ziplog::impl;

int main(int argc, char *argv[])
{
    if (argc < 3 || argc > 5)
    {
        std::cerr << "Usage: <mode> <config_file> [id]" << std::endl;
        std::cerr << "Modes:" << std::endl;
        std::cerr << "  zipper              - no ID required" << std::endl;
        std::cerr << "  client <proxy_id>   - ID required" << std::endl;
        std::cerr << "  proxy <id>          - ID required" << std::endl;
        std::cerr << "  server <id>         - ID required" << std::endl;
        std::cerr << "  subscriber <id>     - ID required" << std::endl;
        std::cerr << "\nExamples:" << std::endl;
        std::cerr << "  " << argv[0] << " zipper config/servers.json" << std::endl;
        std::cerr << "  " << argv[0] << " proxy config/servers2.json 0" << std::endl;
        return ERROR;
    }

    try
    {
        string mode = argv[1];
        string config_file = argv[2];

        optional<NodeId> id;
        if (argc >= 4)
        {
            id = static_cast<NodeId>(std::stoi(argv[3]));
        }

        ZiplogConfig config = parse_config(config_file);

        if (mode == "zipper")
        {
            Zipper zipper(config.make_zipper_config());
            // keep alive until signal
            std::cout << "Press CTRL+C to shutdown..." << std::endl;
            signal(SIGTERM, [](int)
                   { exit(0); });
            signal(SIGINT, [](int)
                   { exit(0); });
            pause(); // sleep until signal
        }
        else if (mode == "client")
        {
            if (!id.has_value())
            {
                std::cerr << "Client mode requires an proxy ID to contact" << std::endl;
                return ERROR;
            }
            Address proxy_addr = config.proxies[*id]; // get the proxy address using the id
            Client client(proxy_addr);

            // read from stdin and send appends
            std::cout << "Type string value to send APPENDs and hit Enter. Enter 'quit' to shutdown..." << std::endl;
            string line;
            while (std::getline(std::cin, line))
            {
                if (line == "quit")
                    break;
                bool success = client.append(line);
                std::cout << (success ? "Sent successfully" : "Send failed") << std::endl;
            }
        }
        else if (mode == "proxy")
        {
            if (!id.has_value())
            {
                std::cerr << "Proxy mode requires an ID" << std::endl;
                return ERROR;
            }
            Proxy proxy(config.make_proxy_config(*id));

            // keep alive until signal
            std::cout << "Press CTRL+C to shutdown..." << std::endl;
            signal(SIGTERM, [](int)
                   { exit(0); });
            signal(SIGINT, [](int)
                   { exit(0); });
            pause(); // sleep until signal
        }
        else if (mode == "server")
        {
            if (!id.has_value())
            {
                std::cerr << "Server mode requires an ID" << std::endl;
                return ERROR;
            }
            Server server(config.make_server_config(*id));

            // keep alive until signal
            std::cout << "Press CTRL+C to shutdown..." << std::endl;
            signal(SIGTERM, [](int)
                   { exit(0); });
            signal(SIGINT, [](int)
                   { exit(0); });
            pause(); // sleep until signal
        }
        else if (mode == "subscriber")
        {
            if (!id.has_value())
            {
                std::cerr << "Subscriber mode requires an ID" << std::endl;
                return ERROR;
            }
            Subscriber subscriber(config.make_subscriber_config(*id));

            // keep alive until signal
            std::cout << "Press CTRL+C to shutdown..." << std::endl;
            signal(SIGTERM, [](int)
                   { exit(0); });
            signal(SIGINT, [](int)
                   { exit(0); });
            pause(); // sleep until signal
        }
        else if (mode == "benchmark")
        {
            if (!id.has_value())
            {
                std::cerr << "Benchmark mode requires a proxy ID" << std::endl;
                return ERROR;
            }
            int num_commands = argc >= 5 ? std::stoi(argv[4]) : 1000;

            Address proxy_addr = config.proxies[*id]; // get the proxy address using the id
            Client client(proxy_addr);

            int success = 0;
            for (int i = 0; i < num_commands; i++)
            {
                if (client.append(Command()))
                    success++; // empty payload, client auto builds 4KB message
            }

            cout << "benchmark complete: " << success << "/" << num_commands << " successful\n";
        }
        else
        {
            std::cerr << "Unsupported mode: " << mode << std::endl;
            return ERROR;
        }
    }
    catch (const ConfigParseError &e)
    {
        std::cerr << "Config Error: " << e.what() << std::endl;
        return ERROR;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return ERROR;
    }
    return SUCCESS;
}
