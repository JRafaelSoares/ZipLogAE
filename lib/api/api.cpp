#include <zip/api/api.h>
#include <zip/util/logger.h>

namespace zip::api {

    static zip::util::logger logger("api");

    msg_net_info net_info_to_msg(util::net_info net_inf)
    {
        msg_net_info return_struct;
        int count=0;
        std::string tok;
        auto ss = std::stringstream(net_inf.host);
        while (std::getline(ss, tok, '.'))
        {
            ZIP_ASSERT(count <= 3, "Bad Ipv4 format");
            int num = std::atoi(tok.c_str());
            ZIP_ASSERT(num >= 0, "Bad Ipv4 format");
            return_struct.ipv4[count++]=unsigned(num);
        }
        return_struct.port = net_inf.port;

        return return_struct;
    }

}