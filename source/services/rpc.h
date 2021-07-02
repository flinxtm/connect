
#pragma once

#include <json/json.hpp>
namespace nl = nlohmann;

namespace alles {

class rpc
{
    rpc();
    struct impl; impl* m_impl;
    public:

        static rpc& instance();
        nl::json handle_request(const std::string& _method, nl::json& _request);
};


}


