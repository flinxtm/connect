#pragma once

#include <json.hpp>
namespace nl = nlohmann;

namespace alles {

class settings
{
    public:

        static settings& instance();

        void load(bool _debug);
        void save();

        nl::json& get(const std::string& _id);

        unsigned get_subnet() const;
        std::string get_opc_server() const;

        void setup_vpn();

        std::string get_tls_key_path();
        std::string get_tls_cert_path();

    private:

        settings();
        struct impl; impl* m_impl;
};

}

