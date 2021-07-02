#pragma once

#include <json.hpp>
namespace nl = nlohmann;

#include <libcm/socket.hpp>
#include <functional>

namespace alles {

class session_rpc
{
    public:

        typedef std::function<nl::json (std::string&, nl::json&)> req_func_t;
        void on_request(req_func_t);

        session_rpc();

        explicit session_rpc(cm::socket _socket, bool _client);
        ~session_rpc();

        session_rpc(const session_rpc& _right);
        const session_rpc& operator = (const session_rpc& _right);

        void recv();
        void send();

        bool is_tls_ok() const;
        void tls_connect(const std::string& _host);


        bool is_valid() const;
        bool is_connected() const;
        void disconnect();

        void send_error(int _code, const std::string &_desc);
        void send_result(const nl::json &_result);

    private:

        struct impl; impl* m_impl;
};

}
