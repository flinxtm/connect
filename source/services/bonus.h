#pragma once

#include <json.hpp>
#include <libcm/socket.hpp>
#include <libcm/protocol/tls.hpp>
#include <libcm/protocol/http.hpp>
namespace nl = nlohmann;

namespace alles {

class bonus
{
    struct impl; impl& m_impl;
    bonus();

    public:

        class api_error  : public std::runtime_error
        {
            public: int code = -1;
            std::string desc;
            api_error(int _code, const std::string& _desc): runtime_error("api error")
            {
                code = _code; desc = _desc;
            }
        };

        static bonus& instance();

        void process();
        void set_enabled(bool _flag);
        void set_password(const std::string& _pass);
        void set_programs(const nl::json& _progs);
        void set_cw_serial(const std::string& _sn);
        void connect(cm::socket& _sock, cm::tls& _tls, cm::http &_http);

        nl::json login(const std::string& _phone, const std::string& _pin);
        nl::json get_qr();
        nl::json check_qr(const std::string& _qr);
        void charge(const std::string& _phone, unsigned _sum, const std::string& _order);

        // async - returns immidiatelly
        void append_money(unsigned _sum, bool _cash, const std::string& _phone, const std::string& _order);
        void wash_complete(const std::string& _order, const nl::json& _progs);

        // status

        using idle_t = std::function<void()>;
        using error_t = std::function<void(std::string)>;

        void on_error(error_t _fn);
        void on_idle(idle_t _fn);

};

}


