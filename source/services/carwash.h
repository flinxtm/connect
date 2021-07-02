#pragma once
#include <functional>
#include <future>

#include <json.hpp>
namespace nl = nlohmann;

namespace alles {

class carwash
{
    public:

        static carwash& instance();
        void process_events();
        void perform_background_tasks();
        void update_finance(int _unit_id);

        void set_kkt_status(const std::string& _err, bool _online);
        void set_kkt_info(const std::string& _sn, const std::string& _fn,
                          const std::string& _rn, uint8_t _tax_modes);

        void set_bonus_status(const std::string& _err);

        std::future<nl::json> get_status();
        std::future<nl::json> get_diag_data();
        std::future<nl::json> get_auth_data(const std::string& _seed);

    private:

        carwash();
        struct impl; impl* m_impl;
};

}
