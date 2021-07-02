#pragma once

#include "types.h"
#include <string>
#include <tuple>
#include <json.hpp>
namespace nl = nlohmann;

namespace alles {

class storage
{
    public:

        static storage& instance();

        void init_db();

        bool store_chemistry(chemistry_type,uint32_t);
        void store_collect_info(unsigned _idx, std::string _user);
        void store_wash(unsigned _idx, unsigned _money, unsigned _time, unsigned* _p);
        void store_money(money_type _type, unsigned _unit_id, unsigned _paycenter_id,
                         unsigned _sum, const nl::json& _detail);

        void get_main_stat_data(range &_r, int _type, nl::json& _jresult,
                                unsigned _post_count, uint16_t _filter);

        void get_progs_stat_data(range &_r, uint32_t* _result, unsigned _post_count);
        void get_chemistry_data(range& _r, uint16_t &days_total, uint32_t* arr);

        void get_daily_data(range &_r, uint32_t* arr, uint16_t& days_total, time_t &_start);
        void get_hourly_data(range &_r, uint16_t* arr, uint16_t* arr2, uint32_t &sum_period,
                             uint32_t &avg_period, uint16_t &days_total);

        void get_cash_status(unsigned _unit_id, nl::json& _result);

        void get_collect_history(range &_r, nl::json& _jresult);
        void get_last_collect_info(unsigned _unit_id, std::string& _result, time_t& _last_collect);

        nl::json get_history(const std::string& _table, unsigned _start_id, unsigned _count);
        nl::json get_finance_daily(const std::string& _date);

        unsigned kkt_session_read(); // returns fd
        void kkt_session_create(unsigned _fd);

        unsigned register_receipt(unsigned _unit_id, unsigned _sum, bool _cash);
        void update_receipt(unsigned _id, unsigned _number, unsigned _fd, unsigned _fp,
                            std::vector<uint8_t> &_datetime, unsigned _session, unsigned _vat, unsigned _tax_mode);

        nl::json get_receipt(unsigned _id);

    private:

        storage();
        struct impl; impl* m_impl;

};

}



