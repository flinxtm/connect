
#include <chrono>
#include <time.h>
#include <map>
#include <set>

#include "types.h"
#include "storage.h"

#include <logger.h>
#include <utils.h>
#include <fmt/format.h>
#include <pqxx/pqxx>
#include <fstream>
#include <mutex>
#include <time.hpp>
#include <iostream>
#include<QDebug>
#define DB_CONN_STRING "host=127.0.0.1 dbname=allesdb user=alles password=alles78"

namespace alles {

struct storage::impl
{
    void rename_column(const std::string _table, const std::string& _old, const std::string& _new);

    std::map<unsigned, bool> m_invalidate_last_collect_info;
    std::map<unsigned, std::string> m_last_collect_info;
    std::map<unsigned, time_t> m_last_collect_time;

    std::map<unsigned, unsigned[17]> m_cash_cache;
    std::map<unsigned, bool> m_invalidate_cash_cache;

    std::recursive_mutex m_db_mtx;

};

// ------------------------------------------------------------------------------------------

storage& storage::instance()
{
    static storage v;
    return v;
}

// ------------------------------------------------------------------------------------------

storage::storage() : m_impl(new impl)
{
    init_db();
}

// ------------------------------------------------------------------------------------------

void storage::impl::rename_column(const std::string _table, const std::string& _old, const std::string& _new)
{
    try
    {
        auto sql = "ALTER TABLE IF EXISTS {0} "
                   "RENAME COLUMN {1} TO {2};";

        pqxx::connection conn(DB_CONN_STRING);
        pqxx::work wtxn(conn);
        wtxn.exec(fmt::format(sql, _table, _old, _new));
        wtxn.commit();
    }
    catch(...) // ignore error
    {
    }
}

// ------------------------------------------------------------------------------------------

void storage::init_db()
{
    try
    {
        alles::logger::instance().log_status("Check DB...");
        pqxx::connection conn(DB_CONN_STRING);

        {
            auto sql = "DROP TABLE IF EXISTS \"sync\";";
            pqxx::work wtxn(conn);
            wtxn.exec(sql);
            wtxn.commit();
        }
        {
            auto sql = "ALTER TABLE IF EXISTS chemistry "
                       "ADD COLUMN IF NOT EXISTS brush integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS wheels integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS insect integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS color integer NOT NULL DEFAULT 0;"
                    ;

            pqxx::work wtxn(conn);
            wtxn.exec(sql);
            wtxn.commit();

            auto sql2 = "CREATE TABLE IF NOT EXISTS chemistry ("
                        "id serial PRIMARY KEY,"
                        "date date NOT NULL,"
                        "foam integer NOT NULL DEFAULT 0,"
                        "shampoo integer NOT NULL DEFAULT 0,"
                        "basic integer NOT NULL DEFAULT 0,"
                        "wax integer NOT NULL DEFAULT 0,"
                        "polimer integer NOT NULL DEFAULT 0,"
                        "brush integer NOT NULL DEFAULT 0,"
                        "wheels integer NOT NULL DEFAULT 0,"
                        "insect integer NOT NULL DEFAULT 0,"
                        "color integer NOT NULL DEFAULT 0);"
                    ;

            pqxx::work wtxn2(conn);
            wtxn2.exec(sql2);
            wtxn2.commit();
        }
        {
            m_impl->rename_column("washes", "post_id", "unit_id");

            auto sql = "ALTER TABLE IF EXISTS washes "
                       "ADD COLUMN IF NOT EXISTS time_prog_8 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_9 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_10 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_11 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_12 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_13 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_14 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_15 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_16 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_17 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_18 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_19 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_20 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_21 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_22 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_23 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_24 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_25 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_26 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS time_prog_27 integer NOT NULL DEFAULT 0;"
                    ;

            pqxx::work wtxn(conn);
            wtxn.exec(sql);
            wtxn.commit();

            auto sql2 = "CREATE TABLE IF NOT EXISTS washes ("
                        "id serial PRIMARY KEY,"
                        "date date NOT NULL,"
                        "time time NOT NULL,"
                        "unit_id integer NOT NULL,"
                        "money_spent integer NOT NULL,"
                        "duration integer NOT NULL,"
                        "time_prog_1 integer NOT NULL DEFAULT 0,"
                        "time_prog_2 integer NOT NULL DEFAULT 0,"
                        "time_prog_3 integer NOT NULL DEFAULT 0,"
                        "time_prog_4 integer NOT NULL DEFAULT 0,"
                        "time_prog_5 integer NOT NULL DEFAULT 0,"
                        "time_prog_6 integer NOT NULL DEFAULT 0,"
                        "time_prog_7 integer NOT NULL DEFAULT 0,"
                        "time_prog_8 integer NOT NULL DEFAULT 0,"
                        "time_prog_9 integer NOT NULL DEFAULT 0,"
                        "time_prog_10 integer NOT NULL DEFAULT 0,"
                        "time_prog_11 integer NOT NULL DEFAULT 0,"
                        "time_prog_12 integer NOT NULL DEFAULT 0,"
                        "time_prog_13 integer NOT NULL DEFAULT 0,"
                        "time_prog_14 integer NOT NULL DEFAULT 0,"
                        "time_prog_15 integer NOT NULL DEFAULT 0,"
                        "time_prog_16 integer NOT NULL DEFAULT 0,"
                        "time_prog_17 integer NOT NULL DEFAULT 0,"
                        "time_prog_18 integer NOT NULL DEFAULT 0,"
                        "time_prog_19 integer NOT NULL DEFAULT 0,"
                        "time_prog_20 integer NOT NULL DEFAULT 0,"
                        "time_prog_21 integer NOT NULL DEFAULT 0,"
                        "time_prog_22 integer NOT NULL DEFAULT 0,"
                        "time_prog_23 integer NOT NULL DEFAULT 0,"
                        "time_prog_24 integer NOT NULL DEFAULT 0,"
                        "time_prog_25 integer NOT NULL DEFAULT 0,"
                        "time_prog_26 integer NOT NULL DEFAULT 0,"
                        "time_prog_27 integer NOT NULL DEFAULT 0);"
                    ;

            pqxx::work wtxn2(conn);
            wtxn2.exec(sql2);
            wtxn2.commit();
        }
        {
            auto sql = "ALTER TABLE IF EXISTS finance_z "
                       "DROP COLUMN IF EXISTS rid;";

            pqxx::work wtxn(conn);
            wtxn.exec(sql);
            wtxn.commit();

            auto sql2 = "CREATE TABLE IF NOT EXISTS finance_z ("
                        "id serial PRIMARY KEY,"
                        "unit_id integer NOT NULL,"
                        "date date NOT NULL,"
                        "time time NOT NULL,"
                        "type integer NOT NULL DEFAULT 0,"
                        "sum integer NOT NULL DEFAULT 0);";

            pqxx::work wtxn2(conn);
            wtxn2.exec(sql2);
            wtxn2.commit();
        }
        {
            m_impl->rename_column("finance_cash", "post_id", "unit_id");

            auto sql = "ALTER TABLE IF EXISTS finance_cash "
                       "DROP COLUMN IF EXISTS paycenter_id,"
                       "DROP COLUMN IF EXISTS rid,"
                       "DROP COLUMN IF EXISTS uid;";
                       //"DROP COLUMN IF EXISTS coins_10;";

            pqxx::work wtxn(conn);
            wtxn.exec(sql);
            wtxn.commit();
            qDebug()<<"CREATE FINANCE CASH!!!!!!!!";
            auto sql2 = "CREATE TABLE IF NOT EXISTS finance_cash ("
                        "id serial PRIMARY KEY,"
                        "unit_id integer NOT NULL,"
                        "sum_cash integer NOT NULL DEFAULT 0,"
                        "sum_coins integer NOT NULL DEFAULT 0,"
                        "sum_bills integer NOT NULL DEFAULT 0,"
                        "coins_count integer NOT NULL DEFAULT 0,"
                        "coins_0 integer NOT NULL DEFAULT 0,"
                        "coins_1 integer NOT NULL DEFAULT 0,"
                        "coins_2 integer NOT NULL DEFAULT 0,"
                        "coins_10 integer NOT NULL DEFAULT 0,"
                        "bills_count integer NOT NULL DEFAULT 0,"
                        "bills_10 integer NOT NULL DEFAULT 0,"
                        "bills_50 integer NOT NULL DEFAULT 0,"
                        "bills_100 integer NOT NULL DEFAULT 0,"
                        "bills_200 integer NOT NULL DEFAULT 0,"
                        "bills_500 integer NOT NULL DEFAULT 0,"
                        "bills_1000 integer NOT NULL DEFAULT 0,"
                        "bills_2000 integer NOT NULL DEFAULT 0,"
                        "bills_5000 integer NOT NULL DEFAULT 0,"
                        "collect_user text,"
                        "collect_time time,"
                        "collect_date date);";

            pqxx::work wtxn2(conn);
            wtxn2.exec(sql2);
            wtxn2.commit();
        }
        {
            auto sql = "ALTER TABLE IF EXISTS finance_cash "
                       "ADD COLUMN IF NOT EXISTS coins_0 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS coins_1 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS coins_2 integer NOT NULL DEFAULT 0,"
                       "ADD COLUMN IF NOT EXISTS coins_10 integer NOT NULL DEFAULT 0;"
                    ;

            pqxx::work wtxn(conn);
            wtxn.exec(sql);
            wtxn.commit();
        }

        {
            m_impl->rename_column("finance_daily", "post_id", "unit_id");

            auto sql = "ALTER TABLE IF EXISTS finance_daily "
                       "DROP COLUMN IF EXISTS paycenter_id,"
                       "DROP COLUMN IF EXISTS rid,"
                       "DROP COLUMN IF EXISTS uid;";

            pqxx::work wtxn(conn);
            wtxn.exec(sql);
            wtxn.commit();

            auto sql2 = "CREATE TABLE IF NOT EXISTS finance_daily ("
                        "id serial PRIMARY KEY,"
                        "date date NOT NULL,"
                        "day integer NOT NULL,"
                        "unit_id integer NOT NULL,"
                        "sum integer NOT NULL DEFAULT 0,"
                        "sum_cash integer NOT NULL DEFAULT 0,"
                        "sum_bonus integer NOT NULL DEFAULT 0,"
                        "sum_cards integer NOT NULL DEFAULT 0,"
                        "sum_service integer NOT NULL DEFAULT 0);";

            pqxx::work wtxn2(conn);
            wtxn2.exec(sql2);
            wtxn2.commit();
        }
        {
            m_impl->rename_column("finance_hourly", "post_id", "unit_id");

            auto sql = "ALTER TABLE IF EXISTS finance_hourly "
                       "DROP COLUMN IF EXISTS paycenter_id,"
                       "DROP COLUMN IF EXISTS rid,"
                       "DROP COLUMN IF EXISTS uid;";

            pqxx::work wtxn(conn);
            wtxn.exec(sql);
            wtxn.commit();

            auto sql2 = "CREATE TABLE IF NOT EXISTS finance_hourly ("
                       "id serial PRIMARY KEY,"
                       "date date NOT NULL,"
                       "unit_id integer NOT NULL,"
                       "sum integer NOT NULL DEFAULT 0,"
                       "h0 integer NOT NULL DEFAULT 0,"
                       "h1 integer NOT NULL DEFAULT 0,"
                       "h2 integer NOT NULL DEFAULT 0,"
                       "h3 integer NOT NULL DEFAULT 0,"
                       "h4 integer NOT NULL DEFAULT 0,"
                       "h5 integer NOT NULL DEFAULT 0,"
                       "h6 integer NOT NULL DEFAULT 0,"
                       "h7 integer NOT NULL DEFAULT 0,"
                       "h8 integer NOT NULL DEFAULT 0,"
                       "h9 integer NOT NULL DEFAULT 0,"
                       "h10 integer NOT NULL DEFAULT 0,"
                       "h11 integer NOT NULL DEFAULT 0,"
                       "h12 integer NOT NULL DEFAULT 0,"
                       "h13 integer NOT NULL DEFAULT 0,"
                       "h14 integer NOT NULL DEFAULT 0,"
                       "h15 integer NOT NULL DEFAULT 0,"
                       "h16 integer NOT NULL DEFAULT 0,"
                       "h17 integer NOT NULL DEFAULT 0,"
                       "h18 integer NOT NULL DEFAULT 0,"
                       "h19 integer NOT NULL DEFAULT 0,"
                       "h20 integer NOT NULL DEFAULT 0,"
                       "h21 integer NOT NULL DEFAULT 0,"
                       "h22 integer NOT NULL DEFAULT 0,"
                       "h23 integer NOT NULL DEFAULT 0);";

            pqxx::work wtxn2(conn);
            wtxn2.exec(sql2);
            wtxn2.commit();
        }
        {
            auto sql = "ALTER TABLE IF EXISTS receipts "
                       "ADD COLUMN IF NOT EXISTS vat integer,"
                       "ADD COLUMN IF NOT EXISTS tax_mode integer;";

            pqxx::work wtxn(conn);
            wtxn.exec(sql);
            wtxn.commit();

            auto sql2 = "CREATE TABLE IF NOT EXISTS receipts ("
                       "id serial PRIMARY KEY,"
                       "unit_id integer NOT NULL,"
                       "sum integer NOT NULL,"
                       "cash boolean NOT NULL,"
                       "date date,"
                       "time time,"
                       "number integer,"
                       "session integer,"
                       "fd integer,"
                       "fp text,"
                       "vat integer,"
                       "tax_mode integer);";

            pqxx::work wtxn2(conn);
            wtxn2.exec(sql2);
            wtxn2.commit();
        }
    }
    catch(std::exception& _ex)
    {
        std::string s = "Error wile check DB: ";
        alles::logger::instance().log_error(s + _ex.what());
    }
}

// ------------------------------------------------------------------------------------------
// serialized via mutex

void storage::store_money(money_type _type, unsigned _unit_id,
                          unsigned _paycenter_id, unsigned _sum, const nl::json& _detail)
{
    std::lock_guard<std::recursive_mutex> lock(m_impl->m_db_mtx);
    qDebug()<<"STORE MONEY!!!!"<<_unit_id<<"SUM!!!!"<<_sum<<QString().fromStdString(_detail.dump());
    pqxx::connection conn(DB_CONN_STRING);
    pqxx::work txn(conn);

    if (_type == money_cash)
    {
         auto s = "SELECT sum_cash FROM finance_cash "
                  "WHERE collect_time IS NULL AND unit_id={};";

         auto uid = _unit_id;
         if (_paycenter_id > 0)
             uid = _paycenter_id;

         auto sql = fmt::format(s, uid);

         pqxx::result result = txn.exec(sql);

         std::string cols, vals;
         std::set<std::string> valid_cols = { "sum_coins", "sum_bills", "coins_count","coins_0","coins_1","coins_2",
                                              "coins_10","bills_count", "bills_10", "bills_50",
                                              "bills_100", "bills_200", "bills_500",
                                              "bills_1000", "bills_2000", "bills_5000" };

         if (result.size() > 0) // row exists -> update it
         {
             const pqxx::row& row = result[0];

             // update sum_cash
             {
                 unsigned sum_cash = 0;
                 if (!row[0].is_null()) sum_cash = row[0].as<unsigned>();

                 sum_cash += _sum;

                 for (auto it = _detail.begin(); it != _detail.end(); ++it)
                 {
                     if (valid_cols.count(it.key()))
                     {
                         if (!cols.empty())
                         {
                             cols += ",";
                             vals += ",";
                         }

                         cols += it.key();
                         vals += fmt::format("{}+{}", it.key(), (*it).get<unsigned>());
                     }
                 }

                 auto s = "UPDATE finance_cash SET (sum_cash,{0}) = ({1},{2}) "
                          "WHERE collect_time IS NULL AND unit_id={3};";

                 sql = fmt::format(s, cols, sum_cash, vals, uid);

                 txn.exec(sql);
             }
         }
         else // row does not exist -> create it
         {
             for (auto it = _detail.begin(); it != _detail.end(); ++it)
             {
                 if (valid_cols.count(it.key()))
                 {
                     if (!cols.empty())
                     {
                         cols += ",";
                         vals += ",";
                     }

                     cols += it.key();
                     vals += fmt::format("{}", (*it).get<unsigned>());
                 }
             }

             auto s = "INSERT INTO finance_cash (unit_id,sum_cash,{0}) "
                      "VALUES ({1},{2},{3});";

             auto sql = fmt::format(s, cols, uid, _sum, vals);

             txn.exec(sql);
         }

         m_impl->m_invalidate_cash_cache[uid] = true;
     }

    // daily
    {
        std::string daily_column;

        if (_type == money_cash)
            daily_column = "sum_cash";
        else if (_type == money_service)
            daily_column = "sum_service";
        else if (_type == money_card)
            daily_column = "sum_cards";
        else if (_type == money_bonus)
            daily_column = "sum_bonus";

        auto s = "SELECT {},sum FROM finance_daily "
                 "WHERE date='{}' AND unit_id={};";

        auto date = time_svc::instance().date();
        auto mday = time_svc::instance().mday();

        auto sql = fmt::format(s, daily_column, date, _unit_id);

        pqxx::result result = txn.exec(sql);

        if (result.size() > 0) // row exists -> update it
        {
            const pqxx::row& row = result[0];
            unsigned sum_col = 0;
            unsigned sum = 0;

            if (!row[0].is_null()) sum_col = row[0].as<unsigned>();
            if (!row[1].is_null()) sum = row[1].as<unsigned>();

            sum_col += _sum;
                sum += _sum;

            auto s = "UPDATE finance_daily SET {}={},sum={} "
                     "WHERE date='{}' AND unit_id={};";

            auto sql = fmt::format(s, daily_column, sum_col, sum, date, _unit_id);
            txn.exec(sql);
        }
        else // row does not exist -> create it
        {
            auto s = "INSERT INTO finance_daily (date,unit_id,{},sum,day) "
                     "VALUES ('{}',{},{},{},{});";

            auto sql = fmt::format(s, daily_column, date, _unit_id, _sum, _sum, mday);
            txn.exec(sql);
        }
    }

    // hourly
    {
        auto date = time_svc::instance().date();
        auto hour = time_svc::instance().hour();

        if (_type == money_card || _type == money_cash)
        {
            auto s = "SELECT h{},sum FROM finance_hourly "
                     "WHERE date='{}' AND unit_id={};";
            auto sql = fmt::format(s, hour, date, _unit_id);

            pqxx::result result = txn.exec(sql);

            if (result.size() > 0) // row exists -> update it
            {
                const pqxx::row& row = result[0];
                unsigned h_sum = 0, sum = 0;
                if (!row[0].is_null()) h_sum = row[0].as<unsigned>();
                if (!row[1].is_null()) sum = row[1].as<unsigned>();

                h_sum += _sum;
                sum += _sum;

                auto s = "UPDATE finance_hourly SET h{}={},sum={} "
                         "WHERE date='{}' AND unit_id={};";

                auto sql = fmt::format(s, hour, h_sum, sum, date, _unit_id);
                txn.exec(sql);
            }
            else // row does not exist -> create it
            {
                auto s = "INSERT INTO finance_hourly (date,unit_id,h{},sum) "
                         "VALUES ('{}',{},{},{});";

                auto sql = fmt::format(s, hour, date, _unit_id, _sum, _sum);
                txn.exec(sql);
            }
        }
    }

    txn.commit();
}

// ------------------------------------------------------------------------------------------

void storage::store_collect_info(unsigned _idx, std::string _user)
{
    std::string src = "unit_id";
    auto s = "SELECT id FROM finance_cash "
             "WHERE collect_time IS NULL AND {0}={1};";

    auto sql = fmt::format(s, src, _idx);

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::work txn(conn);
    pqxx::result result = txn.exec(sql);

    if (result.size() > 0) // row exists -> update it
    {
        // timestamp format 1999-01-08 04:05:06

        auto s = "UPDATE finance_cash SET collect_time='{} {}',"
                 "collect_date='{}', "
                 "collect_user='{}' "
                 "WHERE collect_time IS NULL AND {}={};";

        auto date = time_svc::instance().date();
        auto time = time_svc::instance().time();

        auto sql = fmt::format(s, date, time, date, _user, src, _idx);

        txn.exec(sql);
        txn.commit();
    }
    else // row does not exist
    {
        alles::logger::instance().log_debug("No new items to collect...");
    }

    m_impl->m_invalidate_cash_cache[_idx] = true;
    m_impl->m_invalidate_last_collect_info[_idx] = true;
}

// ------------------------------------------------------------------------------------------
#include<QDebug>
void storage::store_wash(unsigned _idx, unsigned _money, unsigned _time, unsigned* _p)
{
    auto date = time_svc::instance().date();
    auto time = time_svc::instance().time();
    qDebug()<<"storage store_wash insert";
    auto s = "INSERT INTO washes (date, unit_id, time, money_spent, duration, "
             "time_prog_1, time_prog_2, time_prog_3, time_prog_4,"
             "time_prog_5, time_prog_6, time_prog_7, time_prog_8,"
             "time_prog_9, time_prog_10, time_prog_11, time_prog_12,"
             "time_prog_13, time_prog_14, time_prog_15, time_prog_16, "
             "time_prog_17, time_prog_18, time_prog_19,"
             "time_prog_20, time_prog_21, time_prog_22, time_prog_23, "
             "time_prog_24, time_prog_25, time_prog_26, time_prog_27) "
             "VALUES ('{}',{},'{}',{},{},{},{},{},{},{},{},{},"
             "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{})";

    auto sql = fmt::format(s, date, _idx, time, _money, _time,
                           _p[1], _p[2], _p[3], _p[4], _p[5], _p[6], _p[7],
                           _p[8], _p[9], _p[10], _p[11], _p[12], _p[13], _p[14],
                           _p[15], _p[16], _p[17], _p[18], _p[19],
            _p[20], _p[21], _p[22], _p[23], _p[24], _p[25], _p[26], _p[27]);
    qDebug()<<"mass"<<_p[18]<<_p[19];
    //for(int i=0;i<30;i++)
    //    std::cout<<_p[i]<<" ";
    //qDebug()<<"\n";
    pqxx::connection conn(DB_CONN_STRING);
    pqxx::work txn(conn);
    txn.exec(sql);
    txn.commit();
}

// ------------------------------------------------------------------------------------------

bool storage::store_chemistry(chemistry_type _type, uint32_t _value)
{
    if (!time_svc::instance().is_valid())
        return false;

    auto date = time_svc::instance().date();
    auto mday = time_svc::instance().mday();
    auto old = date.substr(0, 7);

    bool reset = false;

    std::string col[] = { "shampoo", "basic", "wax", "polimer", "foam", "brush", "wheels", "insect", "color"};

    auto s = "SELECT {} "
             "FROM chemistry "
             "WHERE date='{}';";

    auto sql = fmt::format(s, col[_type], date);

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::work txn(conn);
    pqxx::result result = txn.exec(sql);

    if (result.size() > 0) // row exists -> update it
    {
        auto s = "UPDATE chemistry SET {}={} "
                 "WHERE date='{}';";

        auto sql = fmt::format(s, col[_type], _value, date);

        txn.exec(sql);

    }
    else // row does not exist
    {
        uint32_t prev_val = 0;
        unsigned update_v = _value;

        if (mday > 2 && mday <= 31) // try find prev day
        {
            auto s = "SELECT {} "
                     "FROM chemistry "
                     "WHERE date='{}-{}';";

            auto sql = fmt::format(s, col[_type], old, mday - 1);

            pqxx::result result = txn.exec(sql);

            if (result.size() > 0) // row exists
            {
                const pqxx::row& row = result[0];
                if (!row[0].is_null()) prev_val = row[0].as<unsigned>();

                if (_value > prev_val)
                    update_v = _value - prev_val;

                reset = true;
            }
        }

        auto s = "INSERT INTO chemistry (date, {}) "
                 "VALUES ('{}',{});";

        auto sql = fmt::format(s, col[_type], date, update_v);

        txn.exec(sql);
    }

    txn.commit();
    return reset;
}

// ------------------------------------------------------------------------------------------

void storage::get_main_stat_data(range& _r, int _type, nl::json& _jresult, unsigned _post_count, uint16_t _filter)
{
    auto s = "SELECT unit_id, SUM(sum_cash), SUM(sum_cards), SUM(sum_bonus), SUM(sum_service) "
             "FROM finance_daily "
             "WHERE date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}' "
             "AND unit_id BETWEEN {6} "
             "GROUP BY unit_id ORDER BY unit_id;";

    std::string t = "0 AND 7";
    unsigned base = 0;
    if (_type == 1) // vacuum
    {
        t = "40 AND 45";
        base = 40;
    }

    auto sql = fmt::format(s, _r.start_year, _r.start_month, _r.start_day,
                              _r.end_year,   _r.end_month,   _r.end_day, t);

    alles::logger::instance().log_status("Getting money stats...");

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::read_transaction txn(conn);
    pqxx::result result = txn.exec(sql);

    auto s2 = "SELECT unit_id, COUNT(id), SUM(money_spent), SUM(duration) "
              "FROM washes "
              "WHERE date BETWEEN '{0}-{1}-{2}' "
              "AND '{3}-{4}-{5}' "
              "AND duration >= {6} "
              "AND unit_id BETWEEN {7} "
              "GROUP BY unit_id ORDER BY unit_id;";

    auto sql2 = fmt::format(s2, _r.start_year, _r.start_month, _r.start_day,
                              _r.end_year,   _r.end_month,   _r.end_day, _filter, t);

    alles::logger::instance().log_status("Getting wash stats...");

    pqxx::result result2 = txn.exec(sql2);

    for (unsigned i = 0; i < _post_count; ++i)
    {
        nl::json j;

        for (unsigned ii = 0; ii < result.size(); ++ii) // add money results
        {
            const pqxx::row& row = result[ii];

            if (i == row[0].as<unsigned>() - base)
            {
                if (!row[1].is_null()) j["sum_cash"]    = row[1].as<unsigned>();
                if (!row[2].is_null()) j["sum_cards"]    = row[2].as<unsigned>();
                if (!row[3].is_null()) j["sum_bonus"]   = row[3].as<unsigned>();
                if (!row[4].is_null()) j["sum_service"] = row[4].as<unsigned>();
            }
        }

        for (unsigned ii = 0; ii < result2.size(); ++ii) // add cars results
        {
            const pqxx::row& row = result2[ii];

            if (i == row[0].as<unsigned>() - base)
            {
                if (!row[1].is_null()) j["washes"]      = row[1].as<unsigned>();
                if (!row[2].is_null()) j["money_spent"] = row[2].as<unsigned>();
                if (!row[3].is_null()) j["duration"]    = row[3].as<unsigned>();
            }
        }

        _jresult.push_back(j);
    }

    auto s3 = "SELECT date "
              "FROM finance_daily "
              "WHERE date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}' "
              "GROUP BY date ORDER BY date;";

    auto sql3 = fmt::format(s3, _r.start_year, _r.start_month, _r.start_day,
                                _r.end_year,   _r.end_month,   _r.end_day);

    pqxx::result result3 = txn.exec(sql3);

    if (result3.size() > 0)
    {
        std::string last_date;
        for (unsigned i = 0; i < result3.size(); ++i) // adjust dates
        {
            const pqxx::row& row = result3[i];

            if (i == 0)
            {
                std::string start_date = row[0].as<std::string>();
                int year = 0, month = 0, day = 0;
                std::sscanf(start_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
                _r.start_year = static_cast<uint16_t>(year);
                _r.start_month = static_cast<uint16_t>(month);
                _r.start_day = static_cast<uint16_t>(day);
            }

           last_date = row[0].as<std::string>();
        }

        if (!last_date.empty())
        {
            int year = 0, month = 0, day = 0;
            std::sscanf(last_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
            _r.end_year = static_cast<uint16_t>(year);
            _r.end_month = static_cast<uint16_t>(month);
            _r.end_day = static_cast<uint16_t>(day);
        }
    }
}

// ------------------------------------------------------------------------------------------

#define PROGRAM_SHAMPOO		1	//шампунь
#define PROGRAM_BASIC		2	//основная мойка
#define PROGRAM_COLD_WATER	3	//ополаскивание
#define PROGRAM_WAX         4	//воск
#define PROGRAM_POLIMER		5	//сушка
#define PROGRAM_FOAM            6	//пена
#define PROGRAM_STOP            7	//стоп
#define PROGRAM_BASIC_TURBO     8   //вода+шампунь-турбо
#define PROGRAM_COLD_WATER_TURBO     9   // холодная вода-турбо
#define PROGRAM_WAX_TURBO       10  //воск-турбо
#define PROGRAM_POLIMER_TURBO   11  // сушка+блеск-турбо
#define PROGRAM_HOT_WATER       12  //горячая вода
#define PROGRAM_HOT_WATER_TURBO 13  // гор.вода-турбо
#define PROGRAM_BRUSH           14  // щетка
#define PROGRAM_WHELLS          15 // мойка дисков
#define PROGRAM_INSECT      16 //- антимоскит
#define PROGRAM_VACUUM      17 //- пылесос
#define PROGRAM_AIR         18 //- воздух
#define PROGRAM_RINSE       19 //- стеклоомывайка
#define PROGRAM_FOAM_COLOR  20 //- пена цвет
#define PROGRAM_BRUSH_COLOR  21 //- щетка цвет
#define PROGRAM_SHAMPOO_X2  22 //- шампунь х2
#define PROGRAM_FOAM_X2  23 //- пена х2
#define PROGRAM_BRUSH_X2  24 //- щетка х2
#define PROGRAM_WHELLS_X2  25 //- диски х2
#define PROGRAM_INSECT_X2  26 //- антимоскит х2
#define PROGRAM_TURBOAIR_X2  26 //- турбосушка х2


void storage::get_progs_stat_data(range& _r, uint32_t* _result, unsigned _post_count)
{
    auto s = "SELECT SUM(time_prog_1), SUM(time_prog_2), SUM(time_prog_3), SUM(time_prog_4),"
                     "SUM(time_prog_5), SUM(time_prog_6), SUM(time_prog_7), SUM(time_prog_8),"
                     "SUM(time_prog_9), SUM(time_prog_10), SUM(time_prog_11), SUM(time_prog_12),"
                     "SUM(time_prog_13), SUM(time_prog_14), SUM(time_prog_15), SUM(time_prog_16),"
                     "SUM(time_prog_17), SUM(time_prog_18), SUM(time_prog_19), "
                     "SUM(time_prog_20), SUM(time_prog_21), SUM(time_prog_22), SUM(time_prog_23),"
                     "SUM(time_prog_24), SUM(time_prog_25), SUM(time_prog_26), SUM(time_prog_27)"
             "FROM washes "
             "WHERE date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}' "
             "AND unit_id BETWEEN 0 AND 7;";

    auto sql = fmt::format(s, _r.start_year, _r.start_month, _r.start_day,
                              _r.end_year,   _r.end_month,   _r.end_day);

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::read_transaction txn(conn);
    pqxx::result result = txn.exec(sql);

    if (result.size() > 0)
    {
        const pqxx::row& row = result[0];

        for (unsigned ii = 0; ii < row.size(); ++ii)
        {
            if (ii >= 27)
                break;

            if (!row[ii].is_null())
                _result[ii+1] = row[ii].as<unsigned>();
        }
    }

    auto s3 = "SELECT date "
              "FROM washes "
              "WHERE date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}' "
              "GROUP BY date ORDER BY date;";

    auto sql3 = fmt::format(s3, _r.start_year, _r.start_month, _r.start_day,
                                _r.end_year,   _r.end_month,   _r.end_day);

    pqxx::result result3 = txn.exec(sql3);

    if (result3.size() > 0)
    {
        std::string last_date;
        for (unsigned i = 0; i < result3.size(); ++i) // add money results
        {
            const pqxx::row& row = result3[i];

            if (i == 0)
            {
                std::string start_date = row[0].as<std::string>();
                int year = 0, month = 0, day = 0;
                std::sscanf(start_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
                _r.start_year = static_cast<uint16_t>(year);
                _r.start_month = static_cast<uint16_t>(month);
                _r.start_day = static_cast<uint16_t>(day);
            }

           last_date = row[0].as<std::string>();
        }

        if (!last_date.empty())
        {
            int year = 0, month = 0, day = 0;
            std::sscanf(last_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
            _r.end_year = static_cast<uint16_t>(year);
            _r.end_month = static_cast<uint16_t>(month);
            _r.end_day = static_cast<uint16_t>(day);
        }
    }
}

// ------------------------------------------------------------------------------------------

void storage::get_chemistry_data(range &_r, uint16_t &days_total, uint32_t *arr)
{
    auto s = "SELECT SUM(shampoo),SUM(basic),SUM(wax),SUM(polimer),SUM(foam),"
             "SUM(brush),SUM(wheels),SUM(insect),SUM(color) "
             "FROM chemistry "
             "WHERE date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}' ";

    auto sql = fmt::format(s, _r.start_year, _r.start_month, _r.start_day,
                              _r.end_year,   _r.end_month,   _r.end_day);

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::read_transaction txn(conn);
    pqxx::result result = txn.exec(sql);

    if (result.size() > 0)
    {
        const pqxx::row& row = result[0];

        if (!row[chemistry_basic].is_null())
            arr[chemistry_basic] = row[chemistry_basic].as<unsigned>();

        if (!row[chemistry_foam].is_null())
            arr[chemistry_foam] = row[chemistry_foam].as<unsigned>();

        if (!row[chemistry_polimer].is_null())
            arr[chemistry_polimer] = row[chemistry_polimer].as<unsigned>();

        if (!row[chemistry_shampoo].is_null())
            arr[chemistry_shampoo] = row[chemistry_shampoo].as<unsigned>();

        if (!row[chemistry_wax].is_null())
            arr[chemistry_wax] = row[chemistry_wax].as<unsigned>();

        if (!row[chemistry_brush].is_null())
            arr[chemistry_brush] = row[chemistry_brush].as<unsigned>();

        if (!row[chemistry_wheels].is_null())
            arr[chemistry_wheels] = row[chemistry_wheels].as<unsigned>();

        if (!row[chemistry_insect].is_null())
            arr[chemistry_insect] = row[chemistry_insect].as<unsigned>();
        if (!row[chemistry_color].is_null())
            arr[chemistry_color] = row[chemistry_color].as<unsigned>();
    }

    auto s3 = "SELECT date "
              "FROM chemistry "
              "WHERE date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}' "
              "GROUP BY date ORDER BY date;";

    auto sql3 = fmt::format(s3, _r.start_year, _r.start_month, _r.start_day,
                                _r.end_year,   _r.end_month,   _r.end_day);

    pqxx::result result3 = txn.exec(sql3);

    if (result3.size() > 0)
    {
        std::string last_date;
        for (unsigned i = 0; i < result3.size(); ++i) // add money results
        {
            const pqxx::row& row = result3[i];

            if (i == 0)
            {
                std::string start_date = row[0].as<std::string>();
                int year = 0, month = 0, day = 0;
                std::sscanf(start_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
                _r.start_year = static_cast<uint16_t>(year);
                _r.start_month = static_cast<uint16_t>(month);
                _r.start_day = static_cast<uint16_t>(day);
            }

           last_date = row[0].as<std::string>();
        }

        if (!last_date.empty())
        {
            int year = 0, month = 0, day = 0;
            std::sscanf(last_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
            _r.end_year = static_cast<uint16_t>(year);
            _r.end_month = static_cast<uint16_t>(month);
            _r.end_day = static_cast<uint16_t>(day);
        }

        days_total = result3.size();
    }
}

// ------------------------------------------------------------------------------------------

void storage::get_hourly_data(range& _r, uint16_t *arr, uint16_t *arr2,
                              uint32_t &sum_period, uint32_t &avg_period, uint16_t &days_total)
{
    auto s4 = "SELECT SUM(sum),COUNT(DISTINCT date) "
              "FROM finance_hourly "
              "WHERE date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}';";

    auto sql4 = fmt::format(s4, _r.start_year, _r.start_month, _r.start_day,
                              _r.end_year,   _r.end_month,   _r.end_day);

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::read_transaction txn(conn);
    pqxx::result result4 = txn.exec(sql4);

    if (result4.size() > 0)
    {
        const pqxx::row& row = result4[0];

        if (!row[0].is_null()) sum_period = static_cast<uint32_t>(row[0].as<unsigned>());
        if (!row[1].is_null()) days_total = static_cast<uint16_t>(row[1].as<unsigned>());

        if (days_total > 0)
            avg_period = sum_period / days_total;
    }

    auto s = "SELECT SUM(h0),SUM(h1),SUM(h2),SUM(h3),SUM(h4),SUM(h5),SUM(h6),SUM(h7),SUM(h8),"
                    "SUM(h9),SUM(h10),SUM(h11),SUM(h12), SUM(h13),SUM(h14),SUM(h15),SUM(h16),"
                    "SUM(h17),SUM(h18),SUM(h19),SUM(h20), SUM(h21),SUM(h22),SUM(h23) "
             "FROM finance_hourly "
             "WHERE date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}';";

    auto sql = fmt::format(s, _r.start_year, _r.start_month, _r.start_day,
                              _r.end_year,   _r.end_month,   _r.end_day);

    pqxx::result result = txn.exec(sql);

    if (result.size() > 0)
    {
        const pqxx::row& row = result[0];

        for(unsigned i = 0; i < 24; ++i)
            if (!row[i].is_null())
                if (days_total > 0)
                    arr[i] = static_cast<uint16_t>(row[i].as<double>() / days_total);
    }

    auto date = time_svc::instance().date();

    auto s2 = "SELECT SUM(h0),SUM(h1),SUM(h2),SUM(h3),SUM(h4),SUM(h5),SUM(h6),SUM(h7),SUM(h8),"
                     "SUM(h9),SUM(h10),SUM(h11),SUM(h12), SUM(h13),SUM(h14),SUM(h15),SUM(h16),"
                     "SUM(h17),SUM(h18),SUM(h19),SUM(h20), SUM(h21),SUM(h22),SUM(h23) "
              "FROM finance_hourly "
              "WHERE date='{}';";

    auto sql2 = fmt::format(s2, date);

    pqxx::result result2 = txn.exec(sql2);

    if (result2.size() > 0)
    {
        const pqxx::row& row = result2[0];

        for(unsigned i = 0; i < 24; ++i)
            if (!row[i].is_null())
                arr2[i] = static_cast<uint16_t>(row[i].as<double>());
    }

    auto s3 = "SELECT date "
              "FROM finance_hourly "
              "WHERE date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}' "
              "GROUP BY date ORDER BY date;";

    auto sql3 = fmt::format(s3, _r.start_year, _r.start_month, _r.start_day,
                                _r.end_year,   _r.end_month,   _r.end_day);

    pqxx::result result3 = txn.exec(sql3);

    if (result3.size() > 0)
    {
        std::string last_date;
        for (unsigned i = 0; i < result3.size(); ++i) // add money results
        {
            const pqxx::row& row = result3[i];

            if (i == 0)
            {
                std::string start_date = row[0].as<std::string>();
                int year = 0, month = 0, day = 0;
                std::sscanf(start_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
                _r.start_year = static_cast<uint16_t>(year);
                _r.start_month = static_cast<uint16_t>(month);
                _r.start_day = static_cast<uint16_t>(day);
            }

           last_date = row[0].as<std::string>();
        }

        if (!last_date.empty())
        {
            int year = 0, month = 0, day = 0;
            std::sscanf(last_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
            _r.end_year = static_cast<uint16_t>(year);
            _r.end_month = static_cast<uint16_t>(month);
            _r.end_day = static_cast<uint16_t>(day);
        }
    }

    txn.commit();
}

// ------------------------------------------------------------------------------------------

unsigned get_max_day(unsigned _month, unsigned _year)
{
    switch(_month)
    {
        case 1: case 3: case 5: case 7: case 8: case 10: case 12: return 31;
        case 4: case 6: case 9: case 11: return 30;
        default:
            if(_year % 4 == 0)
            {
                if(_year % 100 == 0)
                {
                    if(_year % 400 == 0)
                        return 29;
                    return 28;
                }
                return 29;
            }
            return 28;
    }
}

void storage::get_daily_data(range& _r, uint32_t *arr, uint16_t &days_total, time_t& _start)
{
    auto s = "SELECT date,SUM(sum_cash), SUM(sum_cards) "
             "FROM finance_daily "
             "WHERE date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}' "
             "GROUP BY date ORDER BY date "
             "LIMIT 1100;";

    auto sql = fmt::format(s, _r.start_year, _r.start_month, _r.start_day,
                              _r.end_year,   _r.end_month,   _r.end_day);

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::read_transaction txn(conn);
    pqxx::result result = txn.exec(sql);
    txn.commit();

    if (result.size() == 0)
        return;

    const pqxx::row& row = result[0];
    std::string start_date;

    if (!row[0].is_null())
    {
        start_date = row[0].as<std::string>();

        unsigned year = 0, month = 0, day = 0;
        std::tm tms;
        memset(&tms, 0, sizeof(tms));
        tms.tm_isdst = -1;
        std::sscanf(start_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
        tms.tm_year = year - 1900;
        tms.tm_mon = month - 1;
        tms.tm_mday = day;

        _start = std::mktime(&tms);

        _r.start_year = static_cast<uint16_t>(year);
        _r.start_month = static_cast<uint16_t>(month);
        _r.start_day = static_cast<uint16_t>(day);

        std::string next_date = fmt::format("{0}-{1:02d}-{2:02d}", year, month, day);
        unsigned j = 0;
        unsigned i = 0;

        for(; i < 1100; ++i)
        {
            const pqxx::row& row = result[j];
            std::string date = row[0].as<std::string>();

            if (date == next_date)
            {
                arr[i] = static_cast<uint32_t>(row[1].as<unsigned>()); // sum_cash
                arr[i] += static_cast<uint32_t>(row[2].as<unsigned>()); // sum_cards
                ++j;
            }

            if (j >= result.size())
                break;

            auto max_day = get_max_day(month, year);
            if (month == 12 && day == max_day) // rotate year
            {
                year++; month = 1; day = 1;
            }
            else if (day == max_day)
            {
                month++; day = 1;
            }
            else
                day++;

            next_date = fmt::format("{0}-{1:02d}-{2:02d}", year, month, day);

 //           logger::instance().log_debug("next: " + next_date);
        }

        _r.end_year = static_cast<uint16_t>(year);
        _r.end_month = static_cast<uint16_t>(month);
        _r.end_day = static_cast<uint16_t>(day);

        days_total = i + 1;
    }

}

// ------------------------------------------------------------------------------------------

void storage::get_collect_history(range& _r, nl::json& _jresult)
{
    auto s = "SELECT collect_date,collect_user,unit_id,SUM(sum_cash) "
             "FROM finance_cash "
             "WHERE collect_date BETWEEN '{0}-{1}-{2}' AND '{3}-{4}-{5}' "
             "GROUP BY collect_date, collect_user, unit_id "
             "ORDER BY collect_date;";

    auto sql = fmt::format(s, _r.start_year, _r.start_month, _r.start_day,
                              _r.end_year,   _r.end_month,   _r.end_day);

    alles::logger::instance().log_status("Getting collect history...");

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::read_transaction txn(conn);
    pqxx::result result = txn.exec(sql);
    txn.commit();

    std::string last_date;
    nl::json j;

    for (unsigned i = 0; i < result.size(); ++i) // add money results
    {
        const pqxx::row& row = result[i];

        if (i == 0)
        {
            std::string start_date = row[0].as<std::string>();
            int year = 0, month = 0, day = 0;
            std::sscanf(start_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
            _r.start_year = static_cast<uint16_t>(year);
            _r.start_month = static_cast<uint16_t>(month);
            _r.start_day = static_cast<uint16_t>(day);
        }

        std::string date = row[0].as<std::string>();
        std::string user = row[1].as<std::string>();
        unsigned unit_id = row[2].as<unsigned>();
        unsigned sum_cash = row[3].as<unsigned>();

        auto add_date = [&]()
        {
            _jresult[last_date] = j;
            j.clear();
        };

        if (last_date != date) // date changed
        {
            if (!last_date.empty())
                add_date();
            last_date = date;
        }

        std::string pid = "unit_" + std::to_string(unit_id);

        j["user"] = user;

        if (j.count(pid))
            sum_cash += (unsigned) j.at(pid);

        j[pid] = sum_cash;

        if (i == result.size() - 1) // last item
            add_date();
    }

    //logger::instance().log_debug(_jresult.dump(4));

    if (!last_date.empty())
    {
        int year = 0, month = 0, day = 0;
        std::sscanf(last_date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
        _r.end_year = static_cast<uint16_t>(year);
        _r.end_month = static_cast<uint16_t>(month);
        _r.end_day = static_cast<uint16_t>(day);
    }
}

// ------------------------------------------------------------------------------------------

void storage::get_last_collect_info(unsigned _unit_id, std::string& _result, time_t& _last_collect)
{
    bool invalidate = true;
    if (m_impl->m_invalidate_last_collect_info.count(_unit_id))
    {
        invalidate = false;
        if (m_impl->m_invalidate_last_collect_info.at(_unit_id) == true)
            invalidate = true;
    }

    if (invalidate)
    {
        m_impl->m_invalidate_last_collect_info[_unit_id] = false;

        auto sql = "SELECT sum_cash, collect_date, collect_time, collect_user FROM finance_cash "
                   "WHERE collect_date IS NOT NULL AND unit_id={0} "
                   "ORDER BY collect_date DESC,collect_time DESC LIMIT 1;";

        auto fsql = fmt::format(sql, _unit_id);

        //logger::instance().log_debug(fsql);

        pqxx::connection conn(DB_CONN_STRING);
        pqxx::read_transaction txn(conn);
        pqxx::result result = txn.exec(fsql);
        txn.commit();

        if (result.size() > 0)
        {
            const pqxx::row& row = result[0];

            unsigned sum = 0;
            if (!row[0].is_null()) sum = row[0].as<unsigned>();

            std::string date, time, user;
            if (!row[1].is_null()) date = row[1].as<std::string>();
            if (!row[2].is_null()) time = row[2].as<std::string>();
            if (!row[3].is_null()) user = row[3].as<std::string>();

            m_impl->m_last_collect_info[_unit_id] = fmt::format("{0} руб, {1},{2},{3}", sum, date, time, user);

            unsigned year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
            std::tm tms;
            memset(&tms, 0, sizeof(tms));
            tms.tm_isdst = -1;
            std::sscanf(date.c_str(), "%4d-%2d-%2d", &year, &month, &day);
            std::sscanf(time.c_str(), "%2d:%2d:%2d", &hour, &minute, &second);

            tms.tm_year = year - 1900;
            tms.tm_mon = month - 1;
            tms.tm_mday = day;
            tms.tm_hour = hour;
            tms.tm_min = minute;
            tms.tm_sec = second;

            m_impl->m_last_collect_time[_unit_id] = timegm(&tms);
        }
     }

    if (m_impl->m_last_collect_info.count(_unit_id))
        _result = m_impl->m_last_collect_info.at(_unit_id);

    if (m_impl->m_last_collect_time.count(_unit_id))
        _last_collect = m_impl->m_last_collect_time.at(_unit_id);
}

// ------------------------------------------------------------------------------------------

void storage::get_cash_status(unsigned _unit_id, nl::json& _result)
{
    bool invalidate = true;
    if (m_impl->m_invalidate_cash_cache.count(_unit_id))
    {
        invalidate = false;
        if (m_impl->m_invalidate_cash_cache.at(_unit_id) == true)
            invalidate = true;
    }

    if (invalidate)
    {
        m_impl->m_invalidate_cash_cache[_unit_id] = false;

        //logger::instance().log_debug(fmt::format("Invalidating cash cache for unit_id={0}", _unit_id));

        auto sql = "SELECT * FROM finance_cash "
                   "WHERE collect_date IS NULL AND "
                   "unit_id={0};";

        pqxx::connection conn(DB_CONN_STRING);
        pqxx::read_transaction txn(conn);
        pqxx::result result = txn.exec(fmt::format(sql, _unit_id));
        txn.commit();

        if (result.size() > 0)
        {
            const pqxx::row& row = result[0];

            for (auto it = row.begin(); it != row.end(); ++it)
            {
                std::string n = it->name();
                if (n =="sum_cash")       m_impl->m_cash_cache[_unit_id][0] = it.as<unsigned>();
                if (n == "sum_coins")     m_impl->m_cash_cache[_unit_id][1] = it.as<unsigned>();
                if (n == "sum_bills")     m_impl->m_cash_cache[_unit_id][2] = it.as<unsigned>();
                if (n == "coins_count")   m_impl->m_cash_cache[_unit_id][3] = it.as<unsigned>();
                if (n == "coins_0")   m_impl->m_cash_cache[_unit_id][4] = it.as<unsigned>();
                if (n == "coins_1")   m_impl->m_cash_cache[_unit_id][5] = it.as<unsigned>();
                if (n == "coins_2")   m_impl->m_cash_cache[_unit_id][6] = it.as<unsigned>();
                if (n == "coins_10")   m_impl->m_cash_cache[_unit_id][7] = it.as<unsigned>();

                if (n == "bills_count")   m_impl->m_cash_cache[_unit_id][8] = it.as<unsigned>();
                if (n == "bills_10")      m_impl->m_cash_cache[_unit_id][9] = it.as<unsigned>();
                if (n == "bills_50")      m_impl->m_cash_cache[_unit_id][10] = it.as<unsigned>();
                if (n == "bills_100")     m_impl->m_cash_cache[_unit_id][11] = it.as<unsigned>();
                if (n == "bills_200")     m_impl->m_cash_cache[_unit_id][12] = it.as<unsigned>();
                if (n == "bills_500")     m_impl->m_cash_cache[_unit_id][13] = it.as<unsigned>();
                if (n == "bills_1000")    m_impl->m_cash_cache[_unit_id][14] = it.as<unsigned>();
                if (n == "bills_2000")    m_impl->m_cash_cache[_unit_id][15] = it.as<unsigned>();
                if (n == "bills_5000")    m_impl->m_cash_cache[_unit_id][16] = it.as<unsigned>();
            }
        }
        else
            m_impl->m_cash_cache.erase(_unit_id);
    }

    if (m_impl->m_cash_cache.count(_unit_id))
    {
        _result["sum_cash"]    = m_impl->m_cash_cache[_unit_id][0];
        _result["sum_coins"]   = m_impl->m_cash_cache[_unit_id][1];
        _result["sum_bills"]   = m_impl->m_cash_cache[_unit_id][2];
        _result["coins_count"] = m_impl->m_cash_cache[_unit_id][3];
        _result["coins_0"] = m_impl->m_cash_cache[_unit_id][4];
        _result["coins_1"] = m_impl->m_cash_cache[_unit_id][5];
        _result["coins_2"] = m_impl->m_cash_cache[_unit_id][6];
        _result["coins_10"] = m_impl->m_cash_cache[_unit_id][7];
        _result["bills_count"] = m_impl->m_cash_cache[_unit_id][8];
        _result["bills_10"]    = m_impl->m_cash_cache[_unit_id][9];
        _result["bills_50"]    = m_impl->m_cash_cache[_unit_id][10];
        _result["bills_100"]   = m_impl->m_cash_cache[_unit_id][11];
        _result["bills_200"]   = m_impl->m_cash_cache[_unit_id][12];
        _result["bills_500"]   = m_impl->m_cash_cache[_unit_id][13];
        _result["bills_1000"]  = m_impl->m_cash_cache[_unit_id][14];
        _result["bills_2000"]  = m_impl->m_cash_cache[_unit_id][15];
        _result["bills_5000"]  = m_impl->m_cash_cache[_unit_id][16];

    }
    else
    {
        _result["sum_cash"]    = 0;
        _result["sum_coins"]   = 0;
        _result["sum_bills"]   = 0;
        _result["coins_count"] = 0;
        _result["coins_0"] = 0;
        _result["coins_1"] = 0;
        _result["coins_2"] = 0;
        _result["coins_10"] = 0;
        _result["bills_count"] = 0;
        _result["bills_10"]    = 0;
        _result["bills_50"]    = 0;
        _result["bills_100"]   = 0;
        _result["bills_200"]   = 0;
        _result["bills_500"]   = 0;
        _result["bills_1000"]  = 0;
        _result["bills_2000"]  = 0;
        _result["bills_5000"]  = 0;
    }
    //qDebug()<<"GET CASH STATUS!!!!"<<_unit_id<<QString::fromStdString(_result.dump());

}

// ------------------------------------------------------------------------------------------

nl::json storage::get_history(const std::string& _table, unsigned _start_id, unsigned _count)
{
    std::set<std::string> v = { "finance_cash", "finance_daily", "finance_hourly",
                                "washes", "chemistry" };

    std::set<std::string> str_fields = { "date", "time", "collect_user", "collect_time",
                                         "collect_date" };

    if (v.find(_table) == v.end())
        throw std::runtime_error("table not found");

    if (_count > 100) _count = 100;

    auto s = "SELECT * FROM {0} WHERE id >= {1} ORDER BY id LIMIT {2};";

    auto sql = fmt::format(s, _table, _start_id, _count);

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::read_transaction txn(conn);
    pqxx::result result = txn.exec(sql);
    txn.commit();

    nl::json res = nl::json::array_t();
    nl::json j;

    for (unsigned i = 0; i < result.size(); ++i) // results to json
    {
        const pqxx::row& row = result[i];
        for (auto it = row.begin(); it != row.end(); ++it)
        {
            std::string n = it.name();

            if (it.is_null())
                j[n] = nullptr;
            else if (str_fields.find(n) != str_fields.end())
                j[n] = it.as<std::string>();
            else
                j[n] = it.as<unsigned>();
        }

        res.push_back(j);
    }

    return res;
}

// ------------------------------------------------------------------------------------------

unsigned storage::kkt_session_read()
{
    std::ifstream file("/alles/data/kkt.json");
    unsigned id = -1;

    try
    {
        nl::json ssn;
        file >> ssn;
        file.close();

        id = ssn.at("fd");
    }
    catch (...) {}

    return id;
}

// ------------------------------------------------------------------------------------------

void storage::kkt_session_create(unsigned _fd)
{
    std::ofstream file("/alles/data/kkt.json");
    nl::json j =
    {
        { "fd", _fd }
    };

    file << j.dump();
    file.flush();
    file.close();
}

// ------------------------------------------------------------------------------------------

unsigned storage::register_receipt(unsigned _unit_id, unsigned _sum, bool _cash)
{
    std::lock_guard<std::recursive_mutex> lock(m_impl->m_db_mtx);

    auto sql = "INSERT INTO receipts (unit_id, sum, cash) "
               "VALUES ({0},{1},{2}) RETURNING id;";

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::work txn(conn);
    pqxx::result result = txn.exec(fmt::format(sql, _unit_id, _sum, _cash));

    unsigned res = -1;
    if (result.size() == 1)
    {
        const pqxx::row& row = result[0];
        if (!row[0].is_null())
        {
            txn.commit();
            return row[0].as<unsigned>();
        }
    }
    else
    {
        txn.abort();
    }

    return res;
}

// ------------------------------------------------------------------------------------------

void storage::update_receipt(unsigned _id, unsigned _number, unsigned _fd, unsigned _fp,
                             std::vector<uint8_t>& _datetime, unsigned _session,
                             unsigned _vat, unsigned _tax_mode)
{
    // datetime 5 bytes in format YMDHm

    std::string date = fmt::format("{}-{}-{}", (unsigned) _datetime[0] + 2000,
                                                _datetime[1], _datetime[2]);
    std::string time = fmt::format("{}:{}", _datetime[3], _datetime[4]);

    auto sql = "UPDATE receipts SET number={},fd={},fp='{}',date='{}',time='{}',session={},vat={},tax_mode={} WHERE id={};";

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::work txn(conn);
    txn.exec(fmt::format(sql, _number, _fd, _fp, date, time, _session, _vat, _tax_mode, _id));
    txn.commit();
}

// ------------------------------------------------------------------------------------------

nl::json storage::get_receipt(unsigned _id)
{
    auto sql = "SELECT * FROM receipts WHERE id={0} AND fd IS NOT NULL;";

    pqxx::connection conn(DB_CONN_STRING);
    pqxx::read_transaction txn(conn);
    pqxx::result result = txn.exec(fmt::format(sql, _id));
    txn.commit();

    nl::json res;
    if (result.size() == 1)
    {
        const pqxx::row& row = result[0];

        res["unit_id"]  = row[1].as<unsigned>();
        res["sum"]      = row[2].as<unsigned>();
        res["cash"]     = row[3].as<bool>();

        if (!row[4].is_null())
            res["date"] = row[4].as<std::string>();
        if (!row[5].is_null())
            res["time"] = row[5].as<std::string>();
        if (!row[6].is_null())
            res["number"] = row[6].as<unsigned>();
        if (!row[7].is_null())
            res["session"] = row[7].as<unsigned>();
        if (!row[8].is_null())
            res["fd"] = row[8].as<unsigned>();
        if (!row[9].is_null())
            res["fp"] = row[9].as<std::string>();
        if (!row[10].is_null())
            res["vat"] = row[10].as<unsigned>();
        if (!row[11].is_null())
            res["tax_mode"] = row[11].as<unsigned>();
    }

    return res;
}

// ------------------------------------------------------------------------------------------

nl::json storage::get_finance_daily(const std::string& _date)
{
    pqxx::connection conn(DB_CONN_STRING);
    pqxx::read_transaction txn(conn);

    nl::json res;
    unsigned sum_cash_wet = 0, sum_card_wet = 0;
    unsigned sum_cash_dry = 0, sum_card_dry = 0;
    nl::json hourly = nl::json::array();

    {
        auto sql = "SELECT SUM(sum_cash), SUM(sum_cards) FROM finance_daily WHERE unit_id BETWEEN 0 AND 29 AND date='{}';";

        pqxx::result result = txn.exec(fmt::format(sql, _date));

        if (result.size() == 1)
        {
            const pqxx::row& row = result[0];

            if (!row[0].is_null())
                sum_cash_wet = row[0].as<unsigned>();

            if (!row[1].is_null())
                sum_card_wet = row[1].as<unsigned>();
        }
    }

    {
        auto sql = "SELECT SUM(sum_cash), SUM(sum_cards) FROM finance_daily WHERE unit_id BETWEEN 40 AND 59 AND date='{}';";

        pqxx::result result = txn.exec(fmt::format(sql, _date));

        if (result.size() == 1)
        {
            const pqxx::row& row = result[0];

            if (!row[0].is_null())
                sum_cash_dry = row[0].as<unsigned>();

            if (!row[1].is_null())
                sum_card_dry = row[1].as<unsigned>();
        }
    }

    {
        auto sql = "SELECT {} FROM finance_hourly WHERE date='{}';";
        std::string cols;
        for (unsigned i = 0; i < 24; ++i)
        {
            cols += fmt::format("SUM(h{})", i);
            if (i < 23) cols += ", ";
        }

        pqxx::result result2 = txn.exec(fmt::format(sql, cols, _date));

        if (result2.size() == 1)
        {
            const pqxx::row& row2 = result2[0];
            for (unsigned i = 0; i < 24; ++i)
            {
                if (!row2[i].is_null())
                    hourly.push_back(row2[i].as<unsigned>());
                else
                    hourly.push_back(0);
            }
        }

        res =
        {
            { "date", _date },
            { "sum_cash_wet", sum_cash_wet },
            { "sum_cash_dry", sum_cash_dry },
            { "sum_card_wet", sum_card_wet },
            { "sum_card_dry", sum_card_dry },
            { "hourly", hourly }
        };
    }

    txn.commit();
    qDebug()<<"RES ="<<QString().fromStdString(res.dump());
    return res;
}

}

