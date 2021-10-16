
#include <functional>
#include <mutex>
#include <thread>
#include <list>
#include <map>

#include "types.h"
#include "carwash.h"
#include "storage.h"
#include "settings.h"
#include "kkt.h"
#include "bonus.h"

#include <logger.h>
#include <utils.h>
#include <time.hpp>
#include <libcm/timer.hpp>
#include <libcm/protocol/tls.hpp>
#include <libcm/socket.hpp>
#include <libcm/protocol/http.hpp>
#include <botan/sha2_32.h>
#include <botan/hex.h>

#include <fmt/format.h>
#include <opcua/client.hpp>
#include <opcua/variant.hpp>

#include "version.h"
#include <QDebug>
#include <iostream>
#define NS 6


#define TAG_POST_PROGRAM        "::AsGlobalPV:PostN[{0}].prog"
#define TAG_POST_BALANCE        "::AsGlobalPV:PostN[{0}].PanelMoney"
#define TAG_POST_PRESSURE       "::AsGlobalPV:PostN[{0}].sensorPressure"
#define TAG_POST_MOTOR_HOURS    "::AsGlobalPV:PostN[{0}].workingHours"
#define TAG_POST_DOOR           "::AsGlobalPV:PostN[{0}].door"
#define TAG_POST_COLLECT        "::AsGlobalPV:PostN[{0}].collect"
#define TAG_POST_ADD_MONEY      "::AsGlobalPV:PostN[{0}].addMoneyCount"
#define TAG_POST_BUSY           "::AsGlobalPV:PostN[{0}].busy"
#define TAG_POST_ENABLED        "::AsGlobalPV:PostN[{0}].active"

#define TAG_VPOST_ENABLED        "::AsGlobalPV:VacuumPost[{0}].active"
#define TAG_VPOST_BALANCE        "::AsGlobalPV:VacuumPost[{0}].panel_money"
#define TAG_VPOST_PROGRAM        "::AsGlobalPV:VacuumPost[{0}].program"
#define TAG_VPOST_MOTOR_HOURS    "::AsGlobalPV:VacuumPost[{0}].workingHours"

#define TAG_VPOST_ADD_MONEY     "::AsGlobalPV:VacuumPost[{0}].addMoneyCount"

#define TAG_VACUUM_COLLECT        "::AsGlobalPV:Vacuum[{0}].collect"
#define TAG_PAYCENTER_COLLECT     "::AsGlobalPV:Paycenter[{0}].collect"

#define TAG_POST_INCASS "::AsGlobalPV:ProcessEventExtern.collectMoney[{0}]"
#define TAG_WASH_NUM    "::AsGlobalPV:ProcessEventExtern.washComplete[{0}].amount"
#define TAG_VACU_NUM    "::AsGlobalPV:ProcessEventExtern.vacuumComplete[{0}].amount"

#define TAG_COIN    "::AsGlobalPV:ProcessEventExtern.moneyPostBox[{0}].CashDetails.CoinsAmount"
#define TAG_BILL    "::AsGlobalPV:ProcessEventExtern.moneyPostBox[{0}].CashDetails.BillsAmount_{1}"
#define TAG_SERVICE "::AsGlobalPV:ProcessEventExtern.moneyPostBox[{0}].ServiceSum"
#define TAG_CARD    "::AsGlobalPV:ProcessEventExtern.moneyPostBox[{0}].CardsSum"
#define TAG_BONUS   "::AsGlobalPV:ProcessEventExtern.moneyPostBox[{0}].BonusSum"

#define TAG_ST1_BUTTON "::Statistic:GeneralStatPeriodParam.request"
#define TAG_ST2_BUTTON "::Statistic:HoursStatPeriodParam.request"
#define TAG_ST3_BUTTON "::Statistic:DaysStatPeriodParam.request"
#define TAG_ST4_BUTTON "::Statistic:ProgUsageStatPeriodParam.request"
#define TAG_ST5_BUTTON "::Statistic:CollectStatPeriodParam.request"

#define TAG_CHEMISTRY "::AsGlobalPV:ProcessEventExtern.chemistryCons[{0}]"

#define TAG_PING            "::AsGlobalPV:DiagnosticExtern.Connect.iLife"
#define TAG_SW_VERSION      "::AsGlobalPV:DiagnosticExtern.Connect.SwVersion"

#define TAG_SERVICE_MONEY    "::AsGlobalPV:Recipe.ServiceMoneyAmount"
#define TAG_VSERVICE_MONEY   "::AsGlobalPV:VacuumRecipe.ServiceMoneyAmount"

#define TAG_KKT_PRINT_ENABLED   "::AsGlobalPV:Recipe.Kkm.PrintReceipt"
#define TAG_KKT_INSTALLED       "::AsGlobalPV:Recipe.Kkm.EnableDevice"
#define TAG_KKT_ADDRESS         "::AsGlobalPV:Recipe.Kkm.Address"
#define TAG_KKT_PLACE           "::AsGlobalPV:Recipe.Kkm.PaymentPlace"
#define TAG_KKT_TAX_SELECTED    "::AsGlobalPV:Recipe.Kkm.TaxMode"
#define TAG_KKT_VAT_SELECTED    "::AsGlobalPV:Recipe.Kkm.VAT"
#define TAG_KKT_ITEM_TEXT       "::AsGlobalPV:Recipe.Kkm.ReceiptText"

#define TAG_KKT_STATUS          "::AsGlobalPV:DiagnosticExtern.Kitcash.StatusTxt"
#define TAG_KKT_SN              "::AsGlobalPV:DiagnosticExtern.Kitcash.SerialNumKkt"
#define TAG_KKT_FN              "::AsGlobalPV:DiagnosticExtern.Kitcash.SerialNumFn"
#define TAG_KKT_RN              "::AsGlobalPV:DiagnosticExtern.Kitcash.RegNum"
#define TAG_KKT_IS_ONLINE       "::AsGlobalPV:DiagnosticExtern.Kitcash.iLife"
#define TAG_KKT_TAX_MODE        "::AsGlobalPV:DiagnosticExtern.Kitcash.TaxMode"

#define TAG_PRICE               "::AsGlobalPV:Recipe.ProgramPrice"
#define TAG_PROGS               "::AsGlobalPV:Recipe.ShowPrograms"

#define TAG_VACUUM_PRICE        "::AsGlobalPV:VacuumRecipe.ProgramPrice"
#define TAG_VACUUM_PROGS        "::AsGlobalPV:VacuumRecipe.ShowPrograms"


#define TAG_BONUS_PASSWORD      "::AsGlobalPV:Recipe.Bonus.Password"
#define TAG_BONUS_ENABLE        "::AsGlobalPV:Recipe.Bonus.Enable"
#define TAG_BONUS_STATUS        "::AsGlobalPV:DiagnosticExtern.Bonus.StatusTxt"

#define TAG_DATE                "::AsGlobalPV:DateTime.Date"
#define TAG_TIME                "::AsGlobalPV:DateTime.Time"

unsigned MoneyScaleFactor = 1;

namespace alles {

// ------------------------------------------------------------------------------------------
// implementation

struct task
{
    std::function<void()> func;
};

struct carwash::impl
{
    opcua::client   m_client;
    opcua::client::state_e m_prev_state = opcua::client::state_disconnected;

    bool            m_try_connect = false;
    bool            m_subscribed = false;
    bool            m_bonus_enabled = false;

    cm::timer       m_ping_timer;

    uint8_t         m_post_count = 0;
    uint8_t         m_vacuum_count = 0;
    uint32_t        m_hw_sn = 0;
    uint8_t         m_hw_nic[10];
    std::string     m_serial;
    nl::json        m_config;

    std::list<task>         m_tasks;
    std::list<task>         m_background_tasks;
    std::recursive_mutex    m_task_mutex;
    std::map <unsigned, unsigned> m_service_money_counter;

    const std::string m_program_name[28] =
    { "Шампунь", "Вода+шампунь", "Холодная вода",
      "Воск и защита", "Сушка и блеск", "Пена",
      "Стоп", "Вода+Шампунь турбо", "Холодная турбо",
      "Воск турбо", "Сушка и блеск турбо", "Горячая вода",
      "Горячая вода турбо", "Щетка", "Мойка дисков",
      "Антимоскит", "Пылесос", "Воздух", "Омывайка", "Пена цвет",
       "Щетка цвет", "Шампунь X2", "Пена X2", "Щетка X2","Диски X2",
      "Антимоскит X2", "Турбосушка",""
    };

    const std::string m_vprogram_name[3] =
    {
        "Пылесос", "Воздух", "Стоп"
    };

    void process_events();
    void subscribe();

    // read variables

    void ping();
    void read_config();

    void read_and_send_progs();


    // tasks

    void queue_task(std::function<void()>&&);
    void queue_background_task(std::function<void()>&&);

    // catch finance events

    void on_money( int _idx, money_type _type, int _sum, opcua::variant _val);
    void on_collect(int _idx, bool _old, opcua::variant _val);
    void on_add_money(unsigned _post_id, opcua::variant _val);

    void on_complete(int _idx, int _type, opcua::variant _val);
    void on_chemistry(chemistry_type _type, opcua::variant _val);

    void on_stat_main_show(opcua::variant _val);
    void on_stat_hourly_show(opcua::variant _val);
    void on_stat_daily_show(opcua::variant _val);
    void on_stat_progs_show(opcua::variant _val);
    void on_collect_history_show(opcua::variant _val);

    void on_kkt_enabled(opcua::variant _val);
    void on_kkt_printer_enabled(opcua::variant _val);
    void on_kkt_address(opcua::variant _val);
    void on_kkt_place(opcua::variant _val);
    void on_kkt_tax_selected(opcua::variant _val);
    void on_kkt_vat_selected(opcua::variant _val);
    void on_kkt_item_text(opcua::variant _val);

    void on_bonus_enable(opcua::variant _val);
    void on_refresh_cert();
    void on_bonus_password(opcua::variant _val);

    void on_price_changed(opcua::variant _val);
    void on_progs_changed(opcua::variant _val);

    void on_date_changed(opcua::variant _val);
    void on_time_changed(opcua::variant _val);

    void on_sw_version(opcua::variant _val);
    void on_currency_digits_changed(opcua::variant _val);

    void update_finance(int _unit_id);
};

// ------------------------------------------------------------------------------------------

carwash& carwash::instance()
{
    static carwash v;
    return v;
}

// ------------------------------------------------------------------------------------------

carwash::carwash() : m_impl(new impl)
{

}

// ------------------------------------------------------------------------------------------

void carwash::process_events()
{
    m_impl->process_events();
}

void carwash::impl::process_events()
{
    auto st = m_client.get_state();

    switch (st)
    {
        case opcua::client::state_session:
        {
            m_prev_state = st;
            m_try_connect = false;
            //qDebug()<<"opcua::client::state_session"<<st;
            read_config();

            if (!m_subscribed)
            {
                alles::logger::instance().log_status("OPC-UA connected, subscribing...");
                ping();
                m_ping_timer.reset();
                subscribe();
                update_finance(-1);
            }

            if (m_ping_timer.elapsed().count() >= 10000)
            {
                m_ping_timer.reset();
                ping();
            }

            m_client.poll();
        }
        break;

        case opcua::client::state_disconnected:
        {
            if (m_prev_state == opcua::client::state_session)
            {
                logger::instance().log_status("OPC-UA disconnected");
                m_client.disconnect();
            }

            m_prev_state = st;

            auto srv = settings::instance().get_opc_server();

            if (!m_try_connect)
            {
                m_try_connect = true;
                logger::instance().log_status(fmt::format("OPC-UA connecting to {0}", srv));
            }

            m_subscribed = false;

            try
            {
                m_client.connect(srv);
            }
            catch(std::exception& _ex)
            {
                //alles::logger::instance().log_status(_ex.what());
                m_client.disconnect();
            }
        }
    }

    // perform queued tasks
    {
        std::lock_guard<std::recursive_mutex> lock(m_task_mutex);

        for (auto it = m_tasks.begin(); it != m_tasks.end(); ++it)
        {
            task& tsk = (*it);
            tsk.func();
        }

        m_tasks.clear();
    }
}

// ------------------------------------------------------------------------------------------

void carwash::impl::queue_task(std::function<void ()>&& _f)
{
    std::lock_guard<std::recursive_mutex> lock(m_task_mutex);

    task tt; tt.func = _f;
    m_tasks.push_back(tt);
}

// ------------------------------------------------------------------------------------------

void carwash::impl::queue_background_task(std::function<void ()>&& _f)
{
    std::lock_guard<std::recursive_mutex> lock(m_task_mutex);

    task tt; tt.func = _f;
    m_background_tasks.push_back(tt);
}

// ------------------------------------------------------------------------------------------
// to be called in different thread

void carwash::perform_background_tasks()
{
    std::lock_guard<std::recursive_mutex> lock(m_impl->m_task_mutex);

    for (auto it = m_impl->m_background_tasks.begin();
         it != m_impl->m_background_tasks.end(); ++it)
    {
        task& tsk = (*it);
        tsk.func();
    }

    m_impl->m_background_tasks.clear();
}

// ------------------------------------------------------------------------------------------

#define TAG_CONFIG_POST_COUNT   "::AsGlobalPV:Config.numberOfPosts"
#define TAG_CONFIG_VACUUM_COUNT "::AsGlobalPV:Config.numberOfVacuums"
#define TAG_CONFIG_CW_NAME      "::AsGlobalPV:Config.orderData.name"
#define TAG_CONFIG_CW_SERIAL    "::AsGlobalPV:Config.orderData.serialNumber"
#define TAG_CONFIG_CW_ADDRESS   "::AsGlobalPV:Config.orderData.address"
#define TAG_CONFIG_SW_VERSION   "::AsGlobalPV:BuildInfo.swVersion"
#define TAG_CONFIG_HW_VERSION   "::AsGlobalPV:BuildInfo.hwVersion"
#define TAG_CONFIG_HW_SN        "::AsGlobalPV:BuildInfo.sn"
#define TAG_CONFIG_HW_NIC       "::AsGlobalPV:BuildInfo.nic"

#define TAG_CONFIG_CURRENCY_DIGITS "::AsGlobalPV:gFixedCurrency.digits"

void carwash::impl::read_config()
{
    try
    {
        //qDebug()<<"read_config1"<<(int)m_post_count<<(int)m_vacuum_count;
        m_post_count = m_client.read(TAG_CONFIG_POST_COUNT, NS);
        m_vacuum_count = m_client.read(TAG_CONFIG_VACUUM_COUNT, NS);
        //qDebug()<<"read_config1"<<(int)m_post_count<<(int)m_vacuum_count;
        m_config["wash_posts"] = m_post_count;
        m_config["vacuums"] = m_vacuum_count;
        m_config["name"] = (std::string) m_client.read(TAG_CONFIG_CW_NAME, NS);
        m_config["address"] = (std::string) m_client.read(TAG_CONFIG_CW_ADDRESS, NS);

        m_serial = (std::string) m_client.read(TAG_CONFIG_CW_SERIAL, NS);
        m_config["serial"] = m_serial;
        m_config["sw_version"] = (std::string) m_client.read(TAG_CONFIG_SW_VERSION, NS);
        m_config["hw_version"] = (std::string) m_client.read(TAG_CONFIG_SW_VERSION, NS);

        m_hw_sn = m_client.read(TAG_CONFIG_HW_SN, NS);
        opcua::variant nic = m_client.read(TAG_CONFIG_HW_NIC, NS);
        if (nic.array_size() == 10)
            memcpy(m_hw_nic, (uint8_t*) nic, 10);

        //qDebug()<<"OPCUA_READ_CONFIG"<<QString::fromStdString(m_config.dump());

    }
    catch(std::exception& _ex)
    {
        alles::logger::instance().log_error(fmt::format("Exception in read_config(): {}", _ex.what()));
    }
}

// ------------------------------------------------------------------------------------------

void carwash::impl::ping()
{
    try
    {
        m_client.write(TAG_PING, true, NS);
        m_client.write(TAG_SW_VERSION, VERSION, NS);
    }
    catch(...)
    {
    }
}

// ------------------------------------------------------------------------------------------

void carwash::impl::subscribe()
{
    if (!m_subscribed)
    {
        try
        {
            using namespace std::placeholders;
            std::string tag;

            m_client.subscribe(TAG_DATE, NS, std::bind(&impl::on_date_changed, this, _1));
            m_client.subscribe(TAG_TIME, NS, std::bind(&impl::on_time_changed, this, _1));

            m_client.subscribe(TAG_CONFIG_SW_VERSION, NS, std::bind(&impl::on_sw_version, this, _1));

            for (int i = 0; i < 8; ++i)
            {
                // events for new posts

                tag = fmt::format(TAG_POST_COLLECT, i);
                m_client.subscribe(tag, NS, std::bind(&impl::on_collect, this, i, false, _1));

                tag = fmt::format(TAG_POST_ADD_MONEY, i);
                m_client.subscribe(tag, NS, std::bind(&impl::on_add_money, this, i, _1));

                tag = fmt::format(TAG_WASH_NUM, i);
                m_client.subscribe(tag, NS, std::bind(&impl::on_complete, this, i, 0, _1));

                tag = fmt::format(TAG_POST_INCASS, i);
                m_client.subscribe(tag, NS, std::bind(&impl::on_collect, this, i, true, _1));
            }

            for (unsigned i = 0; i < 6; ++i)
            {
                tag = fmt::format(TAG_VACU_NUM, i);
                m_client.subscribe(tag, NS, std::bind(&impl::on_complete, this, i, 1, _1));

                tag = fmt::format(TAG_VPOST_ADD_MONEY, i);
                m_client.subscribe(tag, NS, std::bind(&impl::on_add_money, this, 40 + i, _1));
            }

            for (int i = 0; i < 2; ++i)
            {
                tag = fmt::format(TAG_PAYCENTER_COLLECT, i);
                m_client.subscribe(tag, NS, std::bind(&impl::on_collect, this, 30 + i, false, _1));
            }

            for (int i = 0; i < 3; ++i)
            {
                tag = fmt::format(TAG_VACUUM_COLLECT, i);
                m_client.subscribe(tag, NS, std::bind(&impl::on_collect, this, 40 + i, false, _1));
            }

            // UI events subscriptions

            m_client.subscribe(TAG_ST1_BUTTON, NS, std::bind(&impl::on_stat_main_show, this, _1));
            m_client.subscribe(TAG_ST2_BUTTON, NS, std::bind(&impl::on_stat_hourly_show, this, _1));
            m_client.subscribe(TAG_ST3_BUTTON, NS, std::bind(&impl::on_stat_daily_show, this, _1));
            m_client.subscribe(TAG_ST4_BUTTON, NS, std::bind(&impl::on_stat_progs_show, this, _1));
            m_client.subscribe(TAG_ST5_BUTTON, NS, std::bind(&impl::on_collect_history_show, this, _1));

            // chemistry counters

            unsigned chemistry_count = m_client.array_size("::AsGlobalPV:ProcessEventExtern.chemistryCons", NS);
            qDebug() << "CARWASH: subscribe(): chemistry_count = " << chemistry_count;
            for (unsigned i = 0; i < chemistry_count; i++) {
                tag = fmt::format(TAG_CHEMISTRY, i);
                m_client.subscribe(tag, NS, std::bind(&impl::on_chemistry, this, (chemistry_type) i, _1));
            }

            m_client.subscribe(TAG_KKT_INSTALLED, NS, std::bind(&impl::on_kkt_enabled, this, _1));
            m_client.subscribe(TAG_KKT_PRINT_ENABLED, NS, std::bind(&impl::on_kkt_printer_enabled, this, _1));
            m_client.subscribe(TAG_KKT_ADDRESS, NS, std::bind(&impl::on_kkt_address, this, _1));
            m_client.subscribe(TAG_KKT_PLACE, NS, std::bind(&impl::on_kkt_place, this, _1));
            m_client.subscribe(TAG_KKT_TAX_SELECTED, NS, std::bind(&impl::on_kkt_tax_selected, this, _1));
            m_client.subscribe(TAG_KKT_VAT_SELECTED, NS, std::bind(&impl::on_kkt_vat_selected, this, _1));
            m_client.subscribe(TAG_KKT_ITEM_TEXT, NS, std::bind(&impl::on_kkt_item_text, this, _1));

            m_client.subscribe(TAG_BONUS_ENABLE, NS, std::bind(&impl::on_bonus_enable, this, _1));
            m_client.subscribe(TAG_BONUS_PASSWORD, NS, std::bind(&impl::on_bonus_password, this, _1));

            m_client.subscribe(TAG_PRICE, NS, std::bind(&impl::read_and_send_progs, this));
            m_client.subscribe(TAG_PROGS, NS, std::bind(&impl::read_and_send_progs, this));

            m_client.subscribe(TAG_VACUUM_PRICE, NS, std::bind(&impl::read_and_send_progs, this));
            m_client.subscribe(TAG_VACUUM_PROGS, NS, std::bind(&impl::read_and_send_progs, this));

            alles::logger::instance().log_status("OPC-UA subscribe finished");
        }
        catch(std::exception& _ex)
        {
            std::string s = "Exception during subscription: ";
            s+= _ex.what();
            alles::logger::instance().log_error(s);
        }

        m_subscribed = true;
    }
}

// ------------------------------------------------------------------------------------------
void carwash::impl::on_sw_version(opcua::variant _val)
{
    try {
        using namespace std::placeholders;
        std::string sw_version = (std::string) _val;
        unsigned sw_number = QString::fromStdString(sw_version.c_str()).split(".").join("").toInt();
        qDebug() << "CARWASH: subscribe(): sw_version = " << sw_number;
        if (sw_number >= 473) {
            m_client.subscribe(TAG_CONFIG_CURRENCY_DIGITS, NS, std::bind(&impl::on_currency_digits_changed, this, _1));
        }
    } catch(std::exception& _ex) {
        logger::instance().log_error(fmt::format("Exception during on_sw_version: {0}", _ex.what()));
    }
}

void carwash::impl::on_currency_digits_changed(opcua::variant _val)
{
    try {
        uint8_t v = _val;
        logger::instance().log_debug(fmt::format("CARWASH: on_currency_digits_changed: {0}", v));
        if (v > 0) {
            MoneyScaleFactor = pow(10, v);
        }
    } catch(std::exception& _ex) {
        logger::instance().log_error(fmt::format("Exception during on_currency_digits_changed: {0}", _ex.what()));
    }
}

void carwash::impl::on_add_money(unsigned _post_id, opcua::variant _val)
{
    try
    {
        uint8_t v = _val;

        if (v > 0)  {
            m_service_money_counter[_post_id] = v;
            logger::instance().log_debug(fmt::format("Add money received... {0}", v));
        } else {
            if (m_service_money_counter.count(_post_id) && m_service_money_counter.at(_post_id) > 0) {
                uint16_t s = 0;
                if (_post_id < 8)  {
                    s = m_client.read(TAG_SERVICE_MONEY, NS);
                    uint32_t sum = s * m_service_money_counter.at(_post_id);
                    //logger::instance().log_debug(fmt::format("Store service money on {0}... {1}", _post_id, sum));

                    storage::instance().store_money(money_service, _post_id, 0, sum, nl::json());
                } else
                if (_post_id >= 40 && _post_id < 46) {
                    s = m_client.read(TAG_VSERVICE_MONEY, NS);
                    uint32_t sum = s * m_service_money_counter.at(_post_id);
                    unsigned unit_id = (_post_id-40) / 2;
                    //logger::instance().log_debug(fmt::format("Store service money on {0}... {1}", 40+unit_id, sum));
                    storage::instance().store_money(money_service, 40 + unit_id, 0, sum, nl::json());
                }
            }
            m_service_money_counter[_post_id] = 0;
        }
    } catch(std::exception& _ex) {
        logger::instance().log_error(fmt::format("Exception during add_money: {0}", _ex.what()));
    }
}

// ------------------------------------------------------------------------------------------

#define TAG_POST_INCASS_USER "::AsGlobalPV:ProcessEventExtern.collectMoneyUser[{0}]"
#define TAG_USER             "::AsGlobalPV:System.userActiveName"

void carwash::impl::on_collect(int _unit_id, bool _old, opcua::variant _val)
{
    try
    {
        if (_val)
        {
            std::string user;

            if (_old)
                user = (std::string) m_client.read(fmt::format(TAG_POST_INCASS_USER, _unit_id), NS);
            else
                user = (std::string) m_client.read(TAG_USER, NS);

            queue_background_task([this, user, _unit_id, _old]()
            {
                try
                {
                    storage::instance().store_collect_info(_unit_id, user);
                }
                catch(std::exception& _ex)
                {
                    std::string s = "Exception in background task on_collect(): ";
                    s += _ex.what();
                    alles::logger::instance().log_error(s);
                }

                if (_old)
                {
                    queue_task([this, _unit_id]() // set result in main thread
                    {
                        try
                        {
                            m_client.write(fmt::format(TAG_POST_INCASS, _unit_id), false, NS);
                            update_finance(_unit_id);
                        }
                        catch(std::exception& _ex)
                        {
                            std::string s = "Exception while setting on_collect() results: ";
                            s += _ex.what();
                            alles::logger::instance().log_error(s);
                        }
                    });
                }
                else
                {
                    queue_task([this, _unit_id]() // set result in main thread
                    {
                        try
                        {
                            update_finance(_unit_id);
                        }
                        catch(std::exception& _ex)
                        {
                            std::string s = "Exception while setting on_collect() results: ";
                            s += _ex.what();
                            alles::logger::instance().log_error(s);
                        }
                    });
                }
            });


        }
    }
    catch(std::exception& _ex)
    {
        std::string s = fmt::format("Exception in on_collect(): {0}", _ex.what());
        alles::logger::instance().log_error(s);
    }
}

// ------------------------------------------------------------------------------------------

#define TAG_WASH_MONEY       "::AsGlobalPV:ProcessEventExtern.washComplete[{0}].money"
#define TAG_WASH_TOTAL_TIME  "::AsGlobalPV:ProcessEventExtern.washComplete[{0}].totalTime"
#define TAG_WASH_PROGS       "::AsGlobalPV:ProcessEventExtern.washComplete[{0}].progTime"

#define TAG_VACU_MONEY       "::AsGlobalPV:ProcessEventExtern.vacuumComplete[{0}].money"
#define TAG_VACU_TOTAL_TIME  "::AsGlobalPV:ProcessEventExtern.vacuumComplete[{0}].totalTime"
#define TAG_VACU_PROGS       "::AsGlobalPV:ProcessEventExtern.vacuumComplete[{0}].progTime"

void carwash::impl::on_complete(int _idx, int _type, opcua::variant _val)
{
    //qDebug()<<"on_complete";//<<_idx<<_type<<(int)_val;
    try
    {
        uint32_t val_int = _val;
        if (val_int != 0)
        {
            unsigned db_id = _idx;
            std::string tag_num   = TAG_WASH_NUM,
                        tag_money = TAG_WASH_MONEY,
                        tag_time  = TAG_WASH_TOTAL_TIME,
                        tag_progs = TAG_WASH_PROGS;

            if (_type == 1) // Vacuum
            {
                db_id = _idx / 2;
                db_id += 40; // adjust ID to store in single table
                tag_num   = TAG_VACU_NUM;
                tag_money = TAG_VACU_MONEY;
                tag_time  = TAG_VACU_TOTAL_TIME;
                tag_progs = TAG_VACU_PROGS;
            }

            auto money = m_client.read(fmt::format(tag_money, _idx), NS);
            auto time =  m_client.read(fmt::format(tag_time, _idx), NS);
            auto progs = m_client.read(fmt::format(tag_progs, _idx), NS);

            queue_background_task([=]() // run in background
            {
                uint32_t i_money = money;
                uint32_t i_time = time;
                uint32_t p_progs[28]; //DEBUG
                memset(p_progs, 0, 28*sizeof(uint32_t));
                qDebug()<<"queue_background_task";
                unsigned sz = std::min(progs.array_size(), (unsigned) 28);
                memcpy(p_progs, (uint32_t*) progs, sz * sizeof(uint32_t));
                qDebug()<<"progs.array_size()"<<sz;
                try
                {
                    storage::instance().store_wash(db_id, i_money, i_time, p_progs);
                }
                catch(std::exception& _ex)
                {
                    std::string s = "Exception in background task on_complete(): ";
                    s += _ex.what();
                    alles::logger::instance().log_error(s);
                }

                queue_task([=]() // set result in main thread
                {
                    try
                    {
                        m_client.write(fmt::format(tag_num, _idx), (uint32_t) 0, NS);
                        m_client.write(fmt::format(tag_money, _idx), (uint32_t) 0,  NS);
                        m_client.write(fmt::format(tag_time, _idx), (uint32_t) 0, NS);

                        if (progs.is_array())
                        {
                            uint32_t* arr = (uint32_t*) progs;
                            memset(arr, 0, progs.array_size() * sizeof(uint32_t));
                        }

                        m_client.write(fmt::format(tag_progs, _idx), progs, NS);
                    }
                    catch(std::exception& _ex)
                    {
                        std::string s = "Exception while setting on_complete() results: ";
                        s += _ex.what();
                        alles::logger::instance().log_error(s);
                    }
                });
            });
        }
    }
    catch(std::exception& _ex)
    {
        std::string s = "Exception in monitoring function on_complete(): ";
        s += _ex.what();
        alles::logger::instance().log_error(s);
    }
}

// ------------------------------------------------------------------------------------------

void carwash::impl::on_chemistry(chemistry_type _type, opcua::variant _val)
{
    try
    {
        //qDebug()<<"void carwash::impl::on_chemistry(chemistry_type _type, opcua::variant _val)"<<_type<<(uint32_t)_val;

        uint32_t val_int = (uint32_t) _val;
        //_type=chemistry_insect;
        //_val=2000;
        if (val_int != 0)
        {
            queue_background_task([this, _type, val_int]() mutable
            {
                bool reset = false;

                try
                {
                    //qDebug()<<"store_chemistry";
                    reset = storage::instance().store_chemistry(_type,val_int);
                }
                catch(std::exception& _ex)
                {
                    std::string s = "Exception in background task on_chemistry(): ";
                    s += _ex.what();
                    alles::logger::instance().log_error(s);
                }

                if (reset)
                {
                    queue_task([this]() // set result in main thread
                    {
                        try
                        {
                            alles::logger::instance().log_debug("Resetting chemistry...");

                            std::string tag = fmt::format(TAG_CHEMISTRY, chemistry_basic);
                            m_client.write(tag, (uint32_t) 0, NS);

                            tag = fmt::format(TAG_CHEMISTRY, chemistry_foam);
                            m_client.write(tag, (uint32_t) 0, NS);

                            tag = fmt::format(TAG_CHEMISTRY, chemistry_polimer);
                            m_client.write(tag, (uint32_t) 0, NS);

                            tag = fmt::format(TAG_CHEMISTRY, chemistry_shampoo);
                            m_client.write(tag, (uint32_t) 0, NS);

                            tag = fmt::format(TAG_CHEMISTRY, chemistry_wax);
                            m_client.write(tag, (uint32_t) 0, NS);

                            tag = fmt::format(TAG_CHEMISTRY, chemistry_brush);
                            m_client.write(tag, (uint32_t) 0, NS);

                            tag = fmt::format(TAG_CHEMISTRY, chemistry_wheels);
                            m_client.write(tag, (uint32_t) 0, NS);

                            tag = fmt::format(TAG_CHEMISTRY, chemistry_insect);
                            m_client.write(tag, (uint32_t) 0, NS);
                            tag = fmt::format(TAG_CHEMISTRY, chemistry_color);
                            m_client.write(tag, (uint32_t) 0, NS);
                        }
                        catch(std::exception& _ex)
                        {
                            std::string s = "Exception while setting on_chemistry() results: ";
                            s += _ex.what();
                            alles::logger::instance().log_error(s);
                        }
                    });
                }
            });
        }
    }
    catch(std::exception& _ex)
    {
        std::string s = "Exception in monitoring function on_chemistry(): ";
        s += _ex.what();
        alles::logger::instance().log_error(s);
    }
}

// ------------------------------------------------------------------------------------------
// MAIN STATISTIC PAGE

#define TAG_ST1_BONUS_SUM   "::Statistic:GeneralStatDataExtern.{0}[{1}].bonusSum"
#define TAG_ST1_CARDS_SUM   "::Statistic:GeneralStatDataExtern.{0}[{1}].cardsSum"
#define TAG_ST1_CASH_SUM    "::Statistic:GeneralStatDataExtern.{0}[{1}].cashSum"
#define TAG_ST1_SERVICE_SUM "::Statistic:GeneralStatDataExtern.{0}[{1}].serviceSum"
#define TAG_ST1_CARS        "::Statistic:GeneralStatDataExtern.{0}[{1}].carsAmount"
#define TAG_ST1_TIME        "::Statistic:GeneralStatDataExtern.{0}[{1}].carsTime"
#define TAG_ST1_MONEY       "::Statistic:GeneralStatDataExtern.{0}[{1}].carsMoney"
#define TAG_ST1_TIME2       "::Statistic:GeneralStatDataExtern.{0}[{1}].totalTime"

#define TAG_WASH_TIME       "::AsGlobalPV:Recipe.CarWashingTime"

void carwash::impl::on_stat_main_show(opcua::variant _val)
{
    try
    {
        if ((bool) _val) // button clicked
        {
            range r;
            r.start_year  = m_client.read("::Statistic:GeneralStatPeriodParam.yearFrom",  NS);
            r.start_month = m_client.read("::Statistic:GeneralStatPeriodParam.monthFrom", NS);
            r.start_day   = m_client.read("::Statistic:GeneralStatPeriodParam.dayFrom",   NS);
            r.end_year    = m_client.read("::Statistic:GeneralStatPeriodParam.yearTo",    NS);
            r.end_month   = m_client.read("::Statistic:GeneralStatPeriodParam.monthTo",   NS);
            r.end_day     = m_client.read("::Statistic:GeneralStatPeriodParam.dayTo",     NS);

            uint16_t filter = m_client.read(TAG_WASH_TIME, NS);

            alles::logger::instance().log_status("Show main stat requested...");

            queue_background_task([r, filter, this]() mutable
            {
                nl::json jresult1, jresult2;

                try
                {
                    storage::instance().get_main_stat_data(r, 0, jresult1, 8, filter);
                    storage::instance().get_main_stat_data(r, 1, jresult2, 6, 0);
                }
                catch(std::exception& _ex)
                {
                    std::string s = "Exception in background task on_stat_main_show(): ";
                    s += _ex.what();
                    alles::logger::instance().log_error(s);
                }

                auto write_result = [this](nl::json jresult, std::string _type) // set result in main thread
                {
                    try
                    {
                        unsigned i = 0;
                        for (auto it = jresult.begin(); it != jresult.end(); ++it, ++i)
                        {
                            const nl::json& j = (*it);
                            opcua::variant v = (uint32_t) 0;

                            if (j.count("sum_cash")) v = (uint32_t) j["sum_cash"];
                            m_client.write(fmt::format(TAG_ST1_CASH_SUM, _type, i), v, NS);

                            v = 0;
                            if (j.count("sum_cards")) v = (uint32_t) j["sum_cards"];
                            m_client.write(fmt::format(TAG_ST1_CARDS_SUM, _type, i), v, NS);

                            v = 0;
                            if (j.count("sum_bonus")) v = (uint32_t) j["sum_bonus"];
                            m_client.write(fmt::format(TAG_ST1_BONUS_SUM, _type, i), v, NS);

                            v = 0;
                            if (j.count("sum_service")) v = (uint32_t) j["sum_service"];
                            m_client.write(fmt::format(TAG_ST1_SERVICE_SUM, _type, i), v, NS);

                            v = 0;
                            if (j.count("washes")) v = (uint32_t) j["washes"];
                            m_client.write(fmt::format(TAG_ST1_CARS, _type, i), v, NS);

                            v = 0;
                            if (j.count("money_spent")) v = (uint32_t) j["money_spent"];
                            m_client.write(fmt::format(TAG_ST1_MONEY, _type, i), v, NS);

                            v = 0;
                            if (j.count("duration")) v = (uint32_t) j["duration"];
                            m_client.write(fmt::format(TAG_ST1_TIME, _type, i), v, NS);
                            m_client.write(fmt::format(TAG_ST1_TIME2, _type, i), v, NS);
                        }
                    }
                    catch(std::exception& _ex)
                    {
                        std::string s = "Exception while setting on_stat_main_show() results: ";
                        s += _ex.what();
                        alles::logger::instance().log_error(s);
                    }
                };

                queue_task(std::bind(write_result, jresult1, "post"));
                queue_task(std::bind(write_result, jresult2, "vacuum"));

                queue_task([this, r]()
                {
                    try
                    {
                        m_client.write("::Statistic:GeneralStatPeriodParam.yearFrom", r.start_year, NS);
                        m_client.write("::Statistic:GeneralStatPeriodParam.monthFrom", r.start_month, NS);
                        m_client.write("::Statistic:GeneralStatPeriodParam.dayFrom", r.start_day, NS);
                        m_client.write("::Statistic:GeneralStatPeriodParam.yearTo", r.end_year, NS);
                        m_client.write("::Statistic:GeneralStatPeriodParam.monthTo",  r.end_month, NS);
                        m_client.write("::Statistic:GeneralStatPeriodParam.dayTo", r.end_day, NS);

                        alles::logger::instance().log_status("Setting result for stat main...");
                        m_client.write(TAG_ST1_BUTTON, false, NS);
                    }
                    catch(std::exception& _ex)
                    {
                        std::string s = "Exception while setting on_stat_main_show() results: ";
                        s += _ex.what();
                        alles::logger::instance().log_error(s);
                    }
                });

            });
        }
    }
    catch(std::exception& _ex)
    {
        std::string s = "Exception in monitoring function on_stat_main_show(): ";
        s += _ex.what();
        alles::logger::instance().log_error(s);
    }
}

// ------------------------------------------------------------------------------------------
// STATISTIC PROGS SHOW

#define TAG_ST4_PROG   "::Statistic:ProgUsageStatDataExtern.prog"
#define TAG_ST4_CHEM   "::Statistic:ProgUsageStatDataExtern.chem"
#define TAG_ST4_DAYS   "::Statistic:ProgUsageStatDataExtern.numberOfDays"

void carwash::impl::on_stat_progs_show(opcua::variant _val)
{
    try
    {
        if (_val) // button clicked
        {
            range r;
            r.start_year    = m_client.read("::Statistic:ProgUsageStatPeriodParam.yearFrom", NS);
            r.start_month   = m_client.read("::Statistic:ProgUsageStatPeriodParam.monthFrom", NS);
            r.start_day     = m_client.read("::Statistic:ProgUsageStatPeriodParam.dayFrom", NS);
            r.end_year      = m_client.read("::Statistic:ProgUsageStatPeriodParam.yearTo", NS);
            r.end_month     = m_client.read("::Statistic:ProgUsageStatPeriodParam.monthTo", NS);
            r.end_day       = m_client.read("::Statistic:ProgUsageStatPeriodParam.dayTo", NS);

            alles::logger::instance().log_status("Show progs stat requested...");

            queue_background_task([r, this]() mutable
            {
                nl::json jresult;
                uint16_t days = 0;

                uint32_t arr_prog[28]= {0};
                uint32_t arr_chem[9]  = {0};

                try
                {
                    storage::instance().get_progs_stat_data(r, arr_prog, 8);
                    storage::instance().get_chemistry_data(r, days, arr_chem);
                    //for(int i=0; i<28;i++)
                     //   qDebug()<<"I = "<<i<<arr_prog[i];
                }
                catch(std::exception& _ex)
                {
                    std::string s = "Exception in background task on_stat_progs_show(): ";
                    s += _ex.what();
                    alles::logger::instance().log_error(s);
                }

                queue_task([this, r, jresult, days, arr_prog, arr_chem]() // set result in main thread
                {
                    try
                    {
                        m_client.write("::Statistic:ProgUsageStatPeriodParam.yearFrom", r.start_year, NS);
                        m_client.write("::Statistic:ProgUsageStatPeriodParam.monthFrom", r.start_month, NS);
                        m_client.write("::Statistic:ProgUsageStatPeriodParam.dayFrom", r.start_day, NS);
                        m_client.write("::Statistic:ProgUsageStatPeriodParam.yearTo", r.end_year, NS);
                        m_client.write("::Statistic:ProgUsageStatPeriodParam.monthTo",  r.end_month, NS);
                        m_client.write("::Statistic:ProgUsageStatPeriodParam.dayTo", r.end_day, NS);
                        //nl::json pc = alles::settings::instance().get("program_count");
                        //nl::json hc = alles::settings::instance().get("chemistry_count");
                        //qDebug()<<"program_count"<<pc.dump().c_str();
                        unsigned prog_len = m_client.array_size(TAG_ST4_PROG,NS);
                        unsigned chem_len = m_client.array_size(TAG_ST4_CHEM,NS);

                        opcua::variant v1((uint32_t*) arr_prog, prog_len);
                        opcua::variant v2((uint32_t*) arr_chem, chem_len);
                        //qDebug()<<"program_count1"<<m_client.array_size(TAG_ST4_PROG,NS);
                        m_client.write(TAG_ST4_PROG, v1, NS);
                        m_client.write(TAG_ST4_CHEM, v2, NS);
                        m_client.write(TAG_ST4_DAYS, days, NS);
                        //qDebug()<<"write stats"<<arr_prog[26];
                        alles::logger::instance().log_status("Setting result for progs stat...");
                        m_client.write(TAG_ST4_BUTTON, false, NS);
                    }
                    catch(std::exception& _ex)
                    {
                        std::string s = "Exception while setting on_stat_progs_show() results: ";
                        s += _ex.what();
                        alles::logger::instance().log_error(s);
                    }
                });
            });
        }
    }
    catch(std::exception& _ex)
    {
        std::string s = "Exception in monitoring function on_stat_progs_show(): ";
        s += _ex.what();
        alles::logger::instance().log_error(s);
    }
}

// ------------------------------------------------------------------------------------------
// STATISTIC HOURLY SHOW

void carwash::impl::on_stat_hourly_show(opcua::variant _val)
{
    try
    {
        if (_val) // button clicked
        {
            range r;
            r.start_year  = m_client.read("::Statistic:HoursStatPeriodParam.yearFrom", NS);
            r.start_month = m_client.read("::Statistic:HoursStatPeriodParam.monthFrom", NS);
            r.start_day   = m_client.read("::Statistic:HoursStatPeriodParam.dayFrom", NS);
            r.end_year    = m_client.read("::Statistic:HoursStatPeriodParam.yearTo", NS);
            r.end_month   = m_client.read("::Statistic:HoursStatPeriodParam.monthTo", NS);
            r.end_day     = m_client.read("::Statistic:HoursStatPeriodParam.dayTo", NS);

            alles::logger::instance().log_status("Show hourly stat requested...");

            queue_background_task([r, this]() mutable
            {
                uint16_t arr[24]; memset(arr, 0, 24 * sizeof(uint16_t));
                uint16_t arr2[24]; memset(arr2, 0, 24 * sizeof(uint16_t));
                uint16_t days = 0;
                uint32_t sum = 0;
                uint32_t avg = 0;

                try
                {
                    storage::instance().get_hourly_data(r, arr, arr2,sum,avg,days);
                }
                catch(std::exception& _ex)
                {
                    std::string s = "Exception in background task on_stat_hourly_show(): ";
                    s += _ex.what();
                    alles::logger::instance().log_error(s);
                }

                opcua::variant v(arr, 24);
                opcua::variant v2(arr2, 24);

                queue_task([this, r, v, v2, sum, avg, days]() // set result in main thread
                {
                    try
                    {
                        m_client.write("::Statistic:HoursStatPeriodParam.yearFrom", r.start_year, NS);
                        m_client.write("::Statistic:HoursStatPeriodParam.monthFrom", r.start_month, NS);
                        m_client.write("::Statistic:HoursStatPeriodParam.dayFrom", r.start_day, NS);
                        m_client.write("::Statistic:HoursStatPeriodParam.yearTo", r.end_year, NS);
                        m_client.write("::Statistic:HoursStatPeriodParam.monthTo",  r.end_month, NS);
                        m_client.write("::Statistic:HoursStatPeriodParam.dayTo", r.end_day, NS);

                        m_client.write("::Statistic:HoursStatDataExtern.hoursPeriod", v, NS);
                        m_client.write("::Statistic:HoursStatDataExtern.hoursToday", v2, NS);

                        m_client.write("::Statistic:HoursStatDataExtern.sumForPeriod", sum, NS);
                        m_client.write("::Statistic:HoursStatDataExtern.avgForPeriod", avg, NS);
                        m_client.write("::Statistic:HoursStatDataExtern.numberOfDays", days, NS);

                        alles::logger::instance().log_status("Setting result for hourly stat...");
                        m_client.write(TAG_ST2_BUTTON, false, NS);
                    }
                    catch(std::exception& _ex)
                    {
                        std::string s = "Exception while setting on_stat_hourly_show() results: ";
                        s += _ex.what();
                        alles::logger::instance().log_error(s);
                    }
                });
            });
        }
    }
    catch(std::exception& _ex)
    {
        std::string s = "Exception in monitoring function on_stat_hourly_show(): ";
        s += _ex.what();
        alles::logger::instance().log_error(s);
    }
}

// ------------------------------------------------------------------------------------------
// STATISTIC DAILY SHOW

void carwash::impl::on_stat_daily_show(opcua::variant _val)
{
    try
    {
        if (_val) // button clicked
        {
            range r;
            r.start_year  = m_client.read("::Statistic:DaysStatPeriodParam.yearFrom", NS);
            r.start_month = m_client.read("::Statistic:DaysStatPeriodParam.monthFrom", NS);
            r.start_day   = m_client.read("::Statistic:DaysStatPeriodParam.dayFrom", NS);
            r.end_year    = m_client.read("::Statistic:DaysStatPeriodParam.yearTo", NS);
            r.end_month   = m_client.read("::Statistic:DaysStatPeriodParam.monthTo", NS);
            r.end_day     = m_client.read("::Statistic:DaysStatPeriodParam.dayTo", NS);

            alles::logger::instance().log_status("Show daily stat requested...");

            queue_background_task([r, this]() mutable
            {
                const unsigned arr_size = 1100;
                uint16_t days = 0;
                time_t start = 0;

                uint32_t arr[arr_size]; memset(arr, 0, arr_size * sizeof(uint32_t));

                try
                {
                    storage::instance().get_daily_data(r, arr,days, start);
                }
                catch(std::exception& _ex)
                {
                    std::string s = "Exception in background task on_stat_daily_show(): ";
                    s += _ex.what();
                    alles::logger::instance().log_error(s);
                }

                opcua::variant v(arr, arr_size);

                queue_task([this, r, v, days, start]() // set result in main thread
                {
                    try
                    {
                        m_client.write("::Statistic:DaysStatPeriodParam.yearFrom", r.start_year, NS);
                        m_client.write("::Statistic:DaysStatPeriodParam.monthFrom", r.start_month, NS);
                        m_client.write("::Statistic:DaysStatPeriodParam.dayFrom", r.start_day, NS);
                        m_client.write("::Statistic:DaysStatPeriodParam.yearTo", r.end_year, NS);
                        m_client.write("::Statistic:DaysStatPeriodParam.monthTo",  r.end_month, NS);
                        m_client.write("::Statistic:DaysStatPeriodParam.dayTo", r.end_day, NS);

                        m_client.write("::Statistic:DaysStatDataExtern.trendForPeriod", v, NS);
                        m_client.write("::Statistic:DaysStatDataExtern.numberOfDays", days, NS);
                        m_client.write("::Statistic:DaysStatDataExtern.timestamp", start, NS);

                        alles::logger::instance().log_status("Setting result for daily stat...");
                        m_client.write(TAG_ST3_BUTTON, false, NS);
                    }
                    catch(std::exception& _ex)
                    {
                        std::string s = "Exception while setting on_stat_daily_show() results: ";
                        s += _ex.what();
                        alles::logger::instance().log_error(s);
                    }
                });
            });
        }
    }
    catch(std::exception& _ex)
    {
        std::string s = "Exception in monitoring function on_stat_daily_show(): ";
        s += _ex.what();
        alles::logger::instance().log_error(s);
    }
}

// ------------------------------------------------------------------------------------------
// COLLECT HISTORY SHOW

#define TAG_PC_LIST_DATE "::Statistic:CollectStat{0}DataExtern.list[{1}].date"
#define TAG_PC_LIST_USER "::Statistic:CollectStat{0}DataExtern.list[{1}].user"
#define TAG_PC_LIST_POST "::Statistic:CollectStat{0}DataExtern.list[{1}].post[{2}]"
#define TAG_PC_NUM       "::Statistic:CollectStat{0}DataExtern.num"

void carwash::impl::on_collect_history_show(opcua::variant _val)
{
    try
    {
        if (_val) // button clicked
        {
            range r;
            r.start_year    = m_client.read("::Statistic:CollectStatPeriodParam.yearFrom", NS);
            r.start_month   = m_client.read("::Statistic:CollectStatPeriodParam.monthFrom", NS);
            r.start_day     = m_client.read("::Statistic:CollectStatPeriodParam.dayFrom", NS);
            r.end_year      = m_client.read("::Statistic:CollectStatPeriodParam.yearTo", NS);
            r.end_month     = m_client.read("::Statistic:CollectStatPeriodParam.monthTo", NS);
            r.end_day       = m_client.read("::Statistic:CollectStatPeriodParam.dayTo", NS);

            alles::logger::instance().log_status("Show collect history requested...");

            queue_background_task([r, this]() mutable
            {
                nl::json jresult;

                try
                {
                    storage::instance().get_collect_history(r, jresult);
                }
                catch(std::exception& _ex)
                {
                    std::string s = "Exception in background task on_collect_history_show(): ";
                    s += _ex.what();
                    alles::logger::instance().log_error(s);
                }

                queue_task([this, r, jresult]() // set result in main thread
                {
                    try
                    {
                        m_client.write("::Statistic:CollectStatPeriodParam.yearFrom",  r.start_year, NS);
                        m_client.write("::Statistic:CollectStatPeriodParam.monthFrom", r.start_month, NS);
                        m_client.write("::Statistic:CollectStatPeriodParam.dayFrom",   r.start_day, NS);
                        m_client.write("::Statistic:CollectStatPeriodParam.yearTo",    r.end_year, NS);
                        m_client.write("::Statistic:CollectStatPeriodParam.monthTo",   r.end_month, NS);
                        m_client.write("::Statistic:CollectStatPeriodParam.dayTo",     r.end_day, NS);

                        std::string tag[] = { "", "Paycenter", "Vacuum" };
                        unsigned base[] = { 0, 30, 40 };
                        unsigned max[]  = { 8, 32, 43 };
                        unsigned num[3] = { 0,  0,  0 };

                        for (auto it = jresult.rbegin(); it != jresult.rend(); ++it)
                        {
                            const nl::json& j = *it;

                            std::string user;
                            if (j.count("user"))
                                user = j.at("user");

                            std::string date = it.key();

                            // collect data for posts

                            for (unsigned type = 0; type < 3; ++type)
                            {
                                unsigned n_pcs = 0;
                                for (unsigned ii = base[type]; ii < max[type]; ++ii)
                                {
                                    auto p = fmt::format(TAG_PC_LIST_POST, tag[type], num[type], ii - base[type]);
                                    std::string unit = fmt::format("unit_{0}", ii);
                                    if (j.count(unit))
                                    {
                                        uint32_t sum = j.at(unit);
                                        m_client.write(p, sum, NS);
                                        n_pcs++;
                                    }
                                    else
                                    {
                                        m_client.write(p, (uint32_t) 0, NS);
                                    }
                                }

                                if (n_pcs > 0)
                                {
                                    auto d = fmt::format(TAG_PC_LIST_DATE, tag[type], num[type]);
                                    m_client.write(d, date, NS);

                                    auto u = fmt::format(TAG_PC_LIST_USER, tag[type], num[type]);
                                    m_client.write(u, user, NS);

                                    num[type]++;
                                }
                            }
                        }

                        for (unsigned type = 0; type < 3; ++type)
                            m_client.write(fmt::format(TAG_PC_NUM, tag[type]), (uint8_t) num[type], NS);

                        alles::logger::instance().log_status("Setting result for collect history...");
                        m_client.write(TAG_ST5_BUTTON, false, NS);
                    }
                    catch(std::exception& _ex)
                    {
                        std::string s = "Exception while setting on_collect_history_show() results: ";
                        s += _ex.what();
                        alles::logger::instance().log_error(s);
                    }
                });
            });
        }
    }
    catch(std::exception& _ex)
    {
        std::string s = "Exception in monitoring function on_collect_history_show(): ";
        s += _ex.what();
        alles::logger::instance().log_error(s);
    }
}

// ------------------------------------------------------------------------------------------

#define TAG_FINANCE_CASH_SUM            "::Finance:{0}CashExtern[{1}].CashSum"
#define TAG_FINANCE_CASH_BILLS_SUM      "::Finance:{0}CashExtern[{1}].CashDetails.BillsSum"
#define TAG_FINANCE_CASH_BILLS_TOTAL    "::Finance:{0}CashExtern[{1}].CashDetails.BillsAmount"
#define TAG_FINANCE_CASH_BILLS_10       "::Finance:{0}CashExtern[{1}].CashDetails.BillsAmount_10"
#define TAG_FINANCE_CASH_BILLS_50       "::Finance:{0}CashExtern[{1}].CashDetails.BillsAmount_50"
#define TAG_FINANCE_CASH_BILLS_100      "::Finance:{0}CashExtern[{1}].CashDetails.BillsAmount_100"
#define TAG_FINANCE_CASH_BILLS_200      "::Finance:{0}CashExtern[{1}].CashDetails.BillsAmount_200"
#define TAG_FINANCE_CASH_BILLS_500      "::Finance:{0}CashExtern[{1}].CashDetails.BillsAmount_500"
#define TAG_FINANCE_CASH_BILLS_1000     "::Finance:{0}CashExtern[{1}].CashDetails.BillsAmount_1000"
#define TAG_FINANCE_CASH_BILLS_2000     "::Finance:{0}CashExtern[{1}].CashDetails.BillsAmount_2000"
#define TAG_FINANCE_CASH_BILLS_5000     "::Finance:{0}CashExtern[{1}].CashDetails.BillsAmount_5000"
#define TAG_FINANCE_CASH_COINS_TOTAL    "::Finance:{0}CashExtern[{1}].CashDetails.CoinsAmount"
#define TAG_FINANCE_CASH_COINS_SUM      "::Finance:{0}CashExtern[{1}].CashDetails.CoinsSum"
#define TAG_FINANCE_CASH_COINS_1      "::Finance:{0}CashExtern[{1}].CashDetails.CoinsAmount_1"
#define TAG_FINANCE_CASH_COINS_2      "::Finance:{0}CashExtern[{1}].CashDetails.CoinsAmount_2"
#define TAG_FINANCE_CASH_COINS_5      "::Finance:{0}CashExtern[{1}].CashDetails.CoinsAmount_5"
#define TAG_FINANCE_CASH_COINS_10      "::Finance:{0}CashExtern[{1}].CashDetails.CoinsAmount_10"
#define TAG_FINANCE_CASH_BILL_PROGRESS  "::Finance:{0}CashExtern[{1}].BillProgress"
#define TAG_FINANCE_CASH_COIN_PROGRESS  "::Finance:{0}CashExtern[{1}].CoinProgress"
#define TAG_FINANCE_CASH_COLLECT_DATE   "::Finance:{0}CashExtern[{1}].EncashDate"

void carwash::update_finance(int _unit_id)
{
    m_impl->queue_task([_unit_id, this]()
    {
        m_impl->update_finance(_unit_id);
    });
}

void carwash::impl::update_finance(int _unit_id)
{
    try
    {
        auto write_info = [&](const std::string _type, unsigned _unit_id, unsigned _idx)
        {
            nl::json cash;
            storage::instance().get_cash_status(_unit_id, cash);
            if (cash.empty())
                return;

            //logger::instance().log_debug(fmt::format("Updating cash status for unit_id={0}", _unit_id));
            //qDebug()<<"GET update_finance STATUS!!!!"<<_unit_id<<QString::fromStdString(cash.dump());

            m_client.write(fmt::format(TAG_FINANCE_CASH_SUM, _type, _idx),         (uint32_t) cash.at("sum_cash"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILLS_SUM, _type, _idx),   (uint32_t) cash.at("sum_bills"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_COINS_SUM, _type, _idx),   (uint32_t) cash.at("sum_coins"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_COINS_TOTAL, _type, _idx), (uint32_t) cash.at("coins_count"), NS);
            //qDebug()<<"update_finance 1";
            m_client.write(fmt::format(TAG_FINANCE_CASH_COINS_1, _type, _idx),    (uint32_t) cash.at("coins_0"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_COINS_2, _type, _idx),    (uint32_t) cash.at("coins_1"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_COINS_5, _type, _idx),    (uint32_t) cash.at("coins_2"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_COINS_10, _type, _idx),    (uint32_t) cash.at("coins_10"), NS);
            //qDebug()<<"update_finance 2";
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILLS_TOTAL, _type, _idx), (uint32_t) cash.at("bills_count"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILLS_10, _type, _idx),    (uint32_t) cash.at("bills_10"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILLS_50, _type, _idx),    (uint32_t) cash.at("bills_50"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILLS_100, _type, _idx),   (uint32_t) cash.at("bills_100"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILLS_200, _type, _idx),   (uint32_t) cash.at("bills_200"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILLS_500, _type, _idx),   (uint32_t) cash.at("bills_500"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILLS_1000, _type, _idx),  (uint32_t) cash.at("bills_1000"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILLS_2000, _type, _idx),  (uint32_t) cash.at("bills_2000"), NS);
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILLS_5000, _type, _idx),  (uint32_t) cash.at("bills_5000"), NS);
            //qDebug()<<"update_finance 3";
            uint32_t bp = cash.at("bills_count");
            m_client.write(fmt::format(TAG_FINANCE_CASH_BILL_PROGRESS, _type, _idx), uint8_t (bp / 10), NS);

            uint32_t cp = cash.at("coins_count");
            m_client.write(fmt::format(TAG_FINANCE_CASH_COIN_PROGRESS, _type, _idx), uint8_t (cp / 5), NS);
            //qDebug()<<"update_finance 4";
            std::string res;//servertype
            time_t last_collect = 0;
            alles::storage::instance().get_last_collect_info(_unit_id, res, last_collect);
            m_client.write(fmt::format(TAG_FINANCE_CASH_COLLECT_DATE, _type, _idx), last_collect, NS);
            //qDebug()<<"update_finance 5"<<_unit_id;
        };

        if (_unit_id >= 0 && _unit_id < 8)
        {
            write_info("Postbox", _unit_id, _unit_id);
        }
        else if (_unit_id >= 30 && _unit_id < 32)
        {
            write_info("Paycenter", _unit_id, _unit_id - 30);
        }
        else if (_unit_id >= 40 && _unit_id < 43)
        {
            write_info("Vacuum", _unit_id, _unit_id - 40);
        }
        else
        {
            // update all posts

            for (unsigned unit_id = 0; unit_id < 8; ++unit_id)
                write_info("Postbox", unit_id, unit_id);

            for (unsigned unit_id = 30; unit_id < 32; ++unit_id)
                write_info("Paycenter", unit_id, unit_id - 30);

            for (unsigned unit_id = 40; unit_id < 43; ++unit_id)
                write_info("Vacuum", unit_id, unit_id - 40);
        }
        qDebug()<<"update_finance 6";
    }
    catch(std::exception& _ex)
    {
        logger::instance().log_error(fmt::format("Exception while updating cash: {0}", _ex.what()));
        m_client.disconnect();
    }
}

// ------------------------------------------------------------------------------------------

void carwash::impl::on_kkt_enabled(opcua::variant _val)
{
    try
    {
        bool v = _val;
        kkt::instance().set_enabled(v);
    }
    catch(...)
    {
    }
}


// ------------------------------------------------------------------------------------------

void carwash::impl::on_kkt_printer_enabled(opcua::variant _val)
{
    try
    {
        bool v = _val;
        kkt::instance().set_printer_present(v);
    }
    catch(...)
    {
    }
}

// ------------------------------------------------------------------------------------------

void carwash::impl::on_kkt_address(opcua::variant _val)
{
    try
    {
        std::string v = _val;
        kkt::instance().set_address(v);
    }
    catch(...)
    {
    }
}

// ------------------------------------------------------------------------------------------

void carwash::impl::on_kkt_place(opcua::variant _val)
{
    try
    {
        std::string v = _val;
        kkt::instance().set_place(v);
    }
    catch(...)
    {
    }
}

void carwash::impl::on_kkt_tax_selected(opcua::variant _val)
{
    try
    {
        uint8_t mode = _val;
        kkt::instance().set_tax_mode(mode);
    }
    catch(...)
    {
    }
}

void carwash::impl::on_kkt_vat_selected(opcua::variant _val)
{
    try
    {
        uint8_t vat = _val;
        kkt::instance().set_vat(vat);
    }
    catch(...)
    {
    }
}
void carwash::impl::on_kkt_item_text(opcua::variant _val)
{
    try
    {
        std::string v = _val;
        kkt::instance().set_item_text(v);
    }
    catch(...)
    {
    }
}

// ------------------------------------------------------------------------------------------

void carwash::set_kkt_status(const std::string& _err, bool _online)
{
    m_impl->queue_task([_err, _online, this]()
    {
        try
        {
            m_impl->m_client.write(TAG_KKT_STATUS, _err, NS);
            m_impl->m_client.write(TAG_KKT_IS_ONLINE, _online, NS);
        }
        catch(...)
        {
        }
    });
}

// ------------------------------------------------------------------------------------------

void carwash::set_kkt_info(const std::string& _sn, const std::string& _fn,
                  const std::string& _rn, uint8_t _tax_mode)
{
    m_impl->queue_task([=]()
    {
        try
        {
            m_impl->m_client.write(TAG_KKT_SN, _sn, NS);
            m_impl->m_client.write(TAG_KKT_RN, _rn, NS);
            m_impl->m_client.write(TAG_KKT_FN, _fn, NS);
            m_impl->m_client.write(TAG_KKT_TAX_MODE, _tax_mode, NS);
        }
        catch(...)
        {
        }
    });
}

// ------------------------------------------------------------------------------------------

void carwash::set_bonus_status(const std::string& _err)
{
    m_impl->queue_task([_err, this]()
    {
        try
        {
            m_impl->m_client.write(TAG_BONUS_STATUS, _err, NS);
        }
        catch(...)
        {

        }
    });
}

// ------------------------------------------------------------------------------------------
void carwash::impl::on_refresh_cert()
{
    using namespace alles;
    std::string cert = settings::instance().get_tls_cert_path();
    std::string key  = settings::instance().get_tls_key_path();
    if (cert.empty() || key.empty())
    {
        logger::instance().log_error("TLS certificate/key file path not specified");
        m_client.write(TAG_BONUS_STATUS, "Нет сертификата или неверный пароль", NS);
        return;
    }
    else
    {    cm::tls::set_server_cert(cert, key, "alles-hw");
         cm::socket socket;
         cm::tls tls(socket);
         cm::http http(tls);
         bonus::instance().connect(socket,tls,http);
         //
    }

}

void carwash::impl::on_bonus_enable(opcua::variant _val)
{
    std::string serial;

    try {
        bonus::instance().set_enabled(_val);
        if ((bool)_val == true) {
            qDebug()<<"Bonus Enabled";
            qDebug()<<"m_serial = "<<QString::fromStdString(m_serial);
            if (m_serial.size() < 10) {
                serial = exec("cat /proc/cpuinfo | grep Serial | cut -d ' ' -f 2").substr(8, 8);
                serial = "RCW" + serial;
            } else {
                serial = m_serial;
            }
            qDebug()<<"SERIAL_NUMER = "<<QString::fromStdString(serial);
            bonus::instance().set_cw_serial(serial);
            on_refresh_cert();
        }
    } catch(...) {
        qDebug()<<"on_bonus_enable - Exeption";
    }
}

// ------------------------------------------------------------------------------------------

void carwash::impl::on_bonus_password(opcua::variant _val)
{
    try
    {
        bonus::instance().set_password(_val);
        qDebug()<<"set_password";
        on_refresh_cert();
    }
    catch(...)
    {
    }
}


// ------------------------------------------------------------------------------------------

void carwash::impl::on_date_changed(opcua::variant _val)
{
    try
    {
        std::string d = _val;
        time_svc::instance().set_date(d);
    }
    catch(...)
    {
    }
}

// ------------------------------------------------------------------------------------------

void carwash::impl::on_time_changed(opcua::variant _val)
{
    try
    {
        std::string t = _val;
        time_svc::instance().set_time(t);
    }
    catch(...)
    {
    }
}

// ------------------------------------------------------------------------------------------

void carwash::impl::read_and_send_progs()
{
    try
    {
        nl::json jprogs;
        opcua::variant progs = m_client.read(TAG_PROGS, NS);
        opcua::variant price = m_client.read(TAG_PRICE, NS);

        opcua::variant vprogs = m_client.read(TAG_VACUUM_PROGS, NS);
        opcua::variant vprice = m_client.read(TAG_VACUUM_PRICE, NS);

        auto enumerate = [this, &jprogs](int _max, int _start, const opcua::variant& _progs,
                const opcua::variant& _price, const std::string* _names)
        {
            if (_progs.is_array() && _price.is_array() && _progs.array_size() == _price.array_size())
            {
                bool* pr = _progs;
                uint16_t* arr = _price;
                //qDebug()<<"array_size"<<_price.array_size()<<_max;
                for (unsigned i = 1; i < _price.array_size(); ++i)
                {
                    if (i > _max) break;
                    if (pr[i] == false)
                        continue;

                    nl::json j =
                    {
                        { "id", _start + i },
                        { "name", _names[i-1] },
                        { "price", arr[i] }
                    };

                    jprogs.push_back(j);
                }
            }
        };
        int pc = m_client.array_size(TAG_PROGS,NS);
        //qDebug()<<"pc = "<<pc;
        enumerate(pc, 0, progs, price, m_program_name);
        enumerate(3, 100, vprogs, vprice, m_vprogram_name);

        bonus::instance().set_programs(jprogs);
    }
    catch(std::exception& _ex)
    {
        logger::instance().log_error(fmt::format("Exception during on_progs_changed: {0}", _ex.what()));
    }

}

}


