
#include "rpc.h"
#include "storage.h"
#include "carwash.h"
#include "kkt.h"
#include "bonus.h"
#include <QDebug>
#include <fmt/format.h>
#include <logger.h>
#include <time.hpp>

namespace alles {

struct rpc::impl
{
    typedef nlohmann::json (impl::*handler_t)(nl::json& _request);
    std::map<std::string, handler_t> m_handlers;

    void register_rpc_handlers();
    nl::json handle_request(const std::string& _method, nl::json& _request);

    nl::json store_money_rpc(nl::json& _request);
    nl::json collect_rpc(nl::json& _request);
    nl::json cash_read_rpc(nl::json& _request);

    nl::json ping_rpc(nl::json& _request);
    nl::json get_history_rpc(nl::json& _request);

    nl::json kkt_register(nl::json& _request);
    nl::json receipt_register(nl::json& _request);
    nl::json receipt_read(nl::json& _request);
    nl::json receipt_print(nl::json& _request);

    nl::json bonus_login(nl::json& _request);
    nl::json bonus_qr_get(nl::json& _request);
    nl::json bonus_qr_check(nl::json& _request);
    nl::json bonus_charge(nl::json& _request);
    nl::json bonus_money_append(nl::json& _request);
    nl::json bonus_wash_complete(nl::json& _request);

    void wait_for_time(); // waits for time service to obtain current time


    nl::json m_last_status;
    nl::json m_last_diag;
};

// ------------------------------------------------------------------------------------------

rpc& rpc::instance()
{
    static rpc v;
    return v;
}

// ------------------------------------------------------------------------------------------

rpc::rpc() : m_impl(new impl)
{
    m_impl->register_rpc_handlers();
}

// ---------------------------------------------------------------------------------------------------------------------------------
//
// rpc interface
//
// ---------------------------------------------------------------------------------------------------------------------------------

void rpc::impl::register_rpc_handlers()
{
    m_handlers["storage::store_money"]   = &impl::store_money_rpc;
    m_handlers["storage::collect"]       = &impl::collect_rpc;
    m_handlers["storage::cash.read"]     = &impl::cash_read_rpc;

    m_handlers["connect::ping"]          = &impl::ping_rpc;

    m_handlers["kkt::register"]         = &impl::kkt_register;
    m_handlers["kkt::receipt.create"]   = &impl::receipt_register;
    m_handlers["kkt::receipt.read"]     = &impl::receipt_read;
    m_handlers["kkt::receipt.print"]     = &impl::receipt_print;

    m_handlers["bonus::login"]          = &impl::bonus_login;
    m_handlers["bonus::qr.get"]         = &impl::bonus_qr_get;
    m_handlers["bonus::qr.check"]       = &impl::bonus_qr_check;
    m_handlers["bonus::charge"]         = &impl::bonus_charge;
    m_handlers["bonus::money.append"]   = &impl::bonus_money_append;
    m_handlers["bonus::wash.complete"]  = &impl::bonus_wash_complete;
}

// ---------------------------------------------------------------------------------------------------------------------------------

void rpc::impl::wait_for_time()
{
    unsigned count = 0;
    while (!time_svc::instance().is_valid())
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        count += 1;

        if (count == 22)
            throw std::runtime_error("invalid time");
    }
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::handle_request(const std::string& _method, nl::json& _request)
{
    return m_impl->handle_request(_method, _request);
}

nl::json rpc::impl::handle_request(const std::string& _method, nl::json& _request)
{
    //qDebug()<<"handle_request = "<<_method.c_str();
    auto it = m_handlers.find(_method);
    if (it != m_handlers.end())
    {
        handler_t func = it->second;
        return (this->*func)(_request);
    }

    return nl::json();
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::ping_rpc(nl::json& _request)
{
    return { { "result", 0 } };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::store_money_rpc(nl::json& _request)
{
    wait_for_time();
    qDebug()<<"REQUEST!!!!!!"<<QString::fromStdString(_request.dump());
    std::string val;
    std::string type;
    nl::json detail;
    money_type mt;

    int unit_id = -1;
    unsigned paycenter_id = 0;
    unsigned sum = 0;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "type";
        type = params.at(val);
        if (type == "cash")
            mt = money_cash;
        else if (type == "card")
            mt = money_card;
        else if (type == "service")
            mt = money_service;
        else if (type == "bonus")
            mt = money_bonus;
        else
            throw std::runtime_error("unknown money type");

        val = "unit_id";
        unit_id = params.at(val);

        val = "sum";
        sum = params.at(val);

        val = "paycenter_id";
        if (params.count(val))
            paycenter_id = params.at(val);

        val = "detail";
        if (params.count(val))
            detail = params.at(val);
        /*if(detail.count("sum_bills")&&detail.count("sum_coins"))
            sum = (unsigned)detail.at("sum_bills") + (unsigned)detail.at("sum_coins");
        else
            if(detail.count("sum_bills"))
                sum = (unsigned)detail.at("sum_bills");
            else
                if(detail.count("sum_coins"))
                    sum = (unsigned)detail.at("sum_coins");*/
        qDebug()<<"SUMM!!!!!!!"<<sum;
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    if (unit_id < 0 /*|| sum == 0*/) //DEBUG
       throw std::runtime_error("invalid parameters");

     storage::instance().store_money(mt, unit_id, paycenter_id, sum, detail);

    if (paycenter_id > 0)
        carwash::instance().update_finance(paycenter_id);
    else
        carwash::instance().update_finance(unit_id);

    return
    {
        { "result", 0 }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::collect_rpc(nl::json& _request)
{
    wait_for_time();

    std::string val, user;
    int unit_id = -1;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "user";
        user = params.at(val);

        val = "unit_id";
        unit_id = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    storage::instance().store_collect_info(unit_id, user);
    carwash::instance().update_finance(unit_id);

    return
    {
        { "result", 0 }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::cash_read_rpc(nl::json& _request)
{
    std::string val;
    int unit_id = -1;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "unit_id";
        unit_id = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    nl::json result;
    storage::instance().get_cash_status(unit_id, result);
     qDebug()<<"RESUALT!!!!"<<QString().fromStdString(result.dump());
    return
    {
        { "result", 0 },
        { "cash", result }

    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::receipt_register(nl::json& _request)
{
    std::string val;
    int unit_id = -1;
    unsigned sum = 0;
    bool cash = true;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "unit_id";
        unit_id = params.at(val);

        val = "sum";
        sum = params.at(val);

        val = "cash";
        cash = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    if (unit_id < 0 || sum == 0)
       throw std::runtime_error("invalid parameters");

    std::string unit;

    if (unit_id >= 0 && unit_id < 8)
    {
        unit = fmt::format("Пост {}", unit_id + 1);
    }
    else if (unit_id >= 40 && unit_id < 43)
    {
        unit = fmt::format("Пылесос {}", unit_id - 39);
    }
    else if (unit_id >= 30 && unit_id < 31)
    {
        unit = fmt::format("Центр оплаты {}", unit_id - 29);
    }

    // queue new receipt

    auto id = storage::instance().register_receipt(unit_id, sum, cash);
    kkt::instance().receipt_create(id, sum, cash, unit);

    // don't wait for receipt creation return receipt ID only

    return
    {
        { "result", 0 },
        { "id", id }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::receipt_read(nl::json& _request)
{
    std::string val;
    unsigned id = 0;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "id";
        id = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    if (id == 0)
       throw std::runtime_error("invalid parameters");

    auto result = storage::instance().get_receipt(id);
    if (!result.empty())
    {
        nl::json org = kkt::instance().get_info();
        result["org"] = org;
        result["item"] = kkt::instance().get_item_text();
    }

    return
    {
        { "result", 0 },
        { "receipt", result }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::receipt_print(nl::json& _request)
{
    std::string val;
    unsigned fd = 0;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "fd";
        fd = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    if (fd == 0)
       throw std::runtime_error("invalid parameters");

    kkt::instance().receipt_print(fd);

    return
    {
        { "result", 0 }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::kkt_register(nl::json& _request)
{
    std::string val, org, inn, ofd_name, ofd_inn, rn, address, place, cashier, email;
    unsigned tax_mode = 0;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "org";
        org = params.at(val);

        val = "inn";
        inn = params.at(val);

        val = "rn";
        rn = params.at(val);

        val = "ofd_name";
        ofd_name = params.at(val);

        val = "ofd_inn";
        ofd_inn = params.at(val);

        val = "tax_mode";
        tax_mode = params.at(val);

        val = "address";
        address = params.at(val);

        val = "place";
        place = params.at(val);

        val = "cashier";
        cashier = params.at(val);

        val = "email";
        email = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }


    // TODO: check params

    auto tm = static_cast<uint8_t>(tax_mode);
    kkt::register_t res = kkt::instance().register_kkt(org, ofd_inn, ofd_name, inn, rn, tm,
                                                       address, place, cashier, email).get();

    return
    {
        { "result", 0 },
        { "kkt_result", std::get<0>(res) }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::bonus_login(nl::json& _request)
{
    std::string val, phone, pin;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "phone";
        phone = params.at(val);

        val = "pin";
        pin = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    if (phone.empty() || pin.empty())
        throw std::runtime_error("wrong parameters");

    nl::json res;
    try
    {
         res = bonus::instance().login(phone, pin);
    }
    catch(bonus::api_error& _ex)
    {
        return
        {
            { "result", _ex.code },
            { "error", _ex.desc }
        };
    }
    catch(std::exception& _ex)
    {
        return
        {
            { "result", -1 },
            { "error", "Connection error" }
        };
    }

    return
    {
        { "result", 0 },
        { "profile", res }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::bonus_qr_get(nl::json&)
{
    nl::json res;
    try
    {
         res = bonus::instance().get_qr();
    }
    catch(bonus::api_error& _ex)
    {
        return
        {
            { "result", _ex.code },
            { "error", _ex.desc }
        };
    }
    catch(std::exception& _ex)
    {
        return
        {
            { "result", -1 },
            { "error", "Connection error" }
        };
    }

    auto val = "qr_hash";
    if (res.count(val))
    {
        return
        {
            { "result", 0 },
            { "qr", res.at(val) }
        };
    }

    return
    {
        { "result", -1 },
        { "error", "response error" }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::bonus_qr_check(nl::json& _request)
{
    std::string val, qr;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "qr";
        qr = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    if (qr.empty())
        throw std::runtime_error("wrong parameter");

    nl::json res;
    try
    {
         res = bonus::instance().check_qr(qr);
    }
    catch(std::exception& _ex)
    {
        return
        {
            { "result", 0 },
            { "profile", nullptr }
        };
    }

    return
    {
        { "result", 0 },
        { "profile", res }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::bonus_charge(nl::json& _request)
{
    wait_for_time();
    std::string val, phone, order;
    unsigned sum = 0;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "phone";
        phone = params.at(val);

        val = "sum";
        sum = params.at(val);

        val = "order";
        order = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    if (phone.empty() || sum == 0)
        throw std::runtime_error("wrong parameter");

    //try
    //{
         bonus::instance().charge(phone, sum, order);
    //     throw std::runtime_error("DEBUG ERROR");
   // }

    /*catch(bonus::api_error& _ex)
    {
        return
        {
            { "result", _ex.code },
            { "error", _ex.desc }
        };
    }
    catch(std::exception& _ex)
    {
        return
        {
            { "result", -1 },
            { "error", "Connection error" }
        };

    }
    qDebug()<<"good return";
    */
    return
    {
        { "result", 0 }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::bonus_money_append(nl::json& _request)
{
    wait_for_time();

    std::string val, phone, order;
    unsigned sum = 0;
    bool cash = true;

    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "phone";
        phone = params.at(val);

        val = "order";
        order = params.at(val);

        val = "sum";
        sum = params.at(val);

        val = "cash";
        cash = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    if (phone.empty() || order.empty() || sum == 0)
        throw std::runtime_error("wrong parameter");

    // execute request and return immediately, it will be queued in case of error

    bonus::instance().append_money(sum, cash, phone, order);

    return
    {
        { "result", 0 }
    };
}

// ---------------------------------------------------------------------------------------------------------------------------------

nl::json rpc::impl::bonus_wash_complete(nl::json& _request)
{
    wait_for_time();

    std::string val, order;
    nl::json progs;
    qDebug()<<"WASH_COMPLETE"<<QString().fromStdString(_request.dump());
    try
    {
        val = "params";
        nl::json& params = _request.at(val);
        if (!params.is_object())
            throw std::runtime_error("must be an object");

        val = "order";
        order = params.at(val);

        val = "programs";
        progs = params.at(val);
    }
    catch(std::exception& _ex)
    {
        throw std::runtime_error(fmt::format("parameter '{0}': {1}", val,  _ex.what()));
    }

    if (order.empty() || !progs.is_array() || progs.empty())
        throw std::runtime_error("wrong parameter");

    // execute request and return immediately, it will be queued in case of error
    qDebug()<<"RPC!!!"<<_request.dump().c_str();
    bonus::instance().wash_complete(order, progs);

    return
    {
        { "result", 0 }
    };
}
}

