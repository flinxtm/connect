#include <list>
#include <chrono>
#include <time.h>
#include <fmt/time.h>
#include <fstream>

#include "bonus.h"
#include <services/storage.h>
#include <logger.h>

#include <fmt/format.h>
#include <libcm/timer.hpp>
#include <time.hpp>
#include <QDebug>

namespace alles {

bonus& bonus::instance()
{
    static bonus v;
    return v;
}

struct bonus::impl
{
    const std::string m_host        = "admin.alles-bonus.com";
    const std::string m_queue_file  = "/alles/data/bonus_queue.json";

    nl::json    m_send_queue = nl::json::array();
    cm::timer   m_queue_timer;
    cm::timer   m_finance_timer;

    bool        m_try_send_queue = false;
    bool        m_enabled = false;
    bool        m_finance_1st_time = false;
    bool        m_finance_midnight = false;
    bool        m_finance_need_more = false;
    bool        m_programs_sent = false;

    std::string m_cw_serial;
    std::string m_password;
    nl::json    m_programs;

    std::recursive_mutex m_mutex;
    std::recursive_mutex m_queue_mutex;

    bonus::error_t            m_error_fn;
    bonus::idle_t             m_idle_fn;

    impl();

    void queue_load();
    void queue_save();

    void connect(cm::socket& _sock, cm::tls& _tls, cm::http &_http);
    void process();
    void disconnect();
    void read_programs();
    void send_programs(std::string _progs);

    nl::json check_response(const std::string& _req, int http_status, cm::data _body);

    nl::json login(const std::string& _phone, const std::string& _pin);
    nl::json get_qr();
    nl::json check_qr(const std::string& _qr);
    nl::json get_profile(const std::string& _ssn);

    void charge(const std::string& _phone, unsigned _sum, const std::string& _order);
    void append_money(const nl::json& _params);
    void wash_complete(const nl::json& _params);
    void check_finance();
};

// ------------------------------------------------------------------------------------------

bonus::bonus() : m_impl(*new impl)
{

}

bonus::impl::impl()
{
    m_queue_timer.reset();

    queue_load();
}

// ------------------------------------------------------------------------------------------

void bonus::on_error(error_t _fn)
{
   m_impl.m_error_fn = _fn;
}

void bonus::on_idle(idle_t _fn)
{
    m_impl.m_idle_fn = _fn;
}

void bonus::set_enabled(bool _flag)
{
    std::lock_guard<std::recursive_mutex> lock(m_impl.m_mutex);
    m_impl.m_enabled = _flag;
}

void bonus::set_password(const std::string& _pass)
{
    std::lock_guard<std::recursive_mutex> lock(m_impl.m_mutex);
    m_impl.m_password = _pass;
}

void bonus::set_programs(const nl::json& _progs)
{
    std::lock_guard<std::recursive_mutex> lock(m_impl.m_mutex);

    if (_progs.empty() || !_progs.is_array())
        return;

    m_impl.m_programs = _progs;
    m_impl.m_programs_sent = false;
}

void bonus::set_cw_serial(const std::string& _sn)
{
    std::lock_guard<std::recursive_mutex> lock(m_impl.m_mutex);

    m_impl.m_cw_serial = _sn;
}

void bonus::connect(cm::socket &_sock, cm::tls &_tls, cm::http &_http)
{
    m_impl.connect(_sock,_tls,_http);
}

// ------------------------------------------------------------------------------------------

void bonus::impl::connect(cm::socket& _sock, cm::tls& _tls, cm::http& _http)
{
    std::string host, pass;
    {
        std::lock_guard<std::recursive_mutex> lock(m_mutex);
        host = m_host;
        pass = m_password;//

    }
//qDebug()<<"HOST"<<"PASSWORD"<<m_host<<m_password; //dirfin
    try
    {
        _tls.set_client_cert("/alles/config/bonus.crt", "/alles/config/bonus.pkcs8", pass);
    }
    catch(std::exception& _ex)
    {
        m_error_fn("Нет сертификата или неверный пароль");
        throw std::runtime_error("BONUS: no certificate or invalid password");
    }

    auto ip = cm::resolve(host);

    _sock.connect(ip, 443, 5);
    _sock.set_timeout(30);

    _tls.connect(host, "");
    _http.set_header("Host", host);
    //qDebug()<<"BONUS CONNECT"<<host.c_str()<<pass.c_str();
    m_idle_fn();
}

// ------------------------------------------------------------------------------------------

void bonus::process()
{
    m_impl.process();
}

void bonus::impl::process()
{
    std::string pass;
    bool enabled = false;
    nl::json progs;

    {
        std::lock_guard<std::recursive_mutex> lock(m_mutex);
        pass = m_password;
        enabled = m_enabled;
        progs = m_programs;
    }

    if (!pass.empty())
    {
        std::lock_guard<std::recursive_mutex> qlock(m_queue_mutex);

        try
        {
            auto timeout = m_queue_timer.elapsed().count();
            if (!m_send_queue.empty() && timeout >= 10000)
            {
                if (!m_try_send_queue)
                    logger::instance().log_status("BONUS: sending queued items...");

                m_try_send_queue = true;

                const nl::json& j = m_send_queue.begin().value();
                qDebug() << "BONUS: process(): m_send_queue = " << j.dump().c_str();
                if (j.count("method") && j.at("method") == "append_money") {
                    append_money(j.at("params"));
                } else
                if (j.count("method") && j.at("method") == "wash_complete") {
                    wash_complete(j.at("params"));
                }

                qDebug() << "BONUS: process(): m_send_queue = " << j.dump().c_str();

                m_send_queue.erase(0);
                queue_save();

                m_try_send_queue = false;
            }
        }
        catch(api_error& _err)
        {
            logger::instance().log_error(fmt::format("BONUS: api error in wash_complete(): {}, code: {}", _err.desc, _err.code));
            if (_err.code >= 400 && _err.code < 500) // drop request because server says it's invalid
            {
                m_send_queue.erase(0);
                queue_save();
            }
        }
        catch(std::exception& _ex)
        {
            if (!m_try_send_queue)
            {
                auto s = fmt::format("BONUS: error while sending queued items: {}", _ex.what());
                logger::instance().log_error(s);
            }

            m_queue_timer.reset();
        }
    }

    // send programs

    {
        if (enabled && !m_programs_sent)
            send_programs(progs.dump());
    }

    // check and send daily finance data

    auto& ts = time_svc::instance();
    //qDebug()<<"ENABLED!!!"<<enabled<<ts.is_valid()<<ts.hour()<<ts.minute();
    if (enabled && ts.is_valid())
    {
        auto hour = ts.hour();
        auto minute = ts.minute();
        if (!m_finance_midnight && hour == 0 && minute == 0)
        {
            m_finance_midnight = true;
            //qDebug()<<"check_finance!!!!!!";
            check_finance();
        }
        else if (hour != 0 || minute != 0)
        {
            m_finance_midnight = false;
        }
    }

    if (m_enabled && ts.is_valid() && (m_finance_need_more || m_finance_1st_time ||
                                       m_finance_timer.elapsed().count() >= 3600 * 1000))
    {
        m_finance_1st_time = false;
        m_finance_need_more = false;
        m_finance_timer.reset();

        check_finance();
    }
}

// ------------------------------------------------------------------------------------------

void bonus::impl::queue_save()
{
    std::fstream file;
    file.open(m_queue_file, std::ios::out | std::ios::trunc);
    if (file)
    {
        try
        {
           file << m_send_queue.dump(4);
        }
        catch (std::exception& _ex)
        {
            logger::instance().log_error(fmt::format("Exception while writing {0}: {1}", m_queue_file, _ex.what()));
        }
    }
    else
        logger::instance().log_error(fmt::format("Can't open file {0} for output", m_queue_file));

    file.flush();
    file.sync();//sync
    file.close();
}

// ------------------------------------------------------------------------------------------

void bonus::impl::queue_load()
{
    m_send_queue.clear();
    std::ifstream file(m_queue_file);

    try
    {
        file >> m_send_queue;
        if (!m_send_queue.is_array())
        {
            nl::json arr = nl::json::array();
            for (auto it = m_send_queue.begin(); it != m_send_queue.end(); ++it)
            {
                arr.push_back(it.value());
            }
            m_send_queue = arr;
        }
    }
    catch (std::exception& _ex)
    {
        //logger::instance().log_error(fmt::format("Exception while reading {}: {}", _file, _ex.what()));
    }
}

// ------------------------------------------------------------------------------------------
// check HTTP status, parse response, check 'status' block, throw exception on error

nl::json bonus::impl::check_response(const std::string& _url, int _http_status, cm::data _body)
{
    qDebug()<<"check_response"<<_url.c_str();
    if (_http_status == 200) // HTTP 200 OK
    {
        std::string raw(reinterpret_cast<char*>(_body.ptr_at_pos()), _body.remaining_size());
        nl::json response = nl::json::parse(raw);

        if (response.empty())
            throw std::runtime_error("empty response");

        auto val = "status";
        if (response.count(val) && response.at(val).is_object())
        {
            nl::json& status = response.at(val);
            val = "code";
            int code = -1;
            if (status.count(val) && status.at(val).is_number_integer())
            {
                code = status.at(val);
                if (code == 200)
                    return response;

                auto val = "message";
                std::string message;
                if (response.count(val) && response.at(val).is_string())
                    message = response.at(val);

                throw api_error(code, message);
            }
        }
        else
            logger::instance().log_error(fmt::format("http request '{}' wrong response "
                                                     "format: 'status' not found", _url));
    }
    else
        logger::instance().log_error(fmt::format("http request '{}' "
                                                 "returned error {}", _url, _http_status));

    throw std::runtime_error("response format error");
}

// ------------------------------------------------------------------------------------------

nl::json bonus::login(const std::string& _phone, const std::string& _pin)
{
    return m_impl.login(_phone, _pin);
}

nl::json bonus::impl::login(const std::string& _phone, const std::string& _pin)
{
    std::string url = "/api/v1/terminal/auth";
    nl::json j =
    {
        { "phone", _phone },
        { "password", _pin }
    };
    //qDebug()<<"phone"<<_phone.c_str()<<"password"<<_pin.c_str();
    int status = 0;
    cm::data body;
    cm::socket socket;
    cm::tls tls(socket);
    cm::http http(tls);

    connect(socket, tls, http);
    http.set_header("Content-Type", "application/json; charset=utf-8");
    std::tie(status, body) = http.post(url, j.dump());

    nl::json response = check_response(url, status, body);

    auto val = "session_id";
    if (response.count(val) && response.at(val).is_string())
    {
        return get_profile(response.at(val));
    }
    else
        logger::instance().log_error(fmt::format("http request '{}' wrong response "
                                                 "format: '{}' not found", url, val));

    throw std::runtime_error("login(): response error");
}

// ------------------------------------------------------------------------------------------

nl::json bonus::impl::get_profile(const std::string& _ssn)
{
    auto url = fmt::format("/api/v1/terminal/auth/profile?session_id={}", _ssn);

    int status = 0;
    cm::data body;
    cm::socket socket;
    cm::tls tls(socket);
    cm::http http(tls);

    connect(socket, tls, http);
    std::tie(status, body) = http.get(url);

    nl::json response = check_response(url, status, body);

    auto val = "data";
    if (response.count(val) && response.at(val).is_object())
    {
        qDebug()<<"response"<<response.at(val).dump().c_str();
        return response.at(val);
    }
    else
        logger::instance().log_error(fmt::format("http request '{}' wrong response "
                                                 "format: '{}' not found", url, val));

    throw std::runtime_error("get_profile(): response error");
}

// ------------------------------------------------------------------------------------------

nl::json bonus::get_qr()
{
    return m_impl.get_qr();
}

nl::json bonus::impl::get_qr()
{
    std::string url = "/api/v1/terminal/auth/qr";

    //logger::instance().log_debug(url);

    int status = 0;
    cm::data body;
    cm::socket socket;
    cm::tls tls(socket);
    cm::http http(tls);

    connect(socket, tls, http);
    std::tie(status, body) = http.get(url);

    nl::json response = check_response(url, status, body);

    auto val = "data";
    if (response.count(val) && response.at(val).is_object())
    {
        qDebug()<<"BONUS RESPONCE"<<QString().fromStdString(response.at(val).dump());
        return response.at(val);
    }
    else
        logger::instance().log_error(fmt::format("http request '{}' wrong response "
                                                 "format: '{}' not found", url, val));

    throw std::runtime_error("response error");
}

// ------------------------------------------------------------------------------------------

nl::json bonus::check_qr(const std::string& _qr)
{
    return m_impl.check_qr(_qr);
}

nl::json bonus::impl::check_qr(const std::string& _qr)
{
    std::string url = "/api/v1/terminal/auth/qr";

    //logger::instance().log_debug(url);

    nl::json j =
    {
        { "qr_hash", _qr }
    };

    int status = 0;
    cm::data body;
    cm::socket socket;
    cm::tls tls(socket);
    cm::http http(tls);

    connect(socket, tls, http);
    http.set_header("Content-Type", "application/json; charset=utf-8");
    std::tie(status, body) = http.post(url, j.dump());

    nl::json response = check_response(url, status, body);

    auto val = "session_id";
    if (response.count(val) && response.at(val).is_string())
    {
        return get_profile(response.at(val));
    }

    throw std::runtime_error("response error");
}

// ------------------------------------------------------------------------------------------

void bonus::charge(const std::string& _phone, unsigned _sum, const std::string& _order)
{
    m_impl.charge(_phone, _sum, _order);
}

void bonus::impl::charge(const std::string& _phone, unsigned _sum, const std::string& _order)
{
    auto url = "/api/v1/terminal/wash/writeoff";
    //logger::instance().log_debug(url);

    auto when = fmt::format("{} {}", time_svc::instance().date(), time_svc::instance().time());

    nl::json j =
    {
        { "date_time", when },
        { "phone", _phone },
        { "written_of_balls", _sum },
        { "ordernum", m_cw_serial + _order },
    };
    qDebug()<<"void bonus::impl::charge()"<<QString().fromStdString(j.dump());
    int status = 0;
    cm::data body;
    cm::socket socket;
    cm::tls tls(socket);
    cm::http http(tls);


    connect(socket, tls, http);
    http.set_header("Content-Type", "application/json; charset=utf-8");
    try {
    std::tie(status, body) = http.post(url, j.dump());
    }
    catch(std::exception& _ex)
    {
        qDebug()<<"My Exception";
    }
    //check_response(url, status, body);
}

// ------------------------------------------------------------------------------------------

void bonus::append_money(unsigned _sum, bool _cash, const std::string& _phone, const std::string& _order)
{
    std::thread([=]()
    {
        bool sent = false, reset_timer = true;
        nl::json request = nl::json::object();
        auto when = fmt::format("{} {}", time_svc::instance().date(), time_svc::instance().time());

        std::string serial;
        bool queue_empty = true;

        {
            std::lock_guard<std::recursive_mutex> lock(m_impl.m_mutex);
            serial = m_impl.m_cw_serial;
            queue_empty = m_impl.m_send_queue.empty();
        }

        try
        {
            nl::json params =
            {
              { "phone", _phone },
              { "type", _cash ? "cash" : "card" },
              { "amount", _sum },
              { "ordernum", serial + _order },
              { "date_time", when }
            };
            qDebug()<<"void bonus::impl::append_money()"<<QString().fromStdString(params.dump());
            request =
            {
                { "method", "append_money" },
                { "params", params }
            };

            if (queue_empty)
            {
                m_impl.append_money(params);
                sent = true;
            }
            else
                reset_timer = false;
        }
        catch(api_error& _err)
        {
            logger::instance().log_error(fmt::format("BONUS: api error in wash_complete(): {}, code: {}", _err.desc, _err.code));
            if (_err.code >= 400 && _err.code < 500) // drop request because server says it's invalid
                sent = true;
        }
        catch(std::exception& _ex)
        {
            logger::instance().log_error(fmt::format("BONUS: error in append_money(): {}", _ex.what()));
        }

        if (!sent && !request.empty()) // queue for sending
        {
            std::lock_guard<std::recursive_mutex> qlock(m_impl.m_queue_mutex);
            logger::instance().log_debug("bonus::append_money() queued for sending");
            if (reset_timer)
                m_impl.m_queue_timer.reset();

            m_impl.m_send_queue.push_back(request);
            m_impl.queue_save();
        }

    }).detach();
}

void bonus::impl::append_money(const nl::json& _params)
{
    auto url ="/api/v1/terminal/money";

    //logger::instance().log_debug(url);

    int status = 0;
    cm::data body;
    cm::socket socket;
    cm::tls tls(socket);
    cm::http http(tls);

    connect(socket, tls, http);
    http.set_header("Content-Type", "application/json; charset=utf-8");
    std::tie(status, body) = http.post(url, _params.dump());
    qDebug()<<"params = "<<_params.dump().c_str();
    check_response(url, status, body);
}

// ------------------------------------------------------------------------------------------

void bonus::wash_complete(const std::string& _order, const nl::json& _progs)
{
    std::thread([=]()
    {
        bool sent = false, reset_timer = true;
        nl::json request = nl::json::object();
        auto when = fmt::format("{} {}", time_svc::instance().date(), time_svc::instance().time());

        std::string serial;
        bool queue_empty = true;
        qDebug()<<"WASH_COMPLETE!!!!!";
        {
            std::lock_guard<std::recursive_mutex> lock(m_impl.m_mutex);
            serial = m_impl.m_cw_serial;
            queue_empty = m_impl.m_send_queue.empty();
        }

        qDebug()<<"Serial="<<QString().fromStdString(serial);
        qDebug()<<"Order="<<QString().fromStdString(_order);

        try
        {
            nl::json params =
            {
              { "ordernum", serial + _order },
              { "date_time", when },
              { "programs", _progs }
            };
            qDebug()<<"void bonus::impl::wash_complete()"<<QString().fromStdString(params.dump());
            request =
            {
                { "method", "wash_complete" },
                { "params", params }
            };

            if (queue_empty)
            {
                m_impl.wash_complete(params);
                sent = true;
            }
            else
                reset_timer = false;
            qDebug()<<"ORDER NUM wash_complete"<<QString().fromStdString(serial + _order);
        }
        catch(api_error& _err)
        {
            logger::instance().log_error(fmt::format("BONUS: api error in wash_complete(): {}, code: {}", _err.desc, _err.code));
            if (_err.code >= 400 && _err.code < 500) // drop request because server says it's invalid
                sent = true;
        }
        catch(std::exception& _ex)
        {
            logger::instance().log_error(fmt::format("BONUS: error in wash_complete(): {}", _ex.what()));
        }

        if (!sent && !request.empty()) // queue for sending
        {
            std::lock_guard<std::recursive_mutex> qlock(m_impl.m_queue_mutex);
            logger::instance().log_debug("bonus::wash_complete() queued for sending");
            if (reset_timer)
                m_impl.m_queue_timer.reset();

            m_impl.m_send_queue.push_back(request);
            m_impl.queue_save();
        }

    }).detach();
}

void bonus::impl::wash_complete(const nl::json& _params)
{
    auto url ="/api/v1/terminal/wash";
    int status = 0;
    cm::data body;
    cm::socket socket;
    cm::tls tls(socket);
    cm::http http(tls);

    connect(socket, tls, http);
    http.set_header("Content-Type", "application/json; charset=utf-8");
    std::tie(status, body) = http.post(url, _params.dump());
    qDebug()<<"WASH COMPLETE123"<<QString().fromStdString(_params.dump())<<url<<status<<body.c_str();
    nl::json res = check_response(url, status, body);
    qDebug()<<"WASH COMPLETE RES"<<QString().fromStdString(res.dump());
}

// ------------------------------------------------------------------------------------------

void bonus::impl::send_programs(std::string _progs)
{
    cm::socket socket;
    cm::tls tls(socket);
    cm::http http(tls);

    try
    {
        auto url ="/api/v1/terminal/programs";

        //logger::instance().log_debug(url);

        int status = 0;
        cm::data body;

        connect(socket, tls, http);
        m_idle_fn();
        http.set_header("Content-Type", "application/json; charset=utf-8");
        std::tie(status, body) = http.post(url, _progs);
        /*qDebug()<<"url :"<<url;
        qDebug()<<"status :"<<status;
        qDebug()<<"body1"<<body.c_str();*/
        check_response(url, status, body);

        m_programs_sent = true;
    }
    catch(api_error& _ex)
    {
        logger::instance().log_error(fmt::format("BONUS: error while sending programs: {}, {}", _ex.desc, _ex.code));
        m_error_fn("Ошибка соединения с сервером");

        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    catch(std::exception& _ex)
    {
        if (tls.is_client_cert_ok())
        {
            logger::instance().log_error(fmt::format("BONUS: error while sending programs: {}", _ex.what()));
            m_error_fn("Ошибка соединения с сервером");
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}

// ------------------------------------------------------------------------------------------

void bonus::impl::check_finance()
{
    try
    {
        auto url ="/api/v1/terminal/import";

        int status = 0;
        cm::data body;
        cm::socket socket;
        cm::tls tls(socket);
        cm::http http(tls);

        connect(socket, tls, http);
        std::tie(status, body) = http.get(url);

        nl::json response = check_response(url, status, body);

        auto val = "date";
        if (response.count(val) && response.at(val).is_string())
        {
            auto& ts = time_svc::instance();
            std::string date = response.at(val);
            if (date.size() == 10)
            {
                auto year = std::atoi(date.substr(0, 4).c_str());
                auto month = std::atoi(date.substr(5, 2).c_str());
                auto day = std::atoi(date.substr(8, 2).c_str());

                std::string next_date = fmt::format("{0}-{1:02d}-{2:02d}", year, month, day);
                logger::instance().log_debug(fmt::format("BONUS: last known finance date: {}", next_date));

                if (year >= 2019)
                {
                    tm remote; memset(&remote, 0, sizeof(tm));
                    remote.tm_year = year - 1900;
                    remote.tm_mon = month - 1;
                    remote.tm_mday = day;
                    time_t remote_t = mktime(&remote);

                    if (remote_t >= ts.timestamp())
                        return;

                    auto get_max_day = [](unsigned _month, unsigned _year)
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
                    };

                    nl::json days = nl::json::array();

                    auto cur_day = ts.date();
                    while (next_date <= cur_day)
                    {
                        nl::json d = storage::instance().get_finance_daily(next_date);

                        if (!d.empty()) {
                            logger::instance().log_debug("BONUS: adding data for " + next_date);
                            days.push_back(d);
                        }

                        if (days.size() == 10) { // limit 10 days per request
                            m_finance_need_more = true;
                            break;
                        }

                        auto max_day = get_max_day(month, year);
                        if (month == 12 && day == max_day) {
                            year++; month = 1; day = 1; // rotate year
                        } else if (day == max_day)  {
                            month++; day = 1; // rotate month
                        } else {
                            day++;
                        }

                        next_date = fmt::format("{0}-{1:02d}-{2:02d}", year, month, day);
                    }

                    qDebug()<<"IN WHILE"<<QString().fromStdString(next_date);

                    if (days.empty()) {
                        return;
                    }

                    logger::instance().log_debug("BONUS: sending finance data");

                    http.set_header("Content-Type", "application/json; charset=utf-8");
                    std::tie(status, body) = http.post(url, days.dump());
                    logger::instance().log_debug(days.dump());
                    qDebug()<<"GET FINANCE!!!!!"<<url<<QString().fromStdString(days.dump());
                    check_response(url, status, body);
                }
            }
        }
    }
    catch(api_error& _ex)
    {
        logger::instance().log_error(fmt::format("BONUS: error while sending finance: {}, {}", _ex.desc, _ex.code));
    }
    catch(std::exception& _ex)
    {
        qDebug()<<"exeption";
        logger::instance().log_error(fmt::format("BONUS: error while sending finance: {}", _ex.what()));
    }
}
}
