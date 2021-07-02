
#include "kkt.h"

#include <list>
#include <functional>
#include <mutex>

#include <logger.h>
#include <time.hpp>

#include <libcm/socket.hpp>
#include <libcm/timer.hpp>
#include <libcm/ip.hpp>
#include <libcm/protocol/kitcash.hpp>
#include <services/settings.h>
#include <services/storage.h>
#include <QDebug>
#include <fmt/format.h>

namespace alles {

kkt& kkt::instance()
{
    static kkt v;
    return v;
}

struct kkt::impl
{
    logger&                 m_logger = logger::instance();

    cm::socket              m_socket;
    cm::kitcash             m_kitcash;
    cm::timer               m_check_timeout;
    cm::ipv4_t              m_ip;
    uint16_t                m_port = 7777;
    bool                    m_enabled = false;
    bool                    m_printer_present = false;
    bool                    m_try_connect = false;
    bool                    m_is_ready = false;
    bool                    m_params_printed = false;
    bool                    m_is_automatic = true;
    unsigned                m_session = 0;

    bool                    m_init_errors[5];

    cm::kitcash::reg_t      m_kkt_reg;
    cm::kitcash::status_t   m_kkt_status;
    cm::kitcash::reg_info_t m_kkt_reg_info;

    std::string             m_kkt_sn;
    std::string             m_kkt_version;
    std::string             m_kkt_fn;
    std::string             m_kkt_org_name;

    std::string             m_address;
    std::string             m_place;
    std::string             m_item;
    uint8_t                 m_vat = 5;
    uint8_t                 m_tax_mode = 0;

    kkt::error_t            m_error_fn;
    kkt::idle_t             m_idle_fn;

    std::list<std::function<void()>> m_tasks;
    std::recursive_mutex m_task_mutex;

    impl() : m_kitcash(m_socket)
    {
    }

    void queue_task(std::function<void()>&&);

    void process();

    void connect();
    void init();
    void reset();
    void check(bool _throw = true);
    void cancel();
    void check_session();
    bool update_time();

    kkt::register_t register_kkt(const std::string& _org, const std::string& _ofd_inn,
                                 const std::string& _ofd_name, const std::string& _inn,
                                 const std::string& _rn, uint8_t _tax_mode,
                                 const std::string& _address, const std::string& _place,
                                 const std::string &_cashier, const std::string &_email);

    kkt::session_status_t session_status();
    kkt::session_open_t session_open();
    kkt::session_close_t session_close();
    std::vector<uint8_t> session_open_time(unsigned _fd);

    kkt::receipt_t receipt_create(unsigned _id, unsigned _sum, bool _cash,
                                  const std::string& _unit);
    bool receipt_print(uint32_t _fd);
};

// ------------------------------------------------------------------------------------------

kkt::kkt() : m_impl(*new impl)
{
    auto subnet = settings::instance().get_subnet();
    m_impl.m_ip = cm::ipv4_t(fmt::format("192.168.{0}.4", subnet));
}


void kkt::set_enabled(bool _flag)
{
    m_impl.m_enabled = _flag;
}

void kkt::set_printer_present(bool _flag)
{
    m_impl.m_printer_present = _flag;
}

void kkt::set_address(const std::string& _adr)
{
    m_impl.m_address = _adr;
}

void kkt::set_place(const std::string& _place)
{
    m_impl.m_place = _place;
}

void  kkt::set_item_text(const std::string& _item)
{
    m_impl.m_item = _item;
}

std::string kkt::get_item_text() const
{
    return m_impl.m_item;
}

void  kkt::set_vat(uint8_t _vat)
{
    m_impl.m_vat = _vat;
}

void  kkt::set_tax_mode(uint8_t _tax_mode)
{
    m_impl.m_tax_mode = _tax_mode;
}

void kkt::on_error(error_t _fn)
{
   m_impl.m_error_fn = _fn;
}

void kkt::on_idle(idle_t _fn)
{
    m_impl.m_idle_fn = _fn;
}

bool kkt::is_ready() const
{
    return m_impl.m_is_ready;
}

bool kkt::is_enabled() const
{
    return m_impl.m_enabled;
}


kkt::info_t kkt::get_info() const
{
    info_t res = m_impl.m_kkt_reg_info;

    res["fn"] = m_impl.m_kkt_fn;
    res["sn"] = m_impl.m_kkt_sn;
    res["rn"] = m_impl.m_kkt_reg.rn;
    res["address"] = m_impl.m_address;
    res["place"] = m_impl.m_place;

    return res;
}

uint8_t kkt::get_tax_modes() const
{
    return m_impl.m_kkt_reg.tax_modes;
}

// ------------------------------------------------------------------------------------------

void kkt::process()
{
    m_impl.process();
}

void kkt::impl::process()
{
    {
        std::lock_guard<std::recursive_mutex> lock(m_task_mutex);

        for (auto it = m_tasks.begin(); it != m_tasks.end(); ++it)
        {
            const auto& fn = (*it);
            fn();
        }

        m_tasks.clear();
    }

    if (!m_enabled)
    {
        reset();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        return;
    }

    if (!m_is_ready) // get kkt params
    {
        init();

        if (m_kkt_sn.empty())
        {
            if (m_error_fn)
                m_error_fn("Ошибка инициализации ККТ");

            if (!m_init_errors[0])
            {
                m_init_errors[0] = true;
                m_logger.log_error("KKT: not installed");
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            return;
        }

        m_init_errors[0] = false;

        if (m_kkt_fn.empty())
        {
            if (m_error_fn)
                m_error_fn("Фискальный накопитель не установлен или неисправен");

            if (!m_init_errors[1])
            {
                m_init_errors[1] = true;
                m_logger.log_error("KKT: FN not installed");
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            return;
        }

        m_init_errors[1] = false;

        if (m_kkt_reg.rn.empty())
        {
            if (m_error_fn)
                m_error_fn("ККТ не зарегистрирована в ФНС");

            if (!m_init_errors[2])
            {
                m_init_errors[2] = true;
                m_logger.log_error("KKT: not registered");
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            return;
        }

        m_init_errors[2] = false;

        m_logger.log_status(fmt::format("KKT: {}", m_kkt_sn));
        m_logger.log_status(fmt::format("KKT: Version {}", m_kkt_version));
        m_logger.log_status(fmt::format("KKT: FN {}", m_kkt_fn));
        m_logger.log_status(fmt::format("KKT: RN {}", m_kkt_reg.rn));
        m_logger.log_status(fmt::format("KKT: tax mode(s): {:0>8b}", m_kkt_reg.tax_modes));

        m_is_ready = true;
        m_check_timeout.reset();


        auto& ts = time_svc::instance();
        if (ts.is_valid())
        {
            update_time();
            check_session();
        }

        m_idle_fn();
    }
    else
    {
        if (m_check_timeout.elapsed().count() > 5000)
        {
            m_check_timeout.reset();
            check(false);
        }
    }

    auto& ts = time_svc::instance();
    if (m_is_ready && ts.is_valid())
    {
        auto hour = ts.hour();
        auto minute = ts.minute();

        if (hour == 23 && minute == 57)
        {
            if (std::get<0>(session_status()))
                session_close();
        }
        else if (hour == 0 && minute == 2 )
        {
            check_session();
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

// ------------------------------------------------------------------------------------------

void kkt::impl::queue_task(std::function<void ()>&& _f)
{
    std::lock_guard<std::recursive_mutex> lock(m_task_mutex);
    m_tasks.push_back(_f);
}

// ------------------------------------------------------------------------------------------

void kkt::impl::connect()
{
    m_try_connect = true;
    m_socket.connect(m_ip, m_port, 10);
    m_socket.set_timeout(20);
    m_try_connect = false;
}

// ------------------------------------------------------------------------------------------

void kkt::impl::reset()
{
    m_is_ready = false;
    m_kkt_fn.clear();
    m_kkt_org_name.clear();
    m_kkt_sn.clear();
    m_kkt_version.clear();
    m_kkt_reg.rn.clear();
    m_kkt_reg_info.clear();
    m_kkt_status.fn_present = false;
}

// ------------------------------------------------------------------------------------------

void kkt::impl::init()
{
    reset();
    try
    {
        connect();
        m_kkt_sn = m_kitcash.get_serial();
        m_socket.close();

        connect();
        m_kkt_version = m_kitcash.get_config_version();
        m_socket.close();

        connect();
        m_kkt_fn = m_kitcash.get_fn_serial();
        m_socket.close();

        connect();
        m_kkt_reg = m_kitcash.get_registration_status();
        m_socket.close();

        for (unsigned i = 1; ;++i)
        {
            connect();
            auto info = m_kitcash.get_registration_info(i);
            if (!info.empty())
                m_kkt_reg_info = info;
            else break;
            m_socket.close();
        }
        m_socket.close();
    }
    catch(cm::kitcash::error& _err)
    {
        //auto s = fmt::format("{} code:{}", _err.what(), _err.code);
        //m_logger.log_error(s);
    }
    catch(std::exception& _ex)
    {

        if (!m_try_connect)
            m_logger.log_error(_ex.what());
    }

    m_socket.close();
}

// ------------------------------------------------------------------------------------------

kkt::register_t kkt::impl::register_kkt(const std::string& _org, const std::string& _ofd_inn,
                                        const std::string& _ofd_name, const std::string& _inn,
                                        const std::string& _rn, uint8_t _tax_mode,
                                        const std::string& _address, const std::string& _place,
                                        const std::string& _cashier, const std::string& _email)
{
    kkt::register_t res;
    try
    {
        connect();
        m_logger.log_status("KKT: begin registration");
        m_kitcash.begin_registration(1);
        m_socket.close();

        connect();
        m_logger.log_status("KKT: send registration data");
        m_kitcash.send_reg_data(_org, _address, _place, _cashier, _ofd_inn, _ofd_name, "1", _email, 0, 1);
        m_socket.close();

        connect();
        m_logger.log_status("KKT: finish registration");
        res = m_kitcash.finish_registration(_inn, _rn, _tax_mode);
        m_socket.close();

    }
    catch(cm::kitcash::error& _err)
    {
        auto s = fmt::format("KKT: error '{}' code: {:x}", _err.what(), _err.code);
        m_logger.log_error(s);
        m_socket.close();
        throw _err;
    }
    catch(std::exception& _ex)
    {
        m_logger.log_error(fmt::format("KKT: error in register_kkt(): {}", _ex.what()));
        m_socket.close();
        throw _ex;
    }

    return res;
}

// ------------------------------------------------------------------------------------------
// check session and renew if needed

void kkt::impl::check_session()
{
    auto& storage = storage::instance();

    try
    {
        unsigned num, id;
        bool open;
        std::tie(open, id, num) = session_status();

        if (open)
        {
            unsigned fd = storage.kkt_session_read();
            if (fd == -1) // session is unknown
            {
                session_close();
                open = false;
            }
            else // check session validity
            {
                auto& ts = time_svc::instance();
                auto dt = session_open_time(fd); // 5 bytes YMDHm
                if (!dt.empty())
                {
                    if (2000 + dt[0] == ts.year() && dt[1] == ts.month() && dt[2] == ts.mday())
                    {
                        unsigned now = ts.hour() * 3600 + ts.minute() * 60 + ts.second();
                        unsigned then = dt[3] * 3600 + dt[4] * 60;

                        if (now > then && now - then < 24 * 3600)
                        {
                            return;
                        }
                    }
                }

                m_logger.log_debug("session time expired: closing");
                session_close();
                open = false;
            }
        }

        if (!open) // update time and open new session
        {
            if (update_time())
            {
                uint16_t id; uint32_t fd = 0, fp = 0;
                std::tie(id, fd, fp) = session_open();
                storage.kkt_session_create(fd);
            }
        }

    }
    catch(std::exception& _ex)
    {
        m_logger.log_error(fmt::format("KKT: error in check_session(): {}", _ex.what()));
    }
}

// ------------------------------------------------------------------------------------------

void kkt::impl::check(bool _throw)
{
    try
    {
        connect();
        m_kkt_status = m_kitcash.get_status();
        m_socket.close();

        switch (m_kkt_status.printer_status)
        {
            case 0: m_idle_fn(); break;
            case 1: m_error_fn("Принтер не подключен"); break;
            case 2: m_error_fn("Бумага отсутствует в принтере"); break;
            case 3: m_error_fn("Замятие бумаги"); break;
            case 5: m_error_fn("Крышка принтера открыта"); break;
            case 6: m_error_fn("Ошибка отрезчика принтера"); break;
            case 7: m_error_fn("Аппаратная ошибка принтера"); break;
        }

        if (!m_kkt_status.fn_present && m_kkt_status.has_errors)
        {
            m_error_fn("Ошибка в ККТ");
            return;
        }

        return;
    }
    catch(cm::kitcash::error& _err)
    {
        auto s = fmt::format("KKT: error '{}' code: {:x}", _err.what(), _err.code);
        m_logger.log_error(s);
    }
    catch(std::exception& _ex)
    {
        m_logger.log_error(fmt::format("KKT: error in check(): {}", _ex.what()));
    }

    m_is_ready = false;
    m_socket.close();
    if (_throw)
        throw std::runtime_error("Статус не определен");
}

// ------------------------------------------------------------------------------------------

void kkt::impl::cancel()
{
    try
    {
        m_socket.close();
        m_logger.log_status("KKT: cancel current document");
        connect();
        m_kitcash.cancel();
        m_socket.close();
    }
    catch(cm::socket::io_timeout& _ex)
    {
        reset();
    }
    catch(cm::socket::socket_error& _ex)
    {
        reset();
    }

    m_socket.close();
    throw::std::runtime_error("kkt error");
}

// ------------------------------------------------------------------------------------------

kkt::session_status_t kkt::impl::session_status()
{
    check();
    try
    {
        kkt::session_status_t res;

        connect();
        res = m_kitcash.session_status();
        m_socket.close();

        m_session = std::get<1>(res);

        return res;
    }
    catch(cm::kitcash::error& _err)
    {
        auto s = fmt::format("KKT: {} code:{}", _err.what(), _err.code);
        m_logger.log_error(s);
    }
    catch(cm::socket::io_timeout& _ex)
    {
        reset();
    }
    catch(cm::socket::socket_error& _ex)
    {
        reset();
    }
    catch(std::exception& _ex)
    {
        m_logger.log_error(_ex.what());
    }

    return { false, -1, -1 };
}

// ------------------------------------------------------------------------------------------

std::vector<uint8_t> kkt::impl::session_open_time(unsigned _fd)
{
    check();
    std::vector<uint8_t> datetime;

    try
    {
        connect();
        datetime = m_kitcash.session_open_time(_fd);
        m_socket.close();
    }
    catch(cm::kitcash::error& _err)
    {
        auto s = fmt::format("KKT: {} code:{}", _err.what(), _err.code);
        m_logger.log_error(s);
    }
    catch(cm::socket::io_timeout& _ex)
    {
        reset();
    }
    catch(cm::socket::socket_error& _ex)
    {
        reset();
    }
    catch(std::exception& _ex)
    {
        m_logger.log_error(_ex.what());
    }

    return datetime;
}

// ------------------------------------------------------------------------------------------

bool kkt::impl::update_time()
{
    auto& ts = time_svc::instance();
    if (!ts.is_valid() || ts.year() < 2000)
        return false;

    check();
    try
    {
        connect();
        m_kitcash.update_time({ ts.year() - 2000, ts.month(), ts.mday(), ts.hour(), ts.minute() });
        m_socket.close();
        return true;
    }
    catch(cm::kitcash::error& _err)
    {
        auto s = fmt::format("KKT: {} code:{}", _err.what(), _err.code);
        m_logger.log_error(s);
    }
    catch(cm::socket::io_timeout& _ex)
    {
        reset();
    }
    catch(cm::socket::socket_error& _ex)
    {
        reset();
    }
    catch(std::exception& _ex)
    {
        m_logger.log_error(_ex.what());
    }

    return false;
}


// ------------------------------------------------------------------------------------------

kkt::session_open_t kkt::impl::session_open()
{
    check();
    try
    {
        qDebug()<<"KKT: session open";
        m_logger.log_status("KKT: session open");
        kkt::session_open_t res;

        connect();
        m_kitcash.session_begin_open(false);
        m_socket.close();

        connect();
        res = m_kitcash.session_open();
        m_socket.close();

        m_session = std::get<0>(res);

        return res;
    }
    catch(cm::kitcash::error& _err)
    {
        auto s = fmt::format("KKT: {} code: {:x}", _err.what(), _err.code);
        m_logger.log_error(s);
    }
    catch(cm::socket::io_timeout& _ex)
    {
        reset();
    }
    catch(cm::socket::socket_error& _ex)
    {
        reset();
    }
    catch(std::exception& _ex)
    {
        m_logger.log_error(_ex.what());
    }

    cancel();
}

// ------------------------------------------------------------------------------------------

kkt::session_close_t kkt::impl::session_close()
{
    check();
    try
    {
        m_logger.log_status("KKT: session close");
        kkt::session_close_t res;

        connect();
        m_kitcash.session_begin_close(false);
        m_socket.close();

        connect();
        res = m_kitcash.session_close();
        m_socket.close();

        return res;
    }
    catch(cm::kitcash::error& _err)
    {
        auto s = fmt::format("KKT: {} code: {:x}", _err.what(), _err.code);
        m_logger.log_error(s);
    }
    catch(cm::socket::io_timeout& _ex)
    {
        reset();
    }
    catch(cm::socket::socket_error& _ex)
    {
        reset();
    }
    catch(std::exception& _ex)
    {
        m_logger.log_error(_ex.what());
    }

    cancel();
}

// ------------------------------------------------------------------------------------------

kkt::receipt_t kkt::impl::receipt_create(unsigned _id, unsigned _sum, bool _cash,
                                         const std::string& _unit)
{
    check();
    check_session();

    if (!m_is_ready)
        throw std::runtime_error("KKT not ready");
    qDebug()<<"receipt_create!!!!!!";
    std::string step = "open_receipt";
    uint8_t mode = 0;
    cm::kitcash::vat_mode_e vat = cm::kitcash::vat_mode_e::not_applicable;

    try
    {
        kkt::receipt_t res;

        connect();
        qDebug()<<"open_receipt(1, _sum);";
        m_kitcash.open_receipt(false);
        m_socket.close();

        // count and select tax mode

        unsigned tm_total = 0;
        for (unsigned i = 0; i < 8; ++i)
        {
            uint8_t m = 1 << i;
            if (m_kkt_reg.tax_modes & m)
                tm_total++;
        }

        if (tm_total > 1) // several tax mode selected during registration
        {
            if (m_tax_mode > 5)
                throw std::runtime_error("KKT: wrong tax mode");

            mode = 1 << m_tax_mode;

            if (!(mode & m_kkt_reg.tax_modes))
                throw std::runtime_error("KKT: wrong tax mode");
        }
        else mode = m_kkt_reg.tax_modes;

        if (m_vat > 5)
            throw std::runtime_error("KKT: wrong vat");

        if (mode == 1)
            vat = static_cast<cm::kitcash::vat_mode_e>(m_vat+1);

        std::string cashier, email;

        connect();
        step = "add_item";
        m_kitcash.add_item(m_item, 1, _sum, vat , 4);
        m_socket.close();

        connect();
        step = "add_payment";
        if (_cash) m_kitcash.add_payment(mode, _sum, 0, 0, 0, 0, cashier, email);
        else m_kitcash.add_payment(mode, 0, _sum, 0, 0, 0, cashier, email);
        m_socket.close();

        if (m_is_automatic)
        {
            connect();
            step = "set_machine";
            m_kitcash.set_machine(m_address, m_place, _unit);
            m_socket.close();
        }

        connect();
        step = "close_receipt";
        qDebug()<<"close_receipt(1, _sum);";
        res = m_kitcash.close_receipt(1, _sum);
        m_socket.close();

        uint16_t num; uint32_t fd,  fp;
        std::vector<uint8_t> datetime;
        std::tie (num, fd, fp, datetime) = res;

        storage::instance().update_receipt(_id, num, fd, fp, datetime, m_session, (uint8_t) vat, (uint8_t) m_tax_mode);

        return res;
    }
    catch(cm::kitcash::error& _err)
    {
        auto s = fmt::format("KKT: {} code: {:x}\nstep: {}, tax_mode: {}, vat: {}, sum: {}, unit: {}",
                             _err.what(), _err.code, step, mode,  (uint8_t) vat, _sum, _unit);
        m_logger.log_error(s);
        m_logger.log_debug(fmt::format("KKT: address: {}, place: {}", m_address, m_place));
    }
    catch(cm::socket::io_timeout& _ex)
    {
        reset();
    }
    catch(cm::socket::socket_error& _ex)
    {
        reset();
    }
    catch(std::exception& _ex)
    {
        m_logger.log_error(_ex.what());
    }

    cancel();
}

// ------------------------------------------------------------------------------------------

bool kkt::impl::receipt_print(uint32_t _fd)
{
    try
    {
        connect();
        m_kitcash.print_document(_fd);
        m_socket.close();
        return true;
    }
    catch(...) { }

    m_socket.close();
    return false;
}

// ------------------------------------------------------------------------------------------

std::future<kkt::register_t> kkt::register_kkt(const std::string& _org, const std::string& _ofd_inn,
                                               const std::string& _ofd_name, const std::string& _inn,
                                               const std::string& _rn, uint8_t _tax_mode,
                                               const std::string& _address, const std::string& _place,
                                               const std::string& _cashier, const std::string& _email)
{
    using pr_t = std::promise<kkt::register_t>;
    std::shared_ptr<pr_t> pr_ptr(new pr_t);
    auto f = pr_ptr->get_future();
    m_impl.queue_task([=]() mutable
    {
        try
        {
            pr_ptr->set_value(m_impl.register_kkt(_org, _ofd_inn, _ofd_name, _inn,
                                                  _rn, _tax_mode, _address, _place,
                                                  _cashier, _email));
        }
        catch(...)
        {
            pr_ptr->set_exception(std::current_exception());
        }
    });

    return f;
}

// ------------------------------------------------------------------------------------------

std::future<kkt::receipt_t> kkt::receipt_create(unsigned _id, unsigned _sum, bool _cash,
                                                const std::string &_unit)
{
    using pr_t = std::promise<kkt::receipt_t>;
    std::shared_ptr<pr_t> pr_ptr(new pr_t);
    auto f = pr_ptr->get_future();
    m_impl.queue_task([=]() mutable
    {
        try
        {
            pr_ptr->set_value(m_impl.receipt_create(_id, _sum, _cash, _unit));
        }
        catch(...)
        {
            pr_ptr->set_exception(std::current_exception());
        }
    });

    return f;
}

// ------------------------------------------------------------------------------------------

std::future<bool> kkt::receipt_print(uint32_t _fd)
{
    using pr_t = std::promise<bool>;
    std::shared_ptr<pr_t> pr_ptr(new pr_t);
    auto f = pr_ptr->get_future();
    m_impl.queue_task([=]() mutable
    {
        try
        {
            pr_ptr->set_value(m_impl.receipt_print(_fd));
        }
        catch(...)
        {
            pr_ptr->set_exception(std::current_exception());
        }
    });

    return f;
}

}
