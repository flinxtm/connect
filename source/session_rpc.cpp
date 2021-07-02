
#include "session_rpc.h"

#include <atomic>

#include <logger.h>
#include <libcm/protocol/http.hpp>
#include <libcm/protocol/tls.hpp>
#include <libcm/data.hpp>
#include <libcm/socket.hpp>
#include <libcm/timer.hpp>
#include <fmt/format.h>
#include <QString>
#include <QDebug>

namespace alles {

struct session_rpc::impl
{
    volatile std::atomic<size_t> m_refs;
    void inc_ref() { ++m_refs; }
    void dec_ref() { --m_refs; if (m_refs == 0) { delete this; } }

    cm::socket               m_socket;
    cm::http                 m_http;
    cm::tls                  m_tls;
    cm::timer                m_timeout;
    session_rpc::req_func_t  m_on_request;
    bool m_client = false;

    // constructor

    impl(cm::socket _socket, bool _client);

    void recv();

    std::string get_required_fields(const nl::json& _request, std::string& _method);

    void send_error(int _code, std::string _desc);
    void send_result(const nl::json& _res);
};

// ---------------------------------------------------------------------------------------------------------------------------------

session_rpc::session_rpc() : m_impl(nullptr) { }

session_rpc::session_rpc(cm::socket _socket, bool _client) : m_impl(new impl(_socket, _client))
{
}

session_rpc::impl::impl(cm::socket _socket, bool _client) : m_socket(_socket), m_tls(m_socket), m_http(m_tls)
{
    m_refs = 1;
    m_socket.set_timeout(30);
    m_socket.set_nonblocking(true);
    m_client = _client;
    if (!m_client)
        m_tls.listen();
}

// ---------------------------------------------------------------------------------------------------------------------------------
// copy semantics with reference counting

session_rpc::session_rpc(const session_rpc& _right) : m_impl(nullptr)
{
    *this = _right;
}

session_rpc::~session_rpc()
{
    if (m_impl)
        m_impl->dec_ref();
}

const session_rpc& session_rpc::operator = (const session_rpc& _right)
{
    if (this == &_right) return *this;

    if (m_impl)
        m_impl->dec_ref();

    m_impl = _right.m_impl;

    if (m_impl)
        m_impl->inc_ref();


    return *this;
}

bool session_rpc::is_valid() const
{
    return m_impl != nullptr;
}

// ---------------------------------------------------------------------------------------------------------------------------------
// extract required filelds

std::string session_rpc::impl::get_required_fields(const nl::json& _request, std::string& _method)
{
    std::string val;
    try
    {
        val = "method";
        _method = _request.at(val);
    }
    catch(std::exception& _ex)
    {
        return fmt::format("'{0}': {1}", val, _ex.what());
    }

    return std::string();
}

// ---------------------------------------------------------------------------------------------------------------------------------

void session_rpc::recv()
{
    m_impl->recv();
}

void session_rpc::tls_connect(const std::string &_host)
{
    if (m_impl->m_client && !m_impl->m_tls.is_handshake_complete())
    {
        m_impl->m_tls.connect(_host, "");
    }
}

bool session_rpc::is_tls_ok() const
{
    return m_impl->m_tls.is_handshake_complete();
}

void session_rpc::disconnect()
{
    m_impl->m_socket.close();
    m_impl->m_tls.reset();
}


void session_rpc::on_request(req_func_t _fn)
{
    m_impl->m_on_request = _fn;
}

void session_rpc::impl::recv()
{
    std::string http_method, path, method;
    cm::data body;

    while(true)
    {
        try
        {
            //qDebug()<<"RECV_START";
            std::tie(http_method, path, body) = m_http.recv_request();
            //qDebug()<<"RECV12345678"<<QString().fromStdString(http_method)
             //      <<QString().fromStdString(path)<<QString().fromStdString(body.c_str());
            if (http_method == "GET")
            {
                m_http.send_success("GET method is not supported");
                continue;
            }
             //qDebug()<<"RECV_START1";
            if (body.remaining_size() < 3)
                throw std::runtime_error("wrong request");

            std::string raw(reinterpret_cast<char*>(body.ptr_at_pos()), body.remaining_size());
            nl::json request = nl::json::parse(raw);
            //qDebug()<<"RECV_START2";
            if (request.empty())
                throw std::runtime_error("empty request");

            std::string rf = get_required_fields(request, method);
            //qDebug()<<"RECV_START3";
            if (!rf.empty())
                throw std::runtime_error(fmt::format("error checking required fields: {0}", rf));

            if (method.empty())
                throw std::runtime_error("method is empty");

            nl::json res = m_on_request(method, request);
            if (!res.empty())
            {
                send_result(res);
            }
            else
                throw std::runtime_error("handler not found");
            //qDebug()<<"RECV_START4";
        }
        catch(cm::socket::would_block&)
        {
            //qDebug()<<"catch(cm::socket::would_block&)";
            throw;
        }
        catch(cm::tls::alert& _err)
        {
            //qDebug()<<"catch(cm::tls::alert& _err)";
            m_tls.close();
            m_socket.close();
            return;
        }
        catch(std::exception& _ex)
        {
            qDebug()<<"catch(std::exception& _ex)";
            std::string error;
            if (method.empty())
                error = fmt::format("Error: {0}", _ex.what());
            else
                error = fmt::format("Error during handling '{0}' request: {1}", method, _ex.what());

            logger::instance().log_debug(error);

            send_error(-1, error);
            m_socket.close();
            return;
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------------------------

void session_rpc::send()
{
    if (m_impl->m_socket.is_connected())
        m_impl->m_socket.send();
}

// ---------------------------------------------------------------------------------------------------------------------------------

void session_rpc::impl::send_error(int _code, std::string _desc)
{
    send_result({
      { "result", _code },
      { "error", _desc }
    });
}

void session_rpc::send_error(int _code, const std::string &_desc)
{
    m_impl->send_error(_code, _desc);
}

// ---------------------------------------------------------------------------------------------------------------------------------

void session_rpc::send_result(const nl::json& _result)
{
    m_impl->send_result(_result);
}

void session_rpc::impl::send_result(const nl::json& _json)
{
    if (m_socket.is_connected())
        m_http.send_success(_json.dump());
}

bool session_rpc::is_connected() const
{
    return m_impl->m_socket.is_connected();
}

}

