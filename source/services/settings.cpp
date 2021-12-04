
#include <string>
#include <fstream>
#include <iostream>
#include <utils.h>

#include "settings.h"

#include <logger.h>
#include <fmt/format.h>

namespace alles {

settings& settings::instance()
{
    static settings s;
    return s;
}

// ------------------------------------------------------------------------------------------

struct settings::impl
{
    nl::json m_settings;
    std::string m_file = "/alles/config/connect.json";
    void create_default_config();
    void set_ip();
    unsigned get_post_index() const;
    uint8_t get_subnet() const;

};
void settings::impl::create_default_config()
{
    std::fstream settings_file;
    settings_file.open(m_file, std::ios::out | std::ios::trunc );

    nl::json f1;

    f1 =
    {
        { "log_path", "/var/log" }
        //{ "program_count", 27 },
        //{ "chemistry_count", 10 }
    };

    settings_file << f1.dump(4);
    settings_file.flush();
    settings_file.close();
}
// ------------------------------------------------------------------------------------------

settings::settings() : m_impl(new impl)
{
}

void settings::load(bool _debug)
{


    std::fstream settings_file;
    settings_file.open(m_impl->m_file, std::ios::in);

    try
    {
        settings_file >> m_impl->m_settings;
    }
    catch (std::exception& ex)
    {
        std::cout << "Exception during parsing settings: " << ex.what() << std::endl;
        settings_file.close();
        m_impl->create_default_config();
        return;
    }
    if (m_impl->m_settings.empty())
    {
        std::cout << "Settings file is empty or has incorrect format" << std::endl;
        m_impl->create_default_config();
        //exec("reboot");
        return;
    }
    try
    {
        auto val = "log_path";
        if (m_impl->m_settings.count(val))
        {
           std::string lp = m_impl->m_settings.at(val);
           if (!_debug)
                logger::instance().set_path(lp);
        }

        val = "init";
        if (m_impl->m_settings.count(val))
        {
           bool init = m_impl->m_settings.at(val);
           if (init)
           {
                m_impl->m_settings.erase(val);
                save();

                std::cout << exec("/alles/commands/extend_partition.sh") << std::endl;
                std::cout << exec("/alles/commands/setup_postgres.sh") << std::endl;
                std::cout << exec("reboot") << std::endl;
           }
        }
    }
    catch(std::exception& _ex)
    {
        logger::instance().log_error(std::string("Error setting log path: ") + _ex.what());
    }

    exec("mkdir -p /alles/data");

    std::fstream if_rules_file;
    if_rules_file.open("/etc/udev/rules.d/70-persistent-net.rules", std::ios::out | std::ios::trunc);
    if_rules_file.flush();
    if_rules_file.close();

    m_impl->set_ip();
}

// ------------------------------------------------------------------------------------------

void settings::impl::set_ip()
{
    unsigned subnet = get_subnet();

    // write static address config
    {
        auto sconf =
                "auto lo\r\n"
                "iface lo inet loopback\r\n"
                "\r\n"
                "auto eth0\r\n"
                "allow-hotplug eth0\r\n"
                "iface eth0 inet static\r\n"
                "  address 192.168.{0}.3\r\n"
                "  netmask 255.255.255.0\r\n"
                "  gateway 192.168.{0}.1\r\n"
                "  network 192.168.{0}.0\r\n";

        std::fstream interfaces_file;
        interfaces_file.open("/etc/network/interfaces", std::ios::out | std::ios::trunc );
        interfaces_file << fmt::format(sconf, subnet);
        interfaces_file.flush();
        interfaces_file.close();
    }

    logger::instance().log_debug("Setting IP...");

    // apply new IP
    {
        std::cout << exec(fmt::format("ifconfig eth0 192.168.{0}.3", subnet).c_str()) << std::endl;
        std::cout << exec(fmt::format("route add default gw 192.168.{0}.1", subnet).c_str())  << std::endl;
    }

    // set resolve.conf DNS
    {
        auto sconf =
                "nameserver 8.8.8.8\n"
                "nameserver 192.168.{0}.1";
        auto str = fmt::format(sconf, subnet);
        exec(fmt::format("echo '{}' > /etc/resolv.conf", str));
    }
}

// ------------------------------------------------------------------------------------------

void settings::save()
{
    std::fstream settings_file;
    settings_file.open(m_impl->m_file, std::ios::out | std::ios::trunc);

    settings_file << m_impl->m_settings.dump(4);
    settings_file.flush();
    settings_file.sync();
    settings_file.close();
}

// ------------------------------------------------------------------------------------------

nl::json& settings::get(const std::string& _id)
{
    if (m_impl->m_settings.count(_id))
        return m_impl->m_settings[_id];

    throw std::runtime_error(std::string("Setting '" + _id + "' not found"));
}

// ------------------------------------------------------------------------------------------

std::string settings::get_opc_server() const
{
    if (m_impl->m_settings.count("opc_server"))
        return m_impl->m_settings["opc_server"];

    return "opc.tcp://192.168.1.2:4840";
}

// ------------------------------------------------------------------------------------------

unsigned settings::get_subnet() const
{
    return m_impl->get_subnet();
}

uint8_t settings::impl::get_subnet() const
{
    try
    {
        if (m_settings.count("subnet"))
            return m_settings["subnet"];
    }
    catch(std::exception& _ex)
    {
        // TODO: log exception
    }

    return 1;
}

// ---------------------------------------------------------------------------------------------------------------------------------

std::string settings::get_tls_key_path()
{
    try
    {
        if (m_impl->m_settings.count("tls_key"))
             return m_impl->m_settings.at("tls_key");
    }
    catch(std::exception & ex)
    {
        logger::instance().log_error(fmt::format("Exception in settings::get_tls_key_path(): {0}", ex.what()));
    }

    return "/alles/config/key.pkcs8";
}

// ---------------------------------------------------------------------------------------------------------------------------------

std::string settings::get_tls_cert_path()
{
    try
    {
        if (m_impl->m_settings.count("tls_cert"))
             return m_impl->m_settings.at("tls_cert");
    }
    catch(std::exception & ex)
    {
        logger::instance().log_error(std::string("Exception in settings::get_tls_cert_path(): {0}", ex.what()));
    }

    return "/alles/config/cert.pem";
}

} // namespace alles


