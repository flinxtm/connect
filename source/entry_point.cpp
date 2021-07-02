
#include <iostream>
#include <thread>
#include <unistd.h>
#include <functional>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/prctl.h>
#include "string.h"

#include <logger.h>
#include <fmt/format.h>
#include <event_loop.h>
#include <session_rpc.h>
#include <libcm/protocol/tls.hpp>

#include "services/carwash.h"
#include "services/settings.h"
#include "services/rpc.h"
#include "services/control.h"
#include "services/kkt.h"
#include "services/bonus.h"

#include "version.h"
#include <QDebug>
#define MAX_CLIENTS 1000
alles::session_rpc g_clients[MAX_CLIENTS];

unsigned volatile num_children = 0;
int start(bool debug, int argc, char *argv[]);

// ------------------------------------------------------------------------------------------

int main(int argc, char *argv[])
{
    if (argc > 1)
    {
        std::string s(argv[1]);
        if (s == "debug")
            return start(true, argc, argv);
    }

    {
        pid_t pid;
        pid = fork();
        if (pid < 0) exit(EXIT_FAILURE);
        if (pid > 0) exit(EXIT_SUCCESS);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        alles::logger::instance().log_status("Switched to daemon mode...");
    }

    struct sigaction act;
    bzero(&act, sizeof(act));
    act.sa_handler = [](int)
    {
        int saved_errno = errno;
          while (waitpid((pid_t)(-1), 0, WNOHANG) > 0) {}
          errno = saved_errno;
        if (num_children > 0) num_children--;
    };

    act.sa_flags = SA_RESTART | SA_NOCLDSTOP;

    if ( sigaction(SIGCHLD, &act, 0) != 0 )
    {
        perror("sigaction()");
        return -1;
    }

    for (;;)
    {
        if ( num_children < 1 )
        {
            int pid = fork();
            if ( pid == 0 )
            {
                start(false, argc, argv);
            }
            else if ( pid > 0 )
            {
                num_children++;
            }
            else
            {
                perror("fork()");
                return -1;
            }
        }

        sleep(1);
    }
}

// ------------------------------------------------------------------------------------------

#include <botan/auto_rng.h>

int set_bonus_perm()
{
    using namespace alles;
    std::string cert = settings::instance().get_tls_cert_path();
    std::string key  = settings::instance().get_tls_key_path();

    if (cert.empty() || key.empty())
    {
        logger::instance().log_error("TLS certificate/key file path not specified");
        return -1;
    }
    else
        cm::tls::set_server_cert(cert, key, "alles-hw");
    return 0;
}

int start(bool _debug, int argc, char *argv[])
{
    using namespace alles;

    settings::instance().load(_debug);

    alles::logger::instance().log_status(fmt::format("Starting connect vesion {0}...", VERSION));
    logger::instance().log_status("Starting connect worker...");

    // setup TLS listener and session traffic counter

    set_bonus_perm();
    std::thread([]() // background task thread
    {
        prctl(PR_SET_NAME,"background\0",0,0,0);
        for (;;)
        {
            carwash::instance().perform_background_tasks();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            //qDebug()<<"std::thread background";
        }

    }).detach();


    std::thread([]()
    {
        prctl(PR_SET_NAME,"bonus\0",0,0,0);


        bonus::instance().on_error([](const std::string& _err)
        {
            carwash::instance().set_bonus_status(_err);
        });

        bonus::instance().on_idle([]()
        {
            carwash::instance().set_bonus_status("Ошибок нет");
        });
        //int count(0);
        for (;;)
        {
            bonus::instance().process();
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            /*if(count++>60)
            {
                qDebug()<<"bonus update";
                count=0;
                set_bonus_perm();
            }*/

        }

    }).detach();


    std::thread([]()
    {
        prctl(PR_SET_NAME,"kkt\0",0,0,0);

        kkt::instance().on_error([](const std::string& _err)
        {
            auto& kkt = kkt::instance();
            auto& carwash = carwash::instance();

            bool online = kkt.is_ready();
            carwash.set_kkt_status(_err, online);

            const auto info = kkt.get_info();
            auto tm = kkt.get_tax_modes();
            std::string sn, fn, rn;
            if (info.count("sn")) sn = info.at("sn");
            if (info.count("fn")) fn = info.at("fn");
            if (info.count("rn")) rn = info.at("rn");

            carwash.set_kkt_info(sn, fn, rn, tm);
        });

        kkt::instance().on_idle([]()
        {
            auto& kkt = kkt::instance();
            auto& carwash = carwash::instance();

            const auto info = kkt.get_info();
            auto tm = kkt.get_tax_modes();

            auto& sn = info.at("sn");
            auto& fn = info.at("fn");
            auto& rn = info.at("rn");

            carwash.set_kkt_info(sn, fn, rn, tm);
            carwash.set_kkt_status("Готов к работе", true);
        });


        for (;;)
        {
            kkt::instance().process();
        }

    }).detach();

    std::thread([]()
    {
        prctl(PR_SET_NAME,"server\0",0,0,0);

        auto& loop = alles::event_loop::instance();
        loop.add_server("0.0.0.0", 443, [](cm::socket& _sock, unsigned _worker)
        {

            if (_sock.fd() >= MAX_CLIENTS)
            {
                _sock.close();
                return;
            }

            g_clients[_sock.fd()] = alles::session_rpc(_sock, false);
            g_clients[_sock.fd()].on_request([](std::string& _method, nl::json& _request)
            {
                return alles::rpc::instance().handle_request(_method, _request);
            });

        });

        loop.on_event([](int _fd, alles::event_loop::event_type_e _ev, unsigned _worker)
        {
            if (_fd >= MAX_CLIENTS)
                return;

            if (g_clients[_fd].is_valid())
            {
                try
                {
                    switch (_ev)
                    {
                        case event_loop::event_type_e::read:
                            g_clients[_fd].recv();
                        break;

                        case event_loop::event_type_e::write:
                            g_clients[_fd].send();
                        break;

                        case event_loop::event_type_e::closed:
                        case event_loop::event_type_e::error:
                            g_clients[_fd] = session_rpc();
                        return;
                    }

                    if (!g_clients[_fd].is_connected())
                    {
                        g_clients[_fd] = session_rpc();

                    }
                }
                catch(cm::socket::would_block&)
                {
                    throw;
                }
                catch(cm::socket::socket_error& _ex)
                {
                    logger::instance().log_error(fmt::format("socket error: {0}", _ex.what()));
                    g_clients[_fd] = session_rpc();
                }
                catch(std::exception& _ex)
                {
                    logger::instance().log_error(fmt::format("error: {0}", _ex.what()));
                }
            }
        });

        loop.start(4); // start worker threads
        loop.loop(); // starts listen and accept on servers (blocks this thread)

    }).detach();

    for(;;)
    {
        alles::carwash::instance().process_events();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    return 0;
}

