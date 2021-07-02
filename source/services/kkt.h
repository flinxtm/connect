
#pragma once

#include <future>
#include <tuple>
#include <vector>
#include <map>

namespace alles {

class kkt
{
    kkt();
    struct impl; impl& m_impl;

    public:

        static kkt& instance();

        void process();
        void set_enabled(bool _flag);
        void set_printer_present(bool _flag);
        void set_address(const std::string& );
        void set_place(const std::string& );
        void set_vat(uint8_t);
        void set_tax_mode(uint8_t);
        void set_item_text(const std::string&);

        std::string get_item_text() const;

        using info_t = std::map<std::string, std::string>;
        info_t get_info() const;
        uint8_t get_tax_modes() const;
        bool is_ready() const;
        bool is_enabled() const;


        //using status_t

        using receipt_t = std::tuple<uint16_t, uint32_t, uint32_t, std::vector<uint8_t>>; // id, fd, fp
        std::future<receipt_t> receipt_create(unsigned _id, unsigned _sum,
                                              bool _cash, const std::string& _unit);

        std::future<bool> receipt_print(uint32_t _fd);

        using session_status_t = std::tuple<bool, uint16_t, uint16_t>; // is_open, id, num
        using session_open_t = std::tuple<uint16_t, uint32_t, uint32_t>; // id, fd, fp
        using session_close_t = std::tuple<uint32_t, uint32_t>; // fd, fp

        //std::future<session_t>       session_read();
        //std::future<session_open_t>  session_open();
        //std::future<session_close_t> session_close();

        using register_t = std::tuple<uint32_t, uint32_t>; // success, error_code
        std::future<register_t> register_kkt(const std::string& _org, const std::string& _ofd_inn,
                                             const std::string& _ofd_name, const std::string& _inn,
                                             const std::string& _rn, uint8_t _tax_mode,
                                             const std::string& _address, const std::string& _place,
                                             const std::string& _cashier, const std::string& _email);

        // callbacks

        using idle_t = std::function<void()>;
        using error_t = std::function<void(std::string)>;

        void on_error(error_t _fn);
        void on_idle(idle_t _fn);
};

}



