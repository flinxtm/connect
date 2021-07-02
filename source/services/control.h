#pragma once

namespace alles {

class control
{
    control();
    struct impl; impl* m_impl;

    public:

        static control& instance();

        void process();
};

}



