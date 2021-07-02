#pragma once
#include <inttypes.h>

enum money_type
{
    money_cash,
    money_card,
    money_service,
    money_bonus
};

enum chemistry_type
{
    chemistry_shampoo,
    chemistry_basic,
    chemistry_wax,
    chemistry_polimer,
    chemistry_foam,
    chemistry_brush,
    chemistry_wheels,
    chemistry_insect,
    chemistry_color
};

struct range
{
    uint16_t start_year;
    uint8_t  start_month;
    uint8_t  start_day;
    uint16_t end_year;
    uint8_t  end_month;
    uint8_t  end_day;
};

