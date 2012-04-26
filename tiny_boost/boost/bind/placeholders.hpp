#ifndef BOOST_BIND_PLACEHOLDERS_HPP_INCLUDED
#define BOOST_BIND_PLACEHOLDERS_HPP_INCLUDED

// MS compatible compilers support #pragma once

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//
//  bind/placeholders.hpp - _N definitions
//
//  Copyright (c) 2002 Peter Dimov and Multi Media Ltd.
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//
//  See http://www.boost.org/libs/bind/bind.html for documentation.
//

#include <boost/bind/arg.hpp>
#include <boost/config.hpp>

namespace
{

#if defined(__BORLANDC__) || defined(__GNUC__) && (__GNUC__ < 4)

static inline abt_boost::arg<1> _1() { return abt_boost::arg<1>(); }
static inline abt_boost::arg<2> _2() { return abt_boost::arg<2>(); }
static inline abt_boost::arg<3> _3() { return abt_boost::arg<3>(); }
static inline abt_boost::arg<4> _4() { return abt_boost::arg<4>(); }
static inline abt_boost::arg<5> _5() { return abt_boost::arg<5>(); }
static inline abt_boost::arg<6> _6() { return abt_boost::arg<6>(); }
static inline abt_boost::arg<7> _7() { return abt_boost::arg<7>(); }
static inline abt_boost::arg<8> _8() { return abt_boost::arg<8>(); }
static inline abt_boost::arg<9> _9() { return abt_boost::arg<9>(); }

#elif defined(BOOST_MSVC) || (defined(__DECCXX_VER) && __DECCXX_VER <= 60590031) || defined(__MWERKS__) || \
    defined(__GNUC__) && (__GNUC__ == 4 && __GNUC_MINOR__ < 2)  

static abt_boost::arg<1> _1;
static abt_boost::arg<2> _2;
static abt_boost::arg<3> _3;
static abt_boost::arg<4> _4;
static abt_boost::arg<5> _5;
static abt_boost::arg<6> _6;
static abt_boost::arg<7> _7;
static abt_boost::arg<8> _8;
static abt_boost::arg<9> _9;

#else

abt_boost::arg<1> _1;
abt_boost::arg<2> _2;
abt_boost::arg<3> _3;
abt_boost::arg<4> _4;
abt_boost::arg<5> _5;
abt_boost::arg<6> _6;
abt_boost::arg<7> _7;
abt_boost::arg<8> _8;
abt_boost::arg<9> _9;

#endif

} // unnamed namespace

#endif // #ifndef BOOST_BIND_PLACEHOLDERS_HPP_INCLUDED
