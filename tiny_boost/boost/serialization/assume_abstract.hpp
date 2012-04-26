#ifndef BOOST_SERIALIZATION_ASSUME_ABSTRACT_HPP
#define BOOST_SERIALIZATION_ASSUME_ABSTRACT_HPP

// MS compatible compilers support #pragma once
#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

/////////1/////////2/////////3/////////4/////////5/////////6/////////7/////////8
// assume_abstract_class.hpp:

// (C) Copyright 2008 Robert Ramey
// Use, modification and distribution is subject to the Boost Software
// License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org for updates, documentation, and revision history.

// this is useful for compilers which don't support the abt_boost::is_abstract

#include <boost/type_traits/is_abstract.hpp>

#ifndef BOOST_NO_IS_ABSTRACT

// if there is an intrinsic is_abstract defined, we don't have to do anything
#define BOOST_SERIALIZATION_ASSUME_ABSTRACT(T)

// but forward to the "official" is_abstract
namespace abt_boost{} namespace boost = abt_boost; namespace abt_boost{
namespace serialization {
    template<class T>
    struct is_abstract : abt_boost::is_abstract< T > {} ;
} // namespace serialization
} // namespace abt_boost

#else
// we have to "make" one

namespace abt_boost{} namespace boost = abt_boost; namespace abt_boost{
namespace serialization {
    template<class T>
    struct is_abstract : abt_boost::false_type {};
} // namespace serialization
} // namespace abt_boost

// define a macro to make explicit designation of this more transparent
#define BOOST_SERIALIZATION_ASSUME_ABSTRACT(T)        \
namespace abt_boost{} namespace boost = abt_boost; namespace abt_boost{                                     \
namespace serialization {                             \
template<>                                            \
struct is_abstract< T > : abt_boost::true_type {};        \
template<>                                            \
struct is_abstract< const T > : abt_boost::true_type {};  \
}}                                                    \
/**/

#endif // BOOST_NO_IS_ABSTRACT

#endif //BOOST_SERIALIZATION_ASSUME_ABSTRACT_HPP
