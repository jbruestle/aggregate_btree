//  (c) Copyright Fernando Luis Cacciola Carballal 2000-2004
//  Use, modification, and distribution is subject to the Boost Software
//  License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

//  See library home page at http://www.boost.org/libs/numeric/conversion
//
// Contact the author at: fernando_cacciola@hotmail.com
// 
#ifndef BOOST_NUMERIC_CONVERSION_BOUNDS_12NOV2002_HPP
#define BOOST_NUMERIC_CONVERSION_BOUNDS_12NOV2002_HPP

#include "boost/numeric/conversion/detail/bounds.hpp"

namespace abt_boost{} namespace boost = abt_boost; namespace abt_boost{ namespace numeric 
{

template<class N>
struct bounds : boundsdetail::get_impl<N>::type
{} ;

} } // namespace abt_boost::numeric

#endif