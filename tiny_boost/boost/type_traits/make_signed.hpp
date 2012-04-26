
//  (C) Copyright John Maddock 2007.
//  Use, modification and distribution are subject to the Boost Software License,
//  Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt).
//
//  See http://www.boost.org/libs/type_traits for most recent version including documentation.

#ifndef BOOST_TT_MAKE_SIGNED_HPP_INCLUDED
#define BOOST_TT_MAKE_SIGNED_HPP_INCLUDED

#include <boost/mpl/if.hpp>
#include <boost/type_traits/is_integral.hpp>
#include <boost/type_traits/is_signed.hpp>
#include <boost/type_traits/is_unsigned.hpp>
#include <boost/type_traits/is_enum.hpp>
#include <boost/type_traits/is_same.hpp>
#include <boost/type_traits/remove_cv.hpp>
#include <boost/type_traits/is_const.hpp>
#include <boost/type_traits/is_volatile.hpp>
#include <boost/type_traits/add_const.hpp>
#include <boost/type_traits/add_volatile.hpp>
#include <boost/type_traits/detail/ice_or.hpp>
#include <boost/type_traits/detail/ice_and.hpp>
#include <boost/type_traits/detail/ice_not.hpp>
#include <boost/static_assert.hpp>

// should be the last #include
#include <boost/type_traits/detail/type_trait_def.hpp>

namespace abt_boost{} namespace boost = abt_boost; namespace abt_boost{

namespace detail {

template <class T>
struct make_signed_imp
{
   BOOST_STATIC_ASSERT(
      (::abt_boost::type_traits::ice_or< ::abt_boost::is_integral<T>::value, ::abt_boost::is_enum<T>::value>::value));
#if !BOOST_WORKAROUND(BOOST_MSVC, <=1300)
   BOOST_STATIC_ASSERT(
      (::abt_boost::type_traits::ice_not< ::abt_boost::is_same<
         typename remove_cv<T>::type, bool>::value>::value));
#endif

   typedef typename remove_cv<T>::type t_no_cv;
   typedef typename mpl::if_c<
      (::abt_boost::type_traits::ice_and< 
         ::abt_boost::is_signed<T>::value,
         ::abt_boost::is_integral<T>::value,
         ::abt_boost::type_traits::ice_not< ::abt_boost::is_same<t_no_cv, char>::value>::value,
         ::abt_boost::type_traits::ice_not< ::abt_boost::is_same<t_no_cv, wchar_t>::value>::value,
         ::abt_boost::type_traits::ice_not< ::abt_boost::is_same<t_no_cv, bool>::value>::value >::value),
      T,
      typename mpl::if_c<
         (::abt_boost::type_traits::ice_and< 
            ::abt_boost::is_integral<T>::value,
            ::abt_boost::type_traits::ice_not< ::abt_boost::is_same<t_no_cv, char>::value>::value,
            ::abt_boost::type_traits::ice_not< ::abt_boost::is_same<t_no_cv, wchar_t>::value>::value,
            ::abt_boost::type_traits::ice_not< ::abt_boost::is_same<t_no_cv, bool>::value>::value>
         ::value),
         typename mpl::if_<
            is_same<t_no_cv, unsigned char>,
            signed char,
            typename mpl::if_<
               is_same<t_no_cv, unsigned short>,
               signed short,
               typename mpl::if_<
                  is_same<t_no_cv, unsigned int>,
                  int,
                  typename mpl::if_<
                     is_same<t_no_cv, unsigned long>,
                     long,
#if defined(BOOST_HAS_LONG_LONG)
                     abt_boost::long_long_type
#elif defined(BOOST_HAS_MS_INT64)
                     __int64
#else
                     long
#endif
                  >::type
               >::type
            >::type
         >::type,
         // Not a regular integer type:
         typename mpl::if_c<
            sizeof(t_no_cv) == sizeof(unsigned char),
            signed char,
            typename mpl::if_c<
               sizeof(t_no_cv) == sizeof(unsigned short),
               signed short,
               typename mpl::if_c<
                  sizeof(t_no_cv) == sizeof(unsigned int),
                  int,
                  typename mpl::if_c<
                     sizeof(t_no_cv) == sizeof(unsigned long),
                     long,
#if defined(BOOST_HAS_LONG_LONG)
                     abt_boost::long_long_type
#elif defined(BOOST_HAS_MS_INT64)
                     __int64
#else
                     long
#endif
                  >::type
               >::type
            >::type
         >::type
      >::type
   >::type base_integer_type;
   
   // Add back any const qualifier:
   typedef typename mpl::if_<
      is_const<T>,
      typename add_const<base_integer_type>::type,
      base_integer_type
   >::type const_base_integer_type;
   
   // Add back any volatile qualifier:
   typedef typename mpl::if_<
      is_volatile<T>,
      typename add_volatile<const_base_integer_type>::type,
      const_base_integer_type
   >::type type;
};


} // namespace detail

BOOST_TT_AUX_TYPE_TRAIT_DEF1(make_signed,T,typename abt_boost::detail::make_signed_imp<T>::type)

} // namespace abt_boost

#include <boost/type_traits/detail/type_trait_undef.hpp>

#endif // BOOST_TT_ADD_REFERENCE_HPP_INCLUDED

