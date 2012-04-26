// Copyright David Abrahams 2002.
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
#ifndef REGISTER_PTR_TO_PYTHON_HPP
#define REGISTER_PTR_TO_PYTHON_HPP

#include <boost/python/pointee.hpp>
#include <boost/python/object.hpp>
#include <boost/python/object/class_wrapper.hpp>

namespace abt_boost{} namespace boost = abt_boost; namespace abt_boost{ namespace python {
    
template <class P>
void register_ptr_to_python(BOOST_EXPLICIT_TEMPLATE_TYPE(P))
{
    typedef typename abt_boost::python::pointee<P>::type X;
    objects::class_value_wrapper<
        P
      , objects::make_ptr_instance<
            X
          , objects::pointer_holder<P,X>
        >
    >();
}           

}} // namespace abt_boost::python

#endif // REGISTER_PTR_TO_PYTHON_HPP


