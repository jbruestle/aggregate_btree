
#ifndef __spatial_h__
#define __spatial_h__

#include "hilbert.h"

template<size_t dims>
struct point
{
	double coords[dims];
};

template<size_t dims>
struct bounding_region
{
	point<dims> min_point;
	point<dims> max_point;
};

class spatial_less 
{
public:
	template<size_t dims>
	bool operator()(const point<dims>& a, const point<dims>& b) const
	{
		return hilbert_ieee_cmp(dims, a.coords, b.coords) < 0;
	}
};

class spatial_accumulate
{
public:
	template<size_t dims>
	void operator()(bounding_region<dims>& a, const bounding_region<dims>& b)
	{
		for(size_t i = 0; i < dims; i++)
		{
			a.min_point.coords[i] = std::min(a.min_point.coords[i], b.min_point.coords[i]);
			a.max_point.coords[i] = std::max(a.max_point.coords[i], b.max_point.coords[i]);
		}
	}
};

#endif
