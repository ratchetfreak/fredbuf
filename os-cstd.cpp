#include "os.h"

#include <stdlib.h>

#include "macros.h"
#include "enum-utils.h"

namespace OS
{
    namespace
    {
        constexpr SystemInfo cstd_sys_info =
        {
            .page_size = PageSize{ KB(4) },
            .large_page_size = PageSize{ MB(1) },
            .allocation_granularity = AllocGranularity{ KB(4) },
        };
    } // namespace [anon]
    // Queries.
    // System information.
    const SystemInfo* system_info()
    {
        return &cstd_sys_info;
    }

    // Memory allocation.
    void* mem_reserve(AllocationSize size)
    {
        return malloc(rep(size));
    }

    bool mem_commit(void*, AllocationSize)
    {
        // On a usual platform, this would turn into a request for new pages.
        return true;
    }

    void mem_decommit(void*, AllocationSize)
    {
        // On a usual platform, this would turn into a release of pages.
    }

    void mem_release(void* ptr, AllocationSize)
    {
        // Some platforms may need the allocation size.
        free(ptr);
    }

    void* mem_reserve_large(AllocationSize size)
    {
        return malloc(rep(size));
    }

    bool mem_commit_large(void*, AllocationSize)
    {
        // On a usual platform, this would turn into a request for new pages.
        return true;
    }
} // namespace OS