#pragma once

#include "types.h"

namespace OS
{
    enum class PageSize : uint64_t { };
    enum class AllocGranularity : uint64_t { };

    struct SystemInfo
    {
        PageSize page_size;
        PageSize large_page_size;
        AllocGranularity allocation_granularity;
    };

    enum class AllocationSize : uint64_t { };

    // Queries.
    // System information.
    const SystemInfo* system_info();

    // Memory allocation.
    void* mem_reserve(AllocationSize size);
    bool mem_commit(void* ptr, AllocationSize size);
    void mem_decommit(void* ptr, AllocationSize size);
    void mem_release(void* ptr, AllocationSize size);

    void* mem_reserve_large(AllocationSize size);
    bool mem_commit_large(void* ptr, AllocationSize size);
} // namespace OS