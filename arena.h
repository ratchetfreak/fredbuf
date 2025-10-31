#pragma once

#include <algorithm>
#include <type_traits>

#include "macros.h"
#include "types.h"

namespace Arena
{
    enum class Flags : uint32_t
    {
        None       = 0,
        NoChain    = 1U << 0,
        LargePages = 1U << 1,
    };

    enum class ReserveSize : uint64_t { };
    enum class CommitSize : uint64_t { };
    enum class Position : uint64_t { };
    enum class AllocSize : uint64_t { };
    enum class Alignment : uint64_t { };

    enum class ZeroMem : bool { No, Yes };

    struct ArenaCreateParams
    {
        Flags flags = Flags::None;
        ReserveSize reserve_size = ReserveSize{ MB(64) };
        CommitSize commit_size = CommitSize{ KB(64) };
    };

    inline constexpr ArenaCreateParams default_params{};

    struct Arena
    {
        Arena* prev;
        Arena* current;
        Flags flags;
        CommitSize req_cmt_size;  // Requested commit size.
        ReserveSize req_res_size; // Requested reserve size.
        Position base_pos;
        Position pos;
        CommitSize os_cmt; // Computed commit size for OS.
        ReserveSize os_res; // Computed reserve size for the OS.
    };

    struct Temp
    {
        Arena* arena;
        Position pos;
        ~Temp();
    };

    struct Conflicts
    {
        Arena** conflicts;
        uint64_t count;
    };

    struct ScratchArenas
    {
        Arena** arenas;
        uint64_t size;
    };

    inline constexpr Conflicts no_conflicts = {};

    // Scratch arena setup.
    void populate_scratch_arenas(ScratchArenas scratch_arenas);

    // Arena creation/destruction.
    Arena* alloc(ArenaCreateParams params);
    void release(Arena* arena);

    // Basic push/pop core functions.
    void* push(Arena* arena, AllocSize size, Alignment align, ZeroMem zero);
    Position pos(const Arena* arena);
    void pop_to(Arena* arena, Position pos);

    // Push/pop helpers.
    void clear(Arena* arena);
    void pop(Arena* arena, AllocSize size);

    // Temporary arena allocation.
    Temp temp_begin(Arena* arena);
    void temp_end(Temp &temp);

    Arena* nil_arena();
    #define scratch_begin_vararg(...) scratch_begin_vars(__FILE__, __LINE__, __VA_ARGS__, Arena::nil_arena())
    
    Temp scratch_begin_vars(const char* file, int line, ...);

    // (Related to above) temporary per-thread scratch arenas.
    Temp scratch_begin(Conflicts conflicts, const char* file = __builtin_FILE(), int line = __builtin_LINE());
    void scratch_end(Temp &scratch);
    void validate_scratch_arenas();

    // Typed helper functions.
    template <typename T>
    T* push_array_no_zero_aligned(Arena* arena, size_t count, Alignment align)
    {
        static_assert(std::is_trivially_destructible_v<T>);
        return static_cast<T*>(push(arena, AllocSize{ sizeof(T)*count }, align, ZeroMem::No));
    }

    template <typename T>
    T* push_array_aligned(Arena* arena, size_t count, Alignment align)
    {
        static_assert(std::is_trivially_destructible_v<T>);
        return static_cast<T*>(push(arena, AllocSize{ sizeof(T)*count }, align, ZeroMem::Yes));
    }

    template <typename T>
    T* push_array_no_zero(Arena* arena, size_t count)
    {
        return push_array_no_zero_aligned<T>(arena, count, std::max(Alignment{ 8 }, Alignment{ alignof(T) }));
    }

    template <typename T>
    T* push_array(Arena* arena, size_t count)
    {
        return push_array_aligned<T>(arena, count, std::max(Alignment{ 8 }, Alignment{ alignof(T) }));
    }
} // namespace Arena