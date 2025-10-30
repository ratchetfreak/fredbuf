#include "arena.h"

#include <cassert>

#include "macros.h"
#include "os.h"
#include "util.h"

namespace Arena
{
    namespace
    {
        template <typename T>
        constexpr T align_pow_2(T x, T y)
        {
            return (x + y - 1) & (~(y - 1));
        }

        // The actual arena header is much smaller than this, but skipping more bytes guarantees we can
        // add some more meta info about the header later.
        constexpr size_t arena_header = 128;
        static_assert(sizeof(Arena) <= arena_header);

        Arena* alloc_internal(ArenaCreateParams params)
        {
            // Ensure that we round up allocations to keep sizes within powers of 2.
            auto reserve_size = params.reserve_size;
            auto commit_size = params.commit_size;
            if (implies(params.flags, Flags::LargePages))
            {
                reserve_size = ReserveSize{ align_pow_2(rep(reserve_size), rep(OS::system_info()->large_page_size)) };
                commit_size = CommitSize{ align_pow_2(rep(commit_size), rep(OS::system_info()->large_page_size)) };
            }
            else
            {
                reserve_size = ReserveSize{ align_pow_2(rep(reserve_size), rep(OS::system_info()->page_size)) };
                commit_size = CommitSize{ align_pow_2(rep(commit_size), rep(OS::system_info()->page_size)) };
            }
            void* base = nullptr;
            if (implies(params.flags, Flags::LargePages))
            {
                base = OS::mem_reserve_large(OS::AllocationSize{ rep(reserve_size) });
                OS::mem_commit_large(base, OS::AllocationSize{ rep(commit_size) });
            }
            else
            {
                base = OS::mem_reserve(OS::AllocationSize{ rep(reserve_size) });
                OS::mem_commit(base, OS::AllocationSize{ rep(commit_size) });
            }
            // In the off chance that the OS decided to reuse this memory region, we must unpoison it first prior to writing to it.
            ASAN_UNPOISON_MEMORY_REGION(base, arena_header);
            Arena* arena = reinterpret_cast<Arena*>(base);
            arena->prev = nullptr;
            arena->current = arena;
            arena->flags = params.flags;
            arena->req_cmt_size = params.commit_size;
            arena->req_res_size = params.reserve_size;
            arena->base_pos = Position{ 0 };
            arena->pos = Position{ arena_header };
            arena->os_cmt = commit_size;
            arena->os_res = reserve_size;
            ASAN_POISON_MEMORY_REGION(base, rep(commit_size));
            ASAN_UNPOISON_MEMORY_REGION(base, arena_header);
            return arena;
        }

        void* push_internal(Arena* arena, AllocSize size, Alignment align, ZeroMem zero)
        {
            Arena* current = arena->current;
            Position pos_pre = Position{ align_pow_2(rep(current->pos), rep(align)) };
            Position pos_post = extend(pos_pre, rep(size));
            // Chain, if necessary.
            if (rep(current->os_res) < rep(pos_post) and not (implies(arena->flags, Flags::NoChain)))
            {
                Arena* new_blk = nullptr;
                ReserveSize res_size = current->req_res_size;
                CommitSize cmt_size = current->req_cmt_size;
                if (rep(size) + arena_header > rep(res_size))
                {
                    res_size = ReserveSize{ align_pow_2(rep(size) + arena_header, rep(align)) };
                    cmt_size = CommitSize{ align_pow_2(rep(size) + arena_header, rep(align)) };
                }
                ArenaCreateParams params{
                    .flags = current->flags,
                    .reserve_size = res_size,
                    .commit_size = cmt_size
                };
                new_blk = alloc(params);
                new_blk->base_pos = extend(current->base_pos, rep(current->os_res));
                SLLStackPush_N(arena->current, new_blk, prev);

                current = new_blk;
                // Recompute the position based on the current.
                pos_pre = Position{ align_pow_2(rep(current->pos), rep(align)) };
                pos_post = extend(pos_pre, rep(size));
            }
            // Now we figure out what the zero target is.
            uint64_t size_to_zero = 0;
            if (is_yes(zero))
            {
                size_to_zero = std::min(rep(current->os_cmt), rep(pos_post)) - rep(pos_pre);
            }

            // Commit new pages if necessary.
            if (rep(current->os_cmt) < rep(pos_post))
            {
                uint64_t cmt_post_aligned = rep(pos_post) + rep(current->req_cmt_size) - 1;
                cmt_post_aligned -= cmt_post_aligned % rep(current->req_cmt_size);
                uint64_t cmt_post_clamped = std::min(cmt_post_aligned, rep(current->os_res));
                uint64_t cmt_size = cmt_post_clamped - rep(current->os_cmt);
                uint8_t* cmt_ptr = reinterpret_cast<uint8_t*>(current) + rep(current->os_cmt);
                if (implies(current->flags, Flags::LargePages))
                {
                    OS::mem_commit_large(cmt_ptr, OS::AllocationSize{ cmt_size });
                }
                else
                {
                    OS::mem_commit(cmt_ptr, OS::AllocationSize{ cmt_size });
                }
                current->os_cmt = CommitSize{ cmt_post_clamped };
            }
            // Push onto current block.
            void* result = nullptr;
            if (rep(current->os_cmt) >= rep(pos_post))
            {
                result = reinterpret_cast<uint8_t*>(current) + rep(pos_pre);
                current->pos = pos_post;
                ASAN_UNPOISON_MEMORY_REGION(result, rep(size));
                if (size_to_zero != 0)
                {
                    memset(result, 0, size_to_zero);
                }
            }
            return result;
        }

        // Globals.
        // Note: This is not thread-safe.  Your arena implementation should make this either thread-local or have a per-thread lookup somewhere.
        ScratchArenas global_scratch_arenas;
    } // namespace [anon]

    // Scratch arena setup.
    void populate_scratch_arenas(ScratchArenas scratch_arenas)
    {
        global_scratch_arenas = scratch_arenas;
    }

    // Arena creation/destruction.
    Arena* alloc(ArenaCreateParams params)
    {
        return alloc_internal(params);
    }

    void release(Arena* arena)
    {
        for (Arena* a = arena->current, *prev = nullptr; a != nullptr; a = prev)
        {
            prev = a->prev;
            OS::mem_release(a, OS::AllocationSize{ rep(a->os_res) });
        }
    }

    // Basic push/pop core functions.
    void* push(Arena* arena, AllocSize size, Alignment align, ZeroMem zero)
    {
        return push_internal(arena, size, align, zero);
    }

    Position pos(const Arena* arena)
    {
        const Arena* current = arena->current;
        return extend(current->base_pos, rep(current->pos));
    }

    void pop_to(Arena* arena, Position pos)
    {
        Position big_pos = Position{ std::max(arena_header, rep(pos)) };
        Arena* current = arena->current;
        for (Arena* prev = nullptr; current->base_pos >= big_pos; current = prev)
        {
            prev = current->prev;
            OS::mem_release(current, OS::AllocationSize{ rep(current->os_res) });
        }
        arena->current = current;
        Position new_pos = Position{ rep(big_pos) - rep(current->base_pos) };
        assert(new_pos <= current->pos);
        ASAN_POISON_MEMORY_REGION(reinterpret_cast<uint8_t*>(current) + rep(new_pos), (rep(current->pos) - rep(new_pos)));
        current->pos = new_pos;
    }

    // Push/pop helpers.
    void clear(Arena* arena)
    {
        pop_to(arena, Position{ 0 });
    }

    void pop(Arena* arena, AllocSize size)
    {
        Position old_pos = pos(arena);
        Position new_pos = old_pos;
        if (rep(size) < rep(old_pos))
        {
            new_pos = Position{ rep(old_pos) - rep(size) };
        }
        pop_to(arena, new_pos);
    }

    // Temporary arena allocation.
    Temp temp_begin(Arena* arena)
    {
        Temp temp{
            .arena = arena,
            .pos = pos(arena)
        };
        return temp;
    }

    Arena* nil_arena()
    {
        return (Arena*)(void*)-1;
    }

    void temp_end(Temp temp)
    {
        pop_to(temp.arena, temp.pos);
    }
    
    Temp scratch_begin_vars(const char* file, int line, ...)
    {
       
        Arena* result = nullptr;
        Arena** arena_ptr = global_scratch_arenas.arenas;
        for(uint64_t i = 0; i < global_scratch_arenas.size; ++i, ++arena_ptr)
        {
            bool has_conflict = false;
             va_list args;
            va_start(args, line);
            
            Arena* conflict_ptr = va_arg(args, Arena*);
            while (conflict_ptr != nil_arena())
            {
                
                if(*arena_ptr == conflict_ptr)
                {
                    has_conflict = true;
                    break;
                }
            }
            va_end(args);
            if(not has_conflict)
            {
                result = *arena_ptr;
                break;
            }
        
        }
        Temp tmp = temp_begin(result);
        FRED_UNUSED(file);
        FRED_UNUSED(line);
        return tmp;
    }

    // (Related to above) temporary per-thread scratch arenas.
    Temp scratch_begin(Conflicts conflicts, const char* file, int line)
    {
        Arena* result = nullptr;
        Arena** arena_ptr = global_scratch_arenas.arenas;
        for(uint64_t i = 0; i < global_scratch_arenas.size; ++i, ++arena_ptr)
        {
            Arena** conflict_ptr = conflicts.conflicts;
            bool has_conflict = false;
            for(uint64_t j = 0; j < conflicts.count; ++j, ++conflict_ptr)
            {
                if(*arena_ptr == *conflict_ptr)
                {
                    has_conflict = true;
                    break;
                }
            }
            if(not has_conflict)
            {
                result = *arena_ptr;
                break;
            }
        }
        Temp tmp = temp_begin(result);
        FRED_UNUSED(file);
        FRED_UNUSED(line);
        return tmp;
    }

    void scratch_end(Temp scratch)
    {
        temp_end(scratch);
    }
} // namespace Arena