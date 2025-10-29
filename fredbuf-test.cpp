#include <stdio.h>

#include <cassert>



#include "arena.h"
#include "fred-strings.h"

#define COUNT_ALLOC
#include "ratbuf.h"
#include "ratbuf_btree.h"
#define PieceTree RatchetPieceTree 


// Debug helper from fredbuf.cpp.
void print_buffer(const PieceTree::Tree* tree);

void assume_buffer_snapshots(const PieceTree::Tree* tree, String8 expected, PieceTree::CharOffset offset, int locus)
{
    // Owning snapshot.
    {
        auto scratch = Arena::scratch_begin(Arena::no_conflicts);
        auto* owning_snap = tree->owning_snap(scratch.arena);
        PieceTree::TreeWalker walker{ scratch.arena, owning_snap, offset };
        String8List serial_lst{};
        str8_serial_begin(scratch.arena, &serial_lst);
        while (not walker.exhausted())
        {
            str8_serial_push_char(scratch.arena, &serial_lst, walker.next());
        }
        assert(walker.remaining() == PieceTree::Length{ 0 });

        String8 buf = str8_serial_end(scratch.arena, serial_lst);

        if (not str8_match_exact(expected, buf))
        {
            fprintf(stderr, "owning snapshot buffer string '%*s' did not match expected value of '%*s'. Line(%d)\n", int(buf.size), buf.str, int(expected.size), expected.str, locus);
            assert(false);
        }
        release_owning_snap(owning_snap);
        Arena::scratch_end(scratch);
    }
    // Reference snapshot.
    {
        auto scratch = Arena::scratch_begin(Arena::no_conflicts);
        auto ref_snap = tree->ref_snap();
        PieceTree::TreeWalker walker{ scratch.arena, &ref_snap, offset };
        String8List serial_lst{};
        str8_serial_begin(scratch.arena, &serial_lst);
        while (not walker.exhausted())
        {
            str8_serial_push_char(scratch.arena, &serial_lst, walker.next());
        }
        assert(walker.remaining() == PieceTree::Length{ 0 });

        String8 buf = str8_serial_end(scratch.arena, serial_lst);

        if (not str8_match_exact(expected, buf))
        {
            fprintf(stderr, "reference snapshot buffer string '%*s' did not match expected value of '%*s'. Line(%d)\n", int(buf.size), buf.str, int(expected.size), expected.str, locus);
            assert(false);
        }
        Arena::scratch_end(scratch);
    }
}

void assume_reverse_buffer(const PieceTree::Tree* tree, String8 forward_buf, PieceTree::CharOffset offset, int locus)
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    PieceTree::ReverseTreeWalker walker{ scratch.arena, tree, offset };
    String8List serial_lst{};
    str8_serial_begin(scratch.arena, &serial_lst);
    while (not walker.exhausted())
    {
        str8_serial_push_char(scratch.arena, &serial_lst, walker.next());
    }
    assert(walker.remaining() == PieceTree::Length{ 0 });

    String8 buf = str8_serial_end(scratch.arena, serial_lst);

    if (buf.size != forward_buf.size)
    {
        fprintf(stderr, "reference snapshot buffer string '%.*s' did not match expected value of '%.*s'. Line(%d)\n", int(buf.size), buf.str, int(forward_buf.size), forward_buf.str, locus);
        assert(false);
    }

    // Walk 'forward_buf' in reverse and compare.
    bool result = true;
    uint64_t forward_off = forward_buf.size - 1;
    for EachIndex(i, buf.size)
    {
        if (buf.str[i] != forward_buf.str[forward_off - i])
        {
            result = false;
            break;
        }
    }

    if (not result)
    {
        fprintf(stderr, "reference snapshot buffer string '%.*s' did not match expected value of '%.*s'. Line(%d)\n", int(buf.size), buf.str, int(forward_buf.size), forward_buf.str, locus);
        assert(false);
    }
    Arena::scratch_end(scratch);
}

void assume_buffer(const PieceTree::Tree* tree, String8View expected, int locus = __builtin_LINE())
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    constexpr auto start = PieceTree::CharOffset{ 0 };
    PieceTree::TreeWalker walker{ scratch.arena, tree, start };
    String8List serial_lst{};
    str8_serial_begin(scratch.arena, &serial_lst);
    while (not walker.exhausted())
    {
        str8_serial_push_char(scratch.arena, &serial_lst, walker.next());
    }
    assert(walker.remaining() == PieceTree::Length{ 0 });

    String8 buf = str8_serial_end(scratch.arena, serial_lst);

    if (not str8_match_exact(str8_mut(expected), buf))
    {
        fprintf(stderr, "buffer string '%*s' did not match expected value of '%*s'. Line(%d)\n", int(buf.size), buf.str, int(expected.size), expected.str, locus);
        assert(false);
    }
    assume_buffer_snapshots(tree, str8_mut(expected), start, locus);
    assume_reverse_buffer(tree, buf, start + retract(tree->length()), locus);
    Arena::scratch_end(scratch);
}

void assume_lineRange(PieceTree::LineRange actual, PieceTree::LineRange expected)
{
    if(expected.first != actual.first || expected.last != actual.last)
    {
        
        size_t slen = snprintf(NULL, 0,"Range {%lld,%lld} does not match epected result {%lld,%lld} ", actual.first, actual.last, expected.first, expected.last);

         std::string s ;
        s.resize(slen);
         slen = snprintf(s.data(), s.size(),"Range {%lld,%lld} does not match epected result {%lld,%lld} ", actual.first, actual.last, expected.first, expected.last);
        fprintf(stderr, "%s\n", s.c_str());
        assert(false);
    }
}

using namespace PieceTree;
void test1()
{
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("A\nB\nC\nD")));
    Tree* tree = tree_builder_finish(&builder);
    assume_buffer(tree, str8_literal("A\nB\nC\nD"));

    tree->remove(CharOffset{ 4 }, Length{ 1 });
    tree->remove(CharOffset{ 3 }, Length{ 1 });

    print_buffer(tree);
    assume_buffer(tree, str8_literal("A\nB\nD"));

    release_tree(tree);
}

void test2()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    auto tree = tree_builder_finish(&builder);
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("a")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("s")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("d")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("f")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("\n")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("a")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("s")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("d")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("f")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("\n")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("a")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("s")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("d")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("f")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("\n")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("a")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("s")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("d")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("f")));
    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("\n")));
#if 1
    tree->insert(CharOffset{ 1 }, str8_mut(str8_literal("a")));
    tree->insert(CharOffset{ 2 }, str8_mut(str8_literal("s")));
    tree->insert(CharOffset{ 3 }, str8_mut(str8_literal("d")));
    tree->insert(CharOffset{ 4 }, str8_mut(str8_literal("f")));
    tree->insert(CharOffset{ 5 }, str8_mut(str8_literal("\n")));
    tree->insert(CharOffset{ 6 }, str8_mut(str8_literal("a")));
    tree->insert(CharOffset{ 12 }, str8_mut(str8_literal("s")));
    tree->insert(CharOffset{ 15 }, str8_mut(str8_literal("d")));
    tree->insert(CharOffset{ 17 }, str8_mut(str8_literal("f")));
    tree->insert(CharOffset{ 18 }, str8_mut(str8_literal("\n")));
    tree->insert(CharOffset{ 2 }, str8_mut(str8_literal("a")));
    tree->insert(CharOffset{ 21 }, str8_mut(str8_literal("s")));
    tree->insert(CharOffset{ 21 }, str8_mut(str8_literal("d")));
    tree->insert(CharOffset{ 23 }, str8_mut(str8_literal("f")));
    tree->insert(CharOffset{ 29 }, str8_mut(str8_literal("\n")));
    tree->insert(CharOffset{ 30 }, str8_mut(str8_literal("a")));
    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("s")));
    tree->insert(CharOffset{ 1 }, str8_mut(str8_literal("d")));
    tree->insert(CharOffset{ 10 }, str8_mut(str8_literal("f")));
    tree->insert(CharOffset{ 11 }, str8_mut(str8_literal("\n")));
#endif
    auto line = Line{ 1 };
    auto range = tree->get_line_range(line);
    printf("Line{%zu} range: first{%zu} last{%zu}\n", rep(line), rep(range.first), rep(range.last));
    String8 buf = tree->get_line_content(scratch.arena, line);
    printf("content: %s\n", buf.str);
    printf("Line number: %zu\n", rep(tree->line_at(range.first)));

    print_buffer(tree);

    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });
    tree->remove(CharOffset{ 5 }, Length{ 1 });

    print_buffer(tree);
    assume_buffer(tree, str8_literal("sdaaadff\n\ndsfasdf\n\naasdf\n"));

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test3()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("Hello")));
    tree_builder_accept(arena, &builder, str8_mut(str8_literal(",")));
    tree_builder_accept(arena, &builder, str8_mut(str8_literal(" ")));
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("World")));
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("!")));
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("\nThis is a second line.")));
    tree_builder_accept(arena, &builder, str8_mut(str8_literal(" Continue...\nANOTHER!")));
    Tree* tree = tree_builder_finish(&builder);

    print_tree(*tree);

    String8 buf;

    printf("line content at 1:\n");
    buf = tree->get_line_content(scratch.arena, Line{ 1 });
    printf("%.*s\n", int(buf.size), buf.str);

    printf("line content at 2:\n");
    buf = tree->get_line_content(scratch.arena, Line{ 2 });
    printf("%.*s\n", int(buf.size), buf.str);

    printf("line content at 3:\n");
    buf = tree->get_line_content(scratch.arena, Line{ 3 });
    printf("%.*s\n", int(buf.size), buf.str);

    tree->insert(CharOffset{ 37 }, str8_mut(str8_literal("Hello")));

    printf("line content at 1:\n");
    buf = tree->get_line_content(scratch.arena, Line{ 1 });
    printf("%.*s\n", int(buf.size), buf.str);

    printf("line content at 2:\n");
    buf = tree->get_line_content(scratch.arena, Line{ 2 });
    printf("%.*s\n", int(buf.size), buf.str);

    print_buffer(tree);

    auto off = CharOffset{ 13 };
    auto len = Length{ 5 };
    printf("--- Delete at off{%zu}, len{%zu} ---\n", rep(off), rep(len));
    tree->remove(off, len);

#if 0
    printf("line content at 1:\n");
    buf = tree->get_line_content(scratch.arena, Line{ 1 });
    printf("%.*s\n", int(buf.size), buf.str);

    printf("line content at 2:\n");
    buf = tree->get_line_content(scratch.arena, Line{ 2 });
    printf("%.*s\n", int(buf.size), buf.str);
#endif

    print_buffer(tree);

    off = CharOffset{ 37 };
    len = Length{ 5 };
    printf("--- Delete at off{%zu}, len{%zu} ---\n", rep(off), rep(len));
    tree->remove(off, len);

#if 0
    printf("line content at 1:\n");
    buf = tree->get_line_content(scratch.arena, Line{ 1 });
    printf("%.*s\n", int(buf.size), buf.str);

    printf("line content at 2:\n");
    buf = tree->get_line_content(scratch.arena, Line{ 2 });
    printf("%.*s\n", int(buf.size), buf.str);
#endif

    print_buffer(tree);

    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("a")));
    print_buffer(tree);

    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("a")));
    print_buffer(tree);

    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("a")));
    print_buffer(tree);

    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("a")));
    print_buffer(tree);

    tree->insert(CharOffset{ 0 } + tree->length(), str8_mut(str8_literal("END!!")));
    print_buffer(tree);

    tree->remove(CharOffset{ 52 }, Length{ 4 });
    print_buffer(tree);

    //print_buffer(tree, CharOffset{ 26 });

    tree->insert(CharOffset{} + tree->length(), str8_mut(str8_literal("\nfoobar\nnext\nnextnext\nnextnextnext")));
    tree->insert(CharOffset{} + tree->length(), str8_mut(str8_literal("\nfoobar2\nnext\nnextnext\nnextnextnext")));

    print_buffer(tree);

    auto total_lines = Line{ rep(tree->line_feed_count()) + 1 };
    for (Line line = Line{ 1 }; line <= total_lines;line = extend(line))
    {
        auto range = tree->get_line_range(line);
        printf("Line{%zu} range: first{%zu} last{%zu}\n", rep(line), rep(range.first), rep(range.last));
        buf = tree->get_line_content(scratch.arena, line);
        printf("content: %s\n", buf.str);
        printf("Line number: %zu\n", rep(tree->line_at(range.first)));
    }

    printf("out of range line:\n");
    auto range = tree->get_line_range(Line{ 99 });
    printf("Line{%zu} range: first{%zu} last{%zu}\n", size_t{99}, rep(range.first), rep(range.last));
    buf = tree->get_line_content(scratch.arena, Line{ 99 });
    printf("content: %s\n", buf.str);
    printf("Line number: %zu\n", rep(tree->line_at(range.first)));
#if 0
    tree->remove(PieceTree::CharOffset{ 37 }, PieceTree::Length{ 5 });

    printf("line content at 1:\n");
    buf = tree->get_line_content(scratch.arena, PieceTree::Line{ 1 });
    printf("%.*s\n", int(buf.size), buf.str);

    printf("line content at 2:\n");
    buf = tree->get_line_content(scratch.arena, PieceTree::Line{ 2 });
    printf("%.*s\n", int(buf.size), buf.str);

    tree->remove(PieceTree::CharOffset{ 25 }, PieceTree::Length{ 5 });

    printf("line content at 1:\n");
    buf = tree->get_line_content(scratch.arena, PieceTree::Line{ 1 });
    printf("%.*s\n", int(buf.size), buf.str);

    printf("line content at 2:\n");
    buf = tree->get_line_content(scratch.arena, PieceTree::Line{ 2 });
    printf("%.*s\n", int(buf.size), buf.str);
#endif

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test4()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("ABCD")));
    Tree* tree = tree_builder_finish(&builder);

    tree->insert(CharOffset{4}, str8_mut(str8_literal("a")));

    assume_buffer(tree, str8_literal("ABCDa"));

    tree->remove(CharOffset{3}, Length{2});

    assume_buffer(tree, str8_literal("ABC"));

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test5()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("")));
    auto tree = tree_builder_finish(&builder);

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("a")));

    assume_buffer(tree, str8_literal("a"));

    tree->remove(CharOffset{ 0 }, Length{ 1 });

    assume_buffer(tree, str8_literal(""));

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test6()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("Hello, World!")));
    Tree* tree = tree_builder_finish(&builder);

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("a")));
    tree->insert(CharOffset{ 1 }, str8_mut(str8_literal("b")));
    tree->insert(CharOffset{ 2 }, str8_mut(str8_literal("c")));

    assume_buffer(tree, str8_literal("abcHello, World!"));

    tree->remove(CharOffset{ 0 }, Length{ 3 });

    assume_buffer(tree, str8_literal("Hello, World!"));

    auto r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, str8_literal("abcHello, World!"));

    r = tree->try_redo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, str8_literal("Hello, World!"));

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, str8_literal("abcHello, World!"));

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, str8_literal("Hello, World!"));

    r = tree->try_undo(CharOffset{ 0 });
    assert(not r.success);

    r = tree->try_redo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, str8_literal("abcHello, World!"));

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, str8_literal("Hello, World!"));

    // Destroy the redo stack.

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("NEW")));

    assume_buffer(tree, str8_literal("NEWHello, World!"));

    r = tree->try_redo(CharOffset{ 0 });
    assert(not r.success);

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, str8_literal("Hello, World!"));

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test7()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("ABC")));
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("DEF")));
    Tree* tree = tree_builder_finish(&builder);

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("foo")));

    assume_buffer(tree, str8_literal("fooABCDEF"));

    tree->remove(CharOffset{ 6 }, Length{ 3 });

    assume_buffer(tree, str8_literal("fooABC"));

    String8 buf = tree->get_line_content(scratch.arena, Line{ 1 });

    assert(str8_match_exact(buf, str8_mut(str8_literal("fooABC"))));

    for EachIndex(i, buf.size)
    {
        printf("%c", buf.str[i]);
    }
    printf("\n");

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test8()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("Hello, World!")));
    Tree* tree = tree_builder_finish(&builder);

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("a")), PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, str8_literal("aHello, World!"));

    auto r = tree->try_undo(CharOffset{ 0 });
    assert(not r.success);

    tree->remove(CharOffset{ 0 }, Length{ 1 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, str8_literal("Hello, World!"));

    r = tree->try_undo(CharOffset{ 0 });
    assert(not r.success);

    // Snap back to "Hello, World!"
    tree->commit_head(CharOffset{ 0 });
    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("a")), PieceTree::SuppressHistory::Yes);
    tree->insert(CharOffset{ 1 }, str8_mut(str8_literal("b")), PieceTree::SuppressHistory::Yes);
    tree->insert(CharOffset{ 2 }, str8_mut(str8_literal("c")), PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, str8_literal("abcHello, World!"));

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);
    assume_buffer(tree, str8_literal("Hello, World!"));

    // Snap back to "Hello, World!"
    tree->commit_head(CharOffset{ 0 });
    tree->remove(CharOffset{ 0 }, Length{ 7 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, str8_literal("World!"));

    tree->remove(CharOffset{ 5 }, Length{ 1 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, str8_literal("World"));

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);
    assume_buffer(tree, str8_literal("Hello, World!"));

    r = tree->try_redo(CharOffset{ 0 });
    assume_buffer(tree, str8_literal("World"));

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test9()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("Hello, World!")));
    Tree* tree = tree_builder_finish(&builder);

    // Create a scope for initial_commit so it does not escape after the release of the tree.
    {
        auto initial_commit = tree->head();

        tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("a")), PieceTree::SuppressHistory::Yes);
        assume_buffer(tree, str8_literal("aHello, World!"));

        auto r = tree->try_undo(CharOffset{ 0 });
        assert(not r.success);

        auto commit = tree->head();
        tree->snap_to(initial_commit);
        assume_buffer(tree, str8_literal("Hello, World!"));

        tree->snap_to(commit);
        assume_buffer(tree, str8_literal("aHello, World!"));

        tree->remove(CharOffset{ 0 }, Length{ 8 }, PieceTree::SuppressHistory::Yes);
        assume_buffer(tree, str8_literal("World!"));

        tree->snap_to(commit);
        assume_buffer(tree, str8_literal("aHello, World!"));

        tree->snap_to(initial_commit);
        assume_buffer(tree, str8_literal("Hello, World!"));

        // Create a new branch.
        tree->insert(CharOffset{ 13 }, str8_mut(str8_literal(" My name is fredbuf.")), PieceTree::SuppressHistory::Yes);
        assume_buffer(tree, str8_literal("Hello, World! My name is fredbuf."));

        auto branch = tree->head();

        // Revert back.
        tree->snap_to(commit);
        assume_buffer(tree, str8_literal("aHello, World!"));

        // Revert back to branch.
        tree->snap_to(branch);
        assume_buffer(tree, str8_literal("Hello, World! My name is fredbuf."));
    }

    release_tree(tree);
    Arena::scratch_end(scratch);
}

#ifdef TIMING_DATA
#include <chrono>

struct Stopwatch
{
    using Clock = std::chrono::high_resolution_clock;

    void start()
    {
        start_ = Clock::now();
    }

    void stop()
    {
        stop_  = Clock::now();
    }

    Clock::duration ticks() const
    {
        return stop_ - start_;
    }

    // helpers
    template <typename Tick>
    Tick to_ticks() const
    {
        return std::chrono::duration_cast<Tick>(ticks());
    }

    std::chrono::microseconds to_us() const
    {
        return to_ticks<std::chrono::microseconds>();
    }

    Clock::time_point start_ = { };
    Clock::time_point stop_ = { };
};

void time_buffer()
{
    Stopwatch sw;
    constexpr String8View initial_input = str8_literal(
R"(What is Lorem Ipsum?
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap
into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum
passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.)"
    );
    constexpr String8View inserted_buf_expected = str8_literal(
R"(What is Lorem Ipsum?
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaacenturies, but also the leap
into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum
passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.)"
    );
    constexpr String8View upper_half = str8_literal(
R"(enturies, but also the leap
into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum
passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.)"
    );
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(scratch.arena, &builder, str8_mut(initial_input));
    Tree* tree = tree_builder_finish(&builder);

    // Do not let initial_commit escape to the release of the tree.
    {
        auto initial_commit = tree->head();

        constexpr int timing_count = 10;
        std::chrono::microseconds timing_data[timing_count];
        // Append-like insertions in the middle.
        {
            for EachIndex(i, timing_count)
            {
                tree->snap_to(initial_commit);
                auto mid = extend(CharOffset{}, (rep(tree->length()) / 2));
                String8 ins_txt = str8_mut(str8_literal("a"));
                sw.start();
                for (int j = 0; j < 100; ++j)
                {
                    tree->insert(mid, ins_txt, SuppressHistory::Yes);
                    mid = extend(mid);
                }
                sw.stop();
                timing_data[i] = sw.to_us();
                assume_buffer(tree, inserted_buf_expected);
            }
            // Aggregate and display.
            printf("---------- Append-like insertions ----------\n");
            int64_t total_count = 0;
            for EachIndex(i, timing_count)
            {
                printf("[%u] = %ldus\n", unsigned(i), long(timing_data[i].count()));
                total_count += timing_data[i].count();
            }
            // Find mean.
            double mean = static_cast<double>(total_count) / timing_count;
            printf("Average: %.2fus\n", mean);
        }

        // In-place insertions in the middle.
        {
            for EachIndex(i, timing_count)
            {
                tree->snap_to(initial_commit);
                auto mid = extend(CharOffset{}, (rep(tree->length()) / 2));
                String8 ins_txt = str8_mut(str8_literal("a"));
                sw.start();
                for (int j = 0; j < 100; ++j)
                {
                    // Don't change 'mid' to append to the string.
                    tree->insert(mid, ins_txt, SuppressHistory::Yes);
                }
                sw.stop();
                timing_data[i] = sw.to_us();
                assume_buffer(tree, inserted_buf_expected);
            }
            // Aggregate and display.
            printf("---------- In-place insertions ----------\n");
            int64_t total_count = 0;
            for EachIndex(i, timing_count)
            {
                printf("[%u] = %ldus\n", unsigned(i), long(timing_data[i].count()));
                total_count += timing_data[i].count();
            }
            // Find mean.
            double mean = static_cast<double>(total_count) / timing_count;
            printf("Average: %.2fus\n", mean);
        }

        // Deleting characters to beginning starting at middle.
        {
            for EachIndex(i, timing_count)
            {
                tree->snap_to(initial_commit);
                auto mid = extend(CharOffset{}, (rep(tree->length()) / 2));
                sw.start();
                while (mid != CharOffset::Sentinel)
                {
                    // Don't change 'mid' to append to the string.
                    tree->remove(mid, Length{ 1 }, SuppressHistory::Yes);
                    mid = retract(mid);
                }
                sw.stop();
                timing_data[i] = sw.to_us();
                assume_buffer(tree, upper_half);
            }
            // Aggregate and display.
            printf("---------- Deletion starting at middle ----------\n");
            int64_t total_count = 0;
            for EachIndex(i, timing_count)
            {
                printf("[%u] = %ldus\n", unsigned(i), long(timing_data[i].count()));
                total_count += timing_data[i].count();
            }
            // Find mean.
            double mean = static_cast<double>(total_count) / timing_count;
            printf("Average: %.2fus\n", mean);
        }

        // Deleting half the characters starting at beginning.
        {
            for EachIndex(i, timing_count)
            {
                tree->snap_to(initial_commit);
                // This deletion needs to be inclusive of the midpoint (to be consistent with deletion above).
                auto len_to_del = Length{ (rep(tree->length()) / 2) + 1 };
                sw.start();
                while (len_to_del != Length{})
                {
                    // Don't change 'mid' to append to the string.
                    tree->remove(CharOffset{}, Length{ 1 }, SuppressHistory::Yes);
                    len_to_del = retract(len_to_del);
                }
                sw.stop();
                timing_data[i] = sw.to_us();
                assume_buffer(tree, upper_half);
            }
            // Aggregate and display.
            printf("---------- Deletion starting at beginning ----------\n");
            int64_t total_count = 0;
            for EachIndex(i, timing_count)
            {
                printf("[%u] = %ldus\n", unsigned(i), long(timing_data[i].count()));
                total_count += timing_data[i].count();
            }
            // Find mean.
            double mean = static_cast<double>(total_count) / timing_count;
            printf("Average: %.2fus\n", mean);
        }
    }

    release_tree(tree);
    Arena::scratch_end(scratch);
}
#endif // TIMING_DATA

void test10()
{

    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("He")));
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("llo, Worl")));
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("d!")));
    Tree* tree = tree_builder_finish(&builder);

    (void)tree;

/*

    {
        auto it = begin(tree);
        assert(*it == 'H');
        assert(*it == 'H');
        ++it;
        assert(*it == 'e');
        assert(*it == 'e');
        ++it;
        assert(*it == 'l');
        assert(*it == 'l');

        auto it2 = begin(tree);
        assert(it!=it2);
        ++it2;++it2;
        assert(it==it2);
    }
    {
        auto it = rbegin(tree);
        assert(*it == '!');
        assert(*it == '!');
        ++it;
        assert(*it == 'd');
        assert(*it == 'd');
        ++it;
        assert(*it == 'l');
        assert(*it == 'l');

        auto it2 = rbegin(tree);
        assert(it!=it2);
        ++it2;++it2;
        assert(it==it2);
    }*/
    release_tree(tree);
    Arena::scratch_end(scratch);
}


}

void test11()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);
    tree_builder_accept(arena, &builder, str8_mut(str8_literal("Hello, World!")));

    for(int i = 0; i< 1000; i++)
        tree_builder_accept(arena, &builder, str8_mut(str8_literal("Hello, World!")));
        
    Tree* tree = tree_builder_finish(&builder);

    tree->remove(CharOffset{ 14 }, retract(tree->length(),  14*2 ));
    assume_buffer(tree, str8_literal("Hello, World!H!Hello, World!"));
    release_tree(tree);
    Arena::scratch_end(scratch);
}
void test12()
{
    
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    Arena::Arena* arena = Arena::alloc(Arena::default_params);
    TreeBuilder builder = tree_builder_start(arena);

    for(int i = 0; i< 10; i++)
        tree_builder_accept(arena, &builder, str8_mut(str8_literal("Hello, World!")));
        
    for(int i = 0; i< 10; i++)
        tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("Hello, \rWorld!\r\n")));

    Tree* tree = tree_builder_finish(&builder);
    
    LineRange range = tree->get_line_range(Line(2));
    LineRange range_crlf = tree->get_line_range_crlf(Line(2));
    LineRange range_with_newline = tree->get_line_range_with_newline(Line(2));
    printf("range first=%zd, last=%zd\n", range.first, range.last);
    printf("range_crlf first=%zd, last=%zd\n", range_crlf.first, range_crlf.last);
    printf("range_with_newline first=%zd, last=%zd\n", range_with_newline.first, range_with_newline.last);
    
        
    LineRange expected_range; 
    expected_range.first=Offset(16); 
    expected_range.last=Offset(31);
    LineRange expected_range_crlf; 
    expected_range_crlf.first=Offset(16);
    expected_range_crlf.last=Offset(30);
    LineRange expected_range_with_newline; 
    expected_range_with_newline.first=Offset(16); 
    expected_range_with_newline.last=Offset(32);
    
    assume_lineRange(range, expected_range);
    assume_lineRange(range_crlf, expected_range_crlf);
    assume_lineRange(range_with_newline, expected_range_with_newline);
    
    //if (expected != buf)
    
    //printf("first=%zd, last=%zd\n", res.first, res.last);
    //assume_buffer(&tree, "Hello, World!H!Hello, World!");

    release_tree(tree);
    Arena::scratch_end(scratch);

}


int main()
{
    // Setup the scratch arenas.
    constexpr int arena_size = 2;
    Arena::Arena* scratch_arenas[arena_size];
    // Generally, you only need two arenas to handle all conflicts.
    scratch_arenas[0] = Arena::alloc(Arena::default_params);
    scratch_arenas[1] = Arena::alloc(Arena::default_params);
    Arena::populate_scratch_arenas({ scratch_arenas, arena_size });

    test1();
    printf("test1: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test2();
    printf("test2: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test3();
    printf("test3: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test4();
    printf("test4: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test5();
    printf("test5: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test6();
    printf("test6: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test7();
    printf("test7: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test8();
    printf("test8: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test9();
    printf("test9: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test10();
    printf("test10: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test11();
    printf("test11: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;
    test12();
    printf("test12: allocs=%zd, deallocs=%zd\n", alloc_count, dealloc_count);
    alloc_count=0;dealloc_count=0;

#ifdef TIMING_DATA
    time_buffer();
#endif // TIMING_DATA
}

#include "arena.cpp"
#include "fred-strings.cpp"

#if 1
#include "ratbuf.h"
#include "ratbuf_btree.cpp"
#define PieceTree RatchetPieceTree 
#else
#include "fredbuf.cpp"
#endif

#include "os-cstd.cpp"
