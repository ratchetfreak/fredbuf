#include <cassert>

#include <format>
#include <source_location>

#include "arena.h"
#include "fred-strings.h"
#include "fredbuf.h"

// Debug helper from fredbuf.cpp.
void print_buffer(const PieceTree::Tree* tree);

void assume_buffer_snapshots(const PieceTree::Tree* tree, std::string_view expected, PieceTree::CharOffset offset, std::source_location locus)
{
    // Owning snapshot.
    {
        auto scratch = Arena::scratch_begin(Arena::no_conflicts);
        auto* owning_snap = tree->owning_snap(scratch.arena);
        PieceTree::TreeWalker walker{ scratch.arena, owning_snap, offset };
        std::string buf;
        while (not walker.exhausted())
        {
            buf.push_back(walker.next());
        }
        assert(walker.remaining() == PieceTree::Length{ 0 });

        if (expected != buf)
        {
            auto s = std::format("owning snapshot buffer string '{}' did not match expected value of '{}'. Line({})", buf, expected, locus.line());
            fprintf(stderr, "%s\n", s.c_str());
            assert(false);
        }
        Arena::scratch_end(scratch);
    }
    // Reference snapshot.
    {
        auto scratch = Arena::scratch_begin(Arena::no_conflicts);
        auto owning_snap = tree->ref_snap();
        PieceTree::TreeWalker walker{ scratch.arena, &owning_snap, offset };
        std::string buf;
        while (not walker.exhausted())
        {
            buf.push_back(walker.next());
        }
        assert(walker.remaining() == PieceTree::Length{ 0 });

        if (expected != buf)
        {
            auto s = std::format("reference snapshot buffer string '{}' did not match expected value of '{}'. Line({})", buf, expected, locus.line());
            fprintf(stderr, "%s\n", s.c_str());
            assert(false);
        }
        Arena::scratch_end(scratch);
    }
}

void assume_reverse_buffer(const PieceTree::Tree* tree, std::string_view forward_buf, PieceTree::CharOffset offset, std::source_location locus)
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    PieceTree::ReverseTreeWalker walker{ scratch.arena, tree, offset };
    std::string buf;
    while (not walker.exhausted())
    {
        buf.push_back(walker.next());
    }
    assert(walker.remaining() == PieceTree::Length{ 0 });

    // Walk 'forward_buf' in reverse and compare.
    auto rfirst = rbegin(forward_buf);
    auto rlast = rend(forward_buf);
    const bool result = std::equal(rfirst, rlast, begin(buf), end(buf));
    if (not result)
    {
        auto s = std::format("Reversed buffer '{}' is not equal to forward buffer '{}'.  Line({})", buf, forward_buf, locus.line());
        fprintf(stderr, "%s\n", s.c_str());
        assert(false);
    }
    Arena::scratch_end(scratch);
}

void assume_buffer(const PieceTree::Tree* tree, std::string_view expected, std::source_location locus = std::source_location::current())
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    constexpr auto start = PieceTree::CharOffset{ 0 };
    PieceTree::TreeWalker walker{ scratch.arena, tree, start };
    std::string buf;
    while (not walker.exhausted())
    {
        buf.push_back(walker.next());
    }
    assert(walker.remaining() == PieceTree::Length{ 0 });

    if (expected != buf)
    {
        auto s = std::format("buffer string '{}' did not match expected value of '{}'. Line({})", buf, expected, locus.line());
        fprintf(stderr, "%s\n", s.c_str());
        assert(false);
    }
    assume_buffer_snapshots(tree, expected, start, locus);
    assume_reverse_buffer(tree, buf, start + retract(tree->length()), locus);
    Arena::scratch_end(scratch);
}

using namespace PieceTree;
void test1()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    TreeBuilder builder = tree_builder_start(scratch.arena);
    std::string buf;
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("A\nB\nC\nD")));
    Tree* tree = tree_builder_finish(&builder);
    assume_buffer(tree, "A\nB\nC\nD");

    tree->remove(CharOffset{ 4 }, Length{ 1 });
    tree->remove(CharOffset{ 3 }, Length{ 1 });

    print_buffer(tree);
    assume_buffer(tree, "A\nB\nD");

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test2()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    TreeBuilder builder = tree_builder_start(scratch.arena);
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
    printf("Line{%zu} range: first{%zu} last{%zu}\n", line, range.first, range.last);
    String8 buf = tree->get_line_content(scratch.arena, line);
    printf("content: %s\n", buf.str);
    printf("Line number: %zu\n", tree->line_at(range.first));

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
    assume_buffer(tree, "sdaaadff\n\ndsfasdf\n\naasdf\n");

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test3()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    TreeBuilder builder = tree_builder_start(scratch.arena);
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("Hello")));
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal(",")));
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal(" ")));
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("World")));
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("!")));
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("\nThis is a second line.")));
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal(" Continue...\nANOTHER!")));

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
    printf("--- Delete at off{%zu}, len{%zu} ---\n", off, len);
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
    printf("--- Delete at off{%zu}, len{%zu} ---\n", off, len);
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
        printf("Line{%zu} range: first{%zu} last{%zu}\n", line, range.first, range.last);
        buf = tree->get_line_content(scratch.arena, line);
        printf("content: %s\n", buf.str);
        printf("Line number: %zu\n", tree->line_at(range.first));
    }

    printf("out of range line:\n");
    auto range = tree->get_line_range(Line{ 99 });
    printf("Line{%zu} range: first{%zu} last{%zu}\n", size_t{99}, range.first, range.last);
    buf = tree->get_line_content(scratch.arena, Line{ 99 });
    printf("content: %s\n", buf.str);
    printf("Line number: %zu\n", tree->line_at(range.first));
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
    TreeBuilder builder = tree_builder_start(scratch.arena);
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("ABCD")));
    Tree* tree = tree_builder_finish(&builder);

    tree->insert(CharOffset{4}, str8_mut(str8_literal("a")));

    assume_buffer(tree, "ABCDa");

    tree->remove(CharOffset{3}, Length{2});

    assume_buffer(tree, "ABC");

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test5()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    TreeBuilder builder = tree_builder_start(scratch.arena);
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("")));
    auto tree = tree_builder_finish(&builder);

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("a")));

    assume_buffer(tree, "a");

    tree->remove(CharOffset{ 0 }, Length{ 1 });

    assume_buffer(tree, "");

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test6()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    TreeBuilder builder = tree_builder_start(scratch.arena);
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("Hello, World!")));
    Tree* tree = tree_builder_finish(&builder);

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("a")));
    tree->insert(CharOffset{ 1 }, str8_mut(str8_literal("b")));
    tree->insert(CharOffset{ 2 }, str8_mut(str8_literal("c")));

    assume_buffer(tree, "abcHello, World!");

    tree->remove(CharOffset{ 0 }, Length{ 3 });

    assume_buffer(tree, "Hello, World!");

    auto r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, "abcHello, World!");

    r = tree->try_redo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, "Hello, World!");

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, "abcHello, World!");

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, "Hello, World!");

    r = tree->try_undo(CharOffset{ 0 });
    assert(not r.success);

    r = tree->try_redo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, "abcHello, World!");

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, "Hello, World!");

    // Destroy the redo stack.

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("NEW")));

    assume_buffer(tree, "NEWHello, World!");

    r = tree->try_redo(CharOffset{ 0 });
    assert(not r.success);

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(tree, "Hello, World!");

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test7()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    TreeBuilder builder = tree_builder_start(scratch.arena);
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("ABC")));
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("DEF")));
    Tree* tree = tree_builder_finish(&builder);

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("foo")));

    assume_buffer(tree, "fooABCDEF");

    tree->remove(CharOffset{ 6 }, Length{ 3 });

    assume_buffer(tree, "fooABC");

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
    TreeBuilder builder = tree_builder_start(scratch.arena);
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("Hello, World!")));
    Tree* tree = tree_builder_finish(&builder);

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("a")), PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, "aHello, World!");

    auto r = tree->try_undo(CharOffset{ 0 });
    assert(not r.success);

    tree->remove(CharOffset{ 0 }, Length{ 1 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, "Hello, World!");

    r = tree->try_undo(CharOffset{ 0 });
    assert(not r.success);

    // Snap back to "Hello, World!"
    tree->commit_head(CharOffset{ 0 });
    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("a")), PieceTree::SuppressHistory::Yes);
    tree->insert(CharOffset{ 1 }, str8_mut(str8_literal("b")), PieceTree::SuppressHistory::Yes);
    tree->insert(CharOffset{ 2 }, str8_mut(str8_literal("c")), PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, "abcHello, World!");

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);
    assume_buffer(tree, "Hello, World!");

    // Snap back to "Hello, World!"
    tree->commit_head(CharOffset{ 0 });
    tree->remove(CharOffset{ 0 }, Length{ 7 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, "World!");

    tree->remove(CharOffset{ 5 }, Length{ 1 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, "World");

    r = tree->try_undo(CharOffset{ 0 });
    assert(r.success);
    assume_buffer(tree, "Hello, World!");

    r = tree->try_redo(CharOffset{ 0 });
    assume_buffer(tree, "World");

    release_tree(tree);
    Arena::scratch_end(scratch);
}

void test9()
{
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);
    TreeBuilder builder = tree_builder_start(scratch.arena);
    tree_builder_accept(scratch.arena, &builder, str8_mut(str8_literal("Hello, World!")));
    Tree* tree = tree_builder_finish(&builder);

    auto initial_commit = tree->head();

    tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("a")), PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, "aHello, World!");

    auto r = tree->try_undo(CharOffset{ 0 });
    assert(not r.success);

    auto commit = tree->head();
    tree->snap_to(initial_commit);
    assume_buffer(tree, "Hello, World!");

    tree->snap_to(commit);
    assume_buffer(tree, "aHello, World!");

    tree->remove(CharOffset{ 0 }, Length{ 8 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, "World!");

    tree->snap_to(commit);
    assume_buffer(tree, "aHello, World!");

    tree->snap_to(initial_commit);
    assume_buffer(tree, "Hello, World!");

    // Create a new branch.
    tree->insert(CharOffset{ 13 }, str8_mut(str8_literal(" My name is fredbuf.")), PieceTree::SuppressHistory::Yes);
    assume_buffer(tree, "Hello, World! My name is fredbuf.");

    auto branch = tree->head();

    // Revert back.
    tree->snap_to(commit);
    assume_buffer(tree, "aHello, World!");

    // Revert back to branch.
    tree->snap_to(branch);
    assume_buffer(tree, "Hello, World! My name is fredbuf.");

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

#define EachIndex(i,n) (uint64_t i=0;i<n;++i)

void time_buffer()
{
    Stopwatch sw;
    constexpr std::string_view initial_input =
R"(What is Lorem Ipsum?
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap
into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum
passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.)";
    constexpr std::string_view inserted_buf_expected =
R"(What is Lorem Ipsum?
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaacenturies, but also the leap
into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum
passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.)";
    constexpr std::string_view upper_half =
R"(enturies, but also the leap
into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum
passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.)";
    TreeBuilder builder;
    builder.accept(initial_input);
    Tree tree = builder.create();

    auto initial_commit = tree.head();

    constexpr int timing_count = 10;
    std::chrono::microseconds timing_data[timing_count];
    // Append-like insertions in the middle.
    {
        for EachIndex(i, timing_count)
        {
            tree.snap_to(initial_commit);
            auto mid = extend(CharOffset{}, (rep(tree.length()) / 2));
            std::string_view ins_txt = "a";
            sw.start();
            for (int j = 0; j < 100; ++j)
            {
                tree.insert(mid, ins_txt, SuppressHistory::Yes);
                mid = extend(mid);
            }
            sw.stop();
            timing_data[i] = sw.to_us();
            assume_buffer(&tree, inserted_buf_expected);
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
            tree.snap_to(initial_commit);
            auto mid = extend(CharOffset{}, (rep(tree.length()) / 2));
            std::string_view ins_txt = "a";
            sw.start();
            for (int j = 0; j < 100; ++j)
            {
                // Don't change 'mid' to append to the string.
                tree.insert(mid, ins_txt, SuppressHistory::Yes);
            }
            sw.stop();
            timing_data[i] = sw.to_us();
            assume_buffer(&tree, inserted_buf_expected);
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
            tree.snap_to(initial_commit);
            auto mid = extend(CharOffset{}, (rep(tree.length()) / 2));
            sw.start();
            while (mid != CharOffset::Sentinel)
            {
                // Don't change 'mid' to append to the string.
                tree.remove(mid, Length{ 1 }, SuppressHistory::Yes);
                mid = retract(mid);
            }
            sw.stop();
            timing_data[i] = sw.to_us();
            assume_buffer(&tree, upper_half);
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
            tree.snap_to(initial_commit);
            // This deletion needs to be inclusive of the midpoint (to be consistent with deletion above).
            auto len_to_del = Length{ (rep(tree.length()) / 2) + 1 };
            sw.start();
            while (len_to_del != Length{})
            {
                // Don't change 'mid' to append to the string.
                tree.remove(CharOffset{}, Length{ 1 }, SuppressHistory::Yes);
                len_to_del = retract(len_to_del);
            }
            sw.stop();
            timing_data[i] = sw.to_us();
            assume_buffer(&tree, upper_half);
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
#endif // TIMING_DATA

int main()
{
    // Setup the scratch arenas.
    Arena::Arena* scratch_arenas[2];
    // Generally, you only need two arenas to handle all conflicts.
    scratch_arenas[0] = Arena::alloc(Arena::default_params);
    scratch_arenas[1] = Arena::alloc(Arena::default_params);
    Arena::populate_scratch_arenas({ scratch_arenas, std::size(scratch_arenas) });

    test1();
    test2();
    test3();
    test4();
    test5();
    test6();
    test7();
    test8();
    test9();
}

#include "arena.cpp"
#include "fred-strings.cpp"
#include "fredbuf.cpp"
#include "os-cstd.cpp"
