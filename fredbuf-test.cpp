#include <cassert>

//#include <format>
//#include <source_location>

#define COUNT_ALLOC

#if 1
#include "ratbuf.h"
#include "ratbuf_btree.cpp"
#define PieceTree RatchetPieceTree 
#else
#include "fredbuf.cpp"
#endif


void assume_buffer_snapshots(const PieceTree::Tree* tree, std::string_view expected, PieceTree::CharOffset offset)
{
    // Owning snapshot.
    {
        auto owning_snap = tree->owning_snap();
        PieceTree::TreeWalker walker{ &owning_snap, offset };
        std::string buf;
        while (not walker.exhausted())
        {
            buf.push_back(walker.next());
        }
        assert(walker.remaining() == PieceTree::Length{ 0 });

        if (expected != buf)
        {
            size_t slen = snprintf(NULL, 0, "owning snapshot buffer string '%s' did not match expected value of '%.*s'. Line()", buf.c_str(),(int) expected.size(), expected.data());
            std::string s ;
            s.resize(slen);
            slen = snprintf(s.data(), s.size(), "owning snapshot buffer string '%s' did not match expected value of '%.*s'. Line()", buf.c_str(),(int) expected.size(), expected.data());

            //auto s = std::format("owning snapshot buffer string '{}' did not match expected value of '{}'. Line({})", buf, expected, locus.line());
            fprintf(stderr, "%.*s\n", (int)s.size(), s.c_str());
            assert(false);
        }
    }
    // Reference snapshot.
    {
        auto owning_snap = tree->ref_snap();
        PieceTree::TreeWalker walker{ &owning_snap, offset };
        std::string buf;
        while (not walker.exhausted())
        {
            buf.push_back(walker.next());
        }
        assert(walker.remaining() == PieceTree::Length{ 0 });

        if (expected != buf)
        {
            //auto s = std::format("reference snapshot buffer string '{}' did not match expected value of '{}'. Line({})", buf, expected, locus.line());
            size_t slen = snprintf(NULL, 0, "reference snapshot buffer string '%s' did not match expected value of '%.*s'. Line()", buf.c_str(),(int) expected.size(), expected.data());
            std::string s ;
            s.resize(slen);
            slen = snprintf(s.data(), s.size(), "reference snapshot buffer string '%s' did not match expected value of '%.*s'. Line()", buf.c_str(),(int) expected.size(), expected.data());

            fprintf(stderr, "%s\n", s.c_str());
            assert(false);
        }
    }
}

void assume_reverse_buffer(const PieceTree::Tree* tree, std::string_view forward_buf, PieceTree::CharOffset offset)
{
    PieceTree::ReverseTreeWalker walker{ tree, offset };
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
        size_t slen = snprintf(NULL, 0, "Reversed buffer '%s' is not equal to forward buffer '%.*s'.  Line({})", buf.c_str(), (int)forward_buf.size(), forward_buf.data() );
         std::string s ;
            s.resize(slen);
            slen = snprintf(s.data(), s.size(),"Reversed buffer '%s' is not equal to forward buffer '%.*s'.  Line({})", buf.c_str(), (int)forward_buf.size(), forward_buf.data());
        fprintf(stderr, "%s\n", s.c_str());
        assert(false);
    }
}

void assume_buffer(const PieceTree::Tree* tree, std::string_view expected)
{
    constexpr auto start = PieceTree::CharOffset{ 0 };
    PieceTree::TreeWalker walker{ tree, start };
    std::string buf;
    while (not walker.exhausted())
    {
        buf.push_back(walker.next());
    }
    assert(walker.remaining() == PieceTree::Length{ 0 });

    if (expected != buf)
    {
        size_t slen = snprintf(NULL, 0,"buffer string '%s' did not match expected value of '%.*s'. Line({})", buf.c_str(), (int) expected.size(), expected.data());

         std::string s ;
        s.resize(slen);
         slen = snprintf(s.data(), s.size(),"buffer string '%s' did not match expected value of '%.*s'. Line({})", buf.c_str(), (int) expected.size(), expected.data());
        fprintf(stderr, "%s\n", s.c_str());
        assert(false);
    }
    assume_buffer_snapshots(tree, expected, start);
    assume_reverse_buffer(tree, buf, start + retract(tree->length()));
}

using namespace PieceTree;
void test1()
{
    TreeBuilder builder;
    std::string buf;
    builder.accept("A\nB\nC\nD");
    auto tree = builder.create();
    assume_buffer(&tree, "A\nB\nC\nD");

    tree.remove(CharOffset{ 4 }, Length{ 1 });
    tree.remove(CharOffset{ 3 }, Length{ 1 });

    print_buffer(&tree);
    assume_buffer(&tree, "A\nB\nD");
}

void test2()
{
    TreeBuilder builder;
    std::string buf;
    auto tree = builder.create();
    tree.insert(CharOffset{ 0 } + tree.length(), "a");
    tree.insert(CharOffset{ 0 } + tree.length(), "s");
    tree.insert(CharOffset{ 0 } + tree.length(), "d");
    tree.insert(CharOffset{ 0 } + tree.length(), "f");
    tree.insert(CharOffset{ 0 } + tree.length(), "\n");
    tree.insert(CharOffset{ 0 } + tree.length(), "a");
    tree.insert(CharOffset{ 0 } + tree.length(), "s");
    tree.insert(CharOffset{ 0 } + tree.length(), "d");
    tree.insert(CharOffset{ 0 } + tree.length(), "f");
    tree.insert(CharOffset{ 0 } + tree.length(), "\n");
    tree.insert(CharOffset{ 0 } + tree.length(), "a");
    tree.insert(CharOffset{ 0 } + tree.length(), "s");
    tree.insert(CharOffset{ 0 } + tree.length(), "d");
    tree.insert(CharOffset{ 0 } + tree.length(), "f");
    tree.insert(CharOffset{ 0 } + tree.length(), "\n");
    tree.insert(CharOffset{ 0 } + tree.length(), "a");
    tree.insert(CharOffset{ 0 } + tree.length(), "s");
    tree.insert(CharOffset{ 0 } + tree.length(), "d");
    tree.insert(CharOffset{ 0 } + tree.length(), "f");
    tree.insert(CharOffset{ 0 } + tree.length(), "\n");
#if 1
    tree.insert(CharOffset{ 1 }, "a");
    tree.insert(CharOffset{ 2 }, "s");
    tree.insert(CharOffset{ 3 }, "d");
    tree.insert(CharOffset{ 4 }, "f");
    tree.insert(CharOffset{ 5 }, "\n");
    tree.insert(CharOffset{ 6 }, "a");
    tree.insert(CharOffset{ 12 }, "s");
    tree.insert(CharOffset{ 15 }, "d");
    tree.insert(CharOffset{ 17 }, "f");
    tree.insert(CharOffset{ 18 }, "\n");
    tree.insert(CharOffset{ 2 }, "a");
    tree.insert(CharOffset{ 21 }, "s");
    tree.insert(CharOffset{ 21 }, "d");
    tree.insert(CharOffset{ 23 }, "f");
    tree.insert(CharOffset{ 29 }, "\n");
    tree.insert(CharOffset{ 30 }, "a");
    tree.insert(CharOffset{ 0 }, "s");
    tree.insert(CharOffset{ 1 }, "d");
    tree.insert(CharOffset{ 10 }, "f");
    tree.insert(CharOffset{ 11 }, "\n");
#endif
    auto line = Line{ 1 };
    auto range = tree.get_line_range(line);
    printf("Line{%zu} range: first{%zu} last{%zu}\n", line, range.first, range.last);
    tree.get_line_content(&buf, line);
    printf("content: %s\n", buf.c_str());
    printf("Line number: %zu\n", tree.line_at(range.first));

    print_buffer(&tree);

    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });
    tree.remove(CharOffset{ 5 }, Length{ 1 });

    print_buffer(&tree);
    assume_buffer(&tree, "sdaaadff\n\ndsfasdf\n\naasdf\n");
}

void test3()
{
    TreeBuilder builder;
    builder.accept("Hello");
    builder.accept(",");
    builder.accept(" ");
    builder.accept("World");
    builder.accept("!");
    builder.accept("\nThis is a second line.");
    builder.accept(" Continue...\nANOTHER!");

    auto tree = builder.create();

    print_tree(tree);

    std::string buf;

    printf("line content at 1:\n");
    tree.get_line_content(&buf, Line{ 1 });
    printf("%.*s\n", int(buf.size()), buf.c_str());

    printf("line content at 2:\n");
    tree.get_line_content(&buf, Line{ 2 });
    printf("%.*s\n", int(buf.size()), buf.c_str());

    printf("line content at 3:\n");
    tree.get_line_content(&buf, Line{ 3 });
    printf("%.*s\n", int(buf.size()), buf.c_str());

    tree.insert(CharOffset{ 37 }, "Hello");

    printf("line content at 1:\n");
    tree.get_line_content(&buf, Line{ 1 });
    printf("%.*s\n", int(buf.size()), buf.c_str());

    printf("line content at 2:\n");
    tree.get_line_content(&buf, Line{ 2 });
    printf("%.*s\n", int(buf.size()), buf.c_str());

    print_buffer(&tree);

    auto off = CharOffset{ 13 };
    auto len = Length{ 5 };
    printf("--- Delete at off{%zu}, len{%zu} ---\n", off, len);
    tree.remove(off, len);

#if 0
    printf("line content at 1:\n");
    tree.get_line_content(&buf, Line{ 1 });
    printf("%.*s\n", int(buf.size()), buf.c_str());

    printf("line content at 2:\n");
    tree.get_line_content(&buf, Line{ 2 });
    printf("%.*s\n", int(buf.size()), buf.c_str());
#endif

    print_buffer(&tree);

    off = CharOffset{ 37 };
    len = Length{ 5 };
    printf("--- Delete at off{%zu}, len{%zu} ---\n", off, len);
    tree.remove(off, len);

#if 0
    printf("line content at 1:\n");
    tree.get_line_content(&buf, Line{ 1 });
    printf("%.*s\n", int(buf.size()), buf.c_str());

    printf("line content at 2:\n");
    tree.get_line_content(&buf, Line{ 2 });
    printf("%.*s\n", int(buf.size()), buf.c_str());
#endif

    print_buffer(&tree);

    tree.insert(CharOffset{ 0 } + tree.length(), "a");
    print_buffer(&tree);

    tree.insert(CharOffset{ 0 } + tree.length(), "a");
    print_buffer(&tree);

    tree.insert(CharOffset{ 0 } + tree.length(), "a");
    print_buffer(&tree);

    tree.insert(CharOffset{ 0 } + tree.length(), "a");
    print_buffer(&tree);

    tree.insert(CharOffset{ 0 } + tree.length(), "END!!");
    print_buffer(&tree);

    tree.remove(CharOffset{ 52 }, Length{ 4 });
    print_buffer(&tree);

    //print_buffer(&tree, CharOffset{ 26 });

    tree.insert(CharOffset{} + tree.length(), "\nfoobar\nnext\nnextnext\nnextnextnext");
    tree.insert(CharOffset{} + tree.length(), "\nfoobar2\nnext\nnextnext\nnextnextnext");

    print_buffer(&tree);

    auto total_lines = Line{ rep(tree.line_feed_count()) + 1 };
    for (Line line = Line{ 1 }; line <= total_lines;line = extend(line))
    {
        auto range = tree.get_line_range(line);
        printf("Line{%zu} range: first{%zu} last{%zu}\n", line, range.first, range.last);
        tree.get_line_content(&buf, line);
        printf("content: %s\n", buf.c_str());
        printf("Line number: %zu\n", tree.line_at(range.first));
    }

    printf("out of range line:\n");
    auto range = tree.get_line_range(Line{ 99 });
    printf("Line{%zu} range: first{%zu} last{%zu}\n", size_t{99}, range.first, range.last);
    tree.get_line_content(&buf, Line{ 99 });
    printf("content: %s\n", buf.c_str());
    printf("Line number: %zu\n", tree.line_at(range.first));
#if 0
    tree.remove(PieceTree::CharOffset{ 37 }, PieceTree::Length{ 5 });

    printf("line content at 1:\n");
    tree.get_line_content(&buf, PieceTree::Line{ 1 });
    printf("%.*s\n", int(buf.size()), buf.c_str());

    printf("line content at 2:\n");
    tree.get_line_content(&buf, PieceTree::Line{ 2 });
    printf("%.*s\n", int(buf.size()), buf.c_str());

    tree.remove(PieceTree::CharOffset{ 25 }, PieceTree::Length{ 5 });

    printf("line content at 1:\n");
    tree.get_line_content(&buf, PieceTree::Line{ 1 });
    printf("%.*s\n", int(buf.size()), buf.c_str());

    printf("line content at 2:\n");
    tree.get_line_content(&buf, PieceTree::Line{ 2 });
    printf("%.*s\n", int(buf.size()), buf.c_str());
#endif
}

void test4()
{
    TreeBuilder builder;
    std::string buf;
    builder.accept("ABCD");
    auto tree = builder.create();

    tree.insert(CharOffset{4}, "a");

    assume_buffer(&tree, "ABCDa");

    tree.remove(CharOffset{3}, Length{2});

    assume_buffer(&tree, "ABC");
}

void test5()
{
    TreeBuilder builder;
    std::string buf;
    builder.accept("");
    auto tree = builder.create();

    tree.insert(CharOffset{ 0 }, "a");

    assume_buffer(&tree, "a");

    tree.remove(CharOffset{ 0 }, Length{ 1 });

    assume_buffer(&tree, "");
}

void test6()
{
    TreeBuilder builder;
    std::string buf;
    builder.accept("Hello, World!");
    auto tree = builder.create();

    tree.insert(CharOffset{ 0 }, "a");
    tree.insert(CharOffset{ 1 }, "b");
    tree.insert(CharOffset{ 2 }, "c");

    assume_buffer(&tree, "abcHello, World!");

    tree.remove(CharOffset{ 0 }, Length{ 3 });

    assume_buffer(&tree, "Hello, World!");

    auto r = tree.try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(&tree, "abcHello, World!");

    r = tree.try_redo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(&tree, "Hello, World!");

    r = tree.try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(&tree, "abcHello, World!");

    r = tree.try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(&tree, "Hello, World!");

    r = tree.try_undo(CharOffset{ 0 });
    assert(not r.success);

    r = tree.try_redo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(&tree, "abcHello, World!");

    r = tree.try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(&tree, "Hello, World!");

    // Destroy the redo stack.

    tree.insert(CharOffset{ 0 }, "NEW");

    assume_buffer(&tree, "NEWHello, World!");

    r = tree.try_redo(CharOffset{ 0 });
    assert(not r.success);

    r = tree.try_undo(CharOffset{ 0 });
    assert(r.success);

    assume_buffer(&tree, "Hello, World!");
}

void test7()
{
    TreeBuilder builder;
    std::string buf;
    builder.accept("ABC");
    builder.accept("DEF");
    auto tree = builder.create();

    tree.insert(CharOffset{ 0 }, "foo");

    assume_buffer(&tree, "fooABCDEF");

    tree.remove(CharOffset{ 6 }, Length{ 3 });

    assume_buffer(&tree, "fooABC");

    tree.get_line_content(&buf, Line{ 1 });

    assert(buf == "fooABC");

    for (char c : buf)
    {
        printf("%c", c);
    }
    printf("\n");
}

void test8()
{
    TreeBuilder builder;
    std::string buf;
    builder.accept("Hello, World!");
    auto tree = builder.create();

    tree.insert(CharOffset{ 0 }, "a", PieceTree::SuppressHistory::Yes);
    assume_buffer(&tree, "aHello, World!");

    auto r = tree.try_undo(CharOffset{ 0 });
    assert(not r.success);

    tree.remove(CharOffset{ 0 }, Length{ 1 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(&tree, "Hello, World!");

    r = tree.try_undo(CharOffset{ 0 });
    assert(not r.success);

    // Snap back to "Hello, World!"
    tree.commit_head(CharOffset{ 0 });
    tree.insert(CharOffset{ 0 }, "a", PieceTree::SuppressHistory::Yes);
    tree.insert(CharOffset{ 1 }, "b", PieceTree::SuppressHistory::Yes);
    tree.insert(CharOffset{ 2 }, "c", PieceTree::SuppressHistory::Yes);
    assume_buffer(&tree, "abcHello, World!");

    r = tree.try_undo(CharOffset{ 0 });
    assert(r.success);
    assume_buffer(&tree, "Hello, World!");

    // Snap back to "Hello, World!"
    tree.commit_head(CharOffset{ 0 });
    tree.remove(CharOffset{ 0 }, Length{ 7 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(&tree, "World!");

    tree.remove(CharOffset{ 5 }, Length{ 1 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(&tree, "World");

    r = tree.try_undo(CharOffset{ 0 });
    assert(r.success);
    assume_buffer(&tree, "Hello, World!");

    r = tree.try_redo(CharOffset{ 0 });
    assume_buffer(&tree, "World");
}

void test9()
{
    TreeBuilder builder;
    std::string buf;
    builder.accept("Hello, World!");
    auto tree = builder.create();

    auto initial_commit = tree.head();

    tree.insert(CharOffset{ 0 }, "a", PieceTree::SuppressHistory::Yes);
    assume_buffer(&tree, "aHello, World!");

    auto r = tree.try_undo(CharOffset{ 0 });
    assert(not r.success);

    auto commit = tree.head();
    tree.snap_to(initial_commit);
    assume_buffer(&tree, "Hello, World!");

    tree.snap_to(commit);
    assume_buffer(&tree, "aHello, World!");

    tree.remove(CharOffset{ 0 }, Length{ 8 }, PieceTree::SuppressHistory::Yes);
    assume_buffer(&tree, "World!");

    tree.snap_to(commit);
    assume_buffer(&tree, "aHello, World!");

    tree.snap_to(initial_commit);
    assume_buffer(&tree, "Hello, World!");

    // Create a new branch.
    tree.insert(CharOffset{ 13 }, " My name is fredbuf.", PieceTree::SuppressHistory::Yes);
    assume_buffer(&tree, "Hello, World! My name is fredbuf.");

    auto branch = tree.head();

    // Revert back.
    tree.snap_to(commit);
    assume_buffer(&tree, "aHello, World!");

    // Revert back to branch.
    tree.snap_to(branch);
    assume_buffer(&tree, "Hello, World! My name is fredbuf.");
}

void test10()
{

    TreeBuilder builder;
    std::string buf;
    builder.accept("He");
    builder.accept("llo, Worl");
    builder.accept("d!");
    auto tree = builder.create();
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
}


void test11()
{
        
    TreeBuilder builder;
    std::string buf;
    for(int i = 0; i< 1000; i++)
        builder.accept("Hello, World!");
    auto tree = builder.create();
    
    tree.remove(CharOffset{ 14 }, retract(tree.length(),  14*2 ));
    assume_buffer(&tree, "Hello, World!H!Hello, World!");

}

int main()
{
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
}