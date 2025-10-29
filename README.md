# Project

This is the text buffer implementation described in [Text Editor Data Structures](https://cdacamar.github.io/data%20structures/algorithms/benchmarking/text%20editors/c++/editor-data-structures/?fbclid=IwAR1KPqHQU-torrzSq7LKWgK3uUZsTaoEpiAQeDT8XlvlOD3MCSt3sEl2YXc).

## API

Building:

```c++
using namespace PieceTree;
// Note: The tree builder will take ownership of this arena, so do not reuse this
// arena for anything else.
Arena::Arena* arena = Arena::alloc(Arena::default_params);
TreeBuilder builder = tree_builder_start(arena);
// Note: The arena used for the builder methods do _not_ need to be the same arena
// which is used in the initial start method.  But it does not hurt to reuse the
// same arena here.
tree_builder_accept(arena, &builder, str8_mut(str8_literal("ABC")));
tree_builder_accept(arena, &builder, str8_mut(str8_literal("DEF")));
Tree* tree = tree_builder_finish(&builder);
// Resulting total buffer: "ABCDEF"
```

Insertion:

```c++
tree->insert(CharOffset{ 0 }, str8_mut(str8_literal("foo")));
// Resulting total buffer: "fooABCDEF"
```

Deletion:

```c++
tree->remove(CharOffset{ 6 }, Length{ 3 });
// Resulting total buffer: "fooABC"
```

Line retrieval:

```c++
// Note: The arena here does not need to be the tree's arena.
auto scratch = Arena::scratch_begin(Arena::no_conflicts);
String8 str = tree.get_line_content(scratch.arena, Line{ 1 });
// 'str' contains "fooABC"
Arena::scratch_end(scratch);
```

Iteration:

```c++
auto scratch = Arena::scratch_begin(Arena::no_conflicts);
TreeWalker walker{ scratch.arena, tree };
while (not walker.exhausted())
{
    printf("%c", walker.next());
}
Arena::scratch_end(scratch);
```

Tree cleanup:

```c++
release_tree(tree);
// Note: There is also 'release_owning_snap' for terminating usage of an owning snapshot.
//       Reference snapshots are stack-based and use the destructor to remove its reference
//       to the underlying buffers.
```

## Contributing

Feel free to open up a PR or issue but there's no guarantee it will get merged.

## Building

If you're on Windows, open a developer command prompt and invoke `b.bat`.  Do the same for essentially any other compiler except change the flags to be specific to your compiler.  It's all standard C++ all the way down.

Optionally, you can have the build emit timing data by using `b timing` or `.\b.sh timing` on Linux.
