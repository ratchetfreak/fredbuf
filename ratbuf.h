#pragma once

#include <forward_list>
#include <memory>
#include <string_view>
#include <string>
#include <vector>

#include "arena.h"
#include "fred-strings.h"
#include "ratbuf_btree.h"
#include "types.h"

#ifndef NDEBUG
#define TEXTBUF_DEBUG
#endif // NDEBUG

// This is a C++ implementation of the textbuf data structure described in
// https://code.visualstudio.com/blogs/2018/03/23/text-buffer-reimplementation. The differences are
// that this version is based on immutable data structures to achieve fast undo/redo.
namespace RatchetPieceTree
{
    using StorageTree = B_Tree<16>;
    
    struct UndoRedoEntry
    {
        UndoRedoEntry* next;
        StorageTree root;
        CharOffset op_offset;
    };

    struct UndoRedoList
    {
        UndoRedoEntry* first;
        UndoRedoEntry* last;
        uint64_t count;
    };

    // We need the ability to 'release' old entries in this stack.
    using UndoStack = UndoRedoList;
    using RedoStack = UndoRedoList;

    enum class LineStart : size_t { };

    struct LineStarts
    {
        LineStart* starts;
        uint64_t count;
    };

    struct NodePosition
    {
        // Piece Index
        const RatchetPieceTree::NodeData* node = nullptr;
        // Remainder in current piece.
        Length remainder = { };
        // Node start offset in document.
        CharOffset start_offset = { };
        // The line (relative to the document) where this node starts.
        Line line = { };
    };

    struct CharBuffer
    {
        String8 buffer;
        LineStarts line_starts;
    };
    
    struct ModBuffer
    {
        String8 buffer;
    };

    struct ImmutableBufferArray
    {
        const CharBuffer* buffers;
        uint64_t count;
        uint64_t* ref_count;
    };
    
    struct alignas(16) FreeList
    {
        BNodeCounted* head;
        uint64_t tag;
        
    };
    
    // Note: We add/remove from this list using atomic operations, which is why this is 16-byte aligned.
    struct  BNodeFreeList
    {
        FreeList internal;
        FreeList leaf;
    };
    
    struct BTreeBlock
    {
        BNodeFreeList free_list;
        Arena::Arena* alloc_arena;
    };
    
    struct BufferCollection
    {
        const CharBuffer* buffer_at(BufferIndex index) const;
        CharOffset buffer_offset(BufferIndex index, const BufferCursor& cursor) const;

        // The immutable buffer arena is reused for arbitrary node building.
        Arena::Arena* immutable_buf_arena;
        Arena::Arena* undo_redo_stack_arena;
        // The starts array and the buffer array need to be linearly growing arenas so
        // we can quickly index into them since most operations will be O(lg n) to get
        // to the buffer, we want an O(1) operation to offset into the actual data.
        Arena::Arena* mut_buf_starts_arena;
        Arena::Arena* mut_buf_arena;
        ImmutableBufferArray orig_buffers;
        CharBuffer mod_buffer;
        BTreeBlock* rb_tree_blk;
    };

    struct LineRange
    {
        CharOffset first;
        CharOffset last; // Does not include LF.
        bool operator==(const LineRange&) const = default;
    };

    struct UndoRedoResult
    {
        bool success;
        CharOffset op_offset;
    };

    // Owning snapshot owns its own buffer data (performs a lightweight copy) so
    // that even if the original tree is destroyed, the owning snapshot can still
    // reference the underlying text.
    class OwningSnapshot;

    // Reference snapshot owns no data and is only valid for as long as the original
    // tree buffers are valid.
    class ReferenceSnapshot;

    // When mutating the tree nodes are saved by default into the undo stack.  This
    // allows callers to suppress this behavior.
    enum class SuppressHistory : bool { No, Yes };

    struct BufferMeta
    {
        LFCount lf_count = { };
        Length total_content_length = { };
    };

    // Indicates whether or not line was missing a CR (e.g. only a '\n' was at the end).
    enum class IncompleteCRLF : bool { No, Yes };
    
    void dec_buffer_ref(BufferCollection* collection);
    BufferCollection take_buffer_ref(const BufferCollection* collection);

    class Tree
    {
    public:
        explicit Tree(BufferCollection buffers);

        // Interface.
        // Initialization after populating initial immutable buffers from ctor.
        void build_tree();

        // Manipulation.
        void insert(CharOffset offset, String8 txt, SuppressHistory suppress_history = SuppressHistory::No);
        void remove(CharOffset offset, Length count, SuppressHistory suppress_history = SuppressHistory::No);
        UndoRedoResult try_undo(CharOffset op_offset);
        UndoRedoResult try_redo(CharOffset op_offset);

        // Direct history manipulation.
        // This will commit the current node to the history.  The offset provided will be the undo point later.
        void commit_head(CharOffset offset);
        StorageTree head() const;
        // Snaps the tree back to the specified root.  This needs to be called with a root that is derived from
        // the set of buffers based on its creation.
        void snap_to(const StorageTree& new_root);

        // Queries.
        String8 get_line_content(Arena::Arena* arena, Line line) const;
        [[nodiscard]] IncompleteCRLF get_line_content_crlf(Arena::Arena* arena, String8* buf, Line line) const;
        char at(CharOffset offset) const;
        Line line_at(CharOffset offset) const;
        LineRange get_line_range(Line line) const;
        LineRange get_line_range_crlf(Line line) const;
        LineRange get_line_range_with_newline(Line line) const;

        Length length() const
        {
            return root.length();
        }

        bool is_empty() const
        {
            return meta.total_content_length == Length{};
        }

        LFCount line_feed_count() const
        {
            return root.lf_count();
        }

        Length line_count() const
        {
            return Length{ rep(line_feed_count()) + 1 };
        }

        OwningSnapshot* owning_snap(Arena::Arena* arena) const;
        ReferenceSnapshot ref_snap() const;
        uint64_t depth() const
        {
            return root.depth();
        }
        // Note, this will not increment refs.  That must be done by the caller.
        BufferCollection buffer_collection_no_ref() const;
    private:
        friend class TreeWalker;
        friend class ReverseTreeWalker;
        friend class OwningSnapshot;
        friend class ReferenceSnapshot;
#ifdef TEXTBUF_DEBUG
        friend void print_piece(const Piece& piece, const Tree* tree, int level);
        friend void print_tree(const Tree& tree);
#endif // TEXTBUF_DEBUG
        void internal_insert(CharOffset offset, String8 txt);
        void internal_remove(CharOffset offset, Length count);

        using Accumulator = Length(*)(const BufferCollection*, const Piece&, Line);

        template <Accumulator accumulate>
        static void line_start(CharOffset* offset, const BufferCollection* buffers, const StorageTree& node, Line line);
        static void line_end_crlf(CharOffset* offset, const BufferCollection* buffers, StorageTree::NodePtr node, Line line);
        static Length accumulate_value(const BufferCollection* buffers, const Piece& piece, Line index);
        static Length accumulate_value_no_lf(const BufferCollection* buffers, const Piece& piece, Line index);
        static void populate_from_node(Arena::Arena* arena, String8List* lst, const BufferCollection* buffers, const StorageTree& node);
        static void populate_from_node(Arena::Arena* arena, String8List* lst, const BufferCollection* buffers, const StorageTree& node, Line line_index);
        static LFCount line_feed_count(const BufferCollection* buffers, BufferIndex index, const BufferCursor& start, const BufferCursor& end);
        static NodePosition node_at(const BufferCollection* buffers, const StorageTree& node, CharOffset off);
        static BufferCursor buffer_position(const BufferCollection* buffers, const Piece& piece, Length remainder);
        static char char_at(const BufferCollection* buffers, const StorageTree& node, CharOffset offset);
        static Piece trim_piece_right(const BufferCollection* buffers, const Piece& piece, const BufferCursor& pos);
        static Piece trim_piece_left(const BufferCollection* buffers, const Piece& piece, const BufferCursor& pos);
        
        struct ShrinkResult
        {
            Piece left;
            Piece right;
        };

        static ShrinkResult shrink_piece(const BufferCollection* buffers, const Piece& piece, const BufferCursor& first, const BufferCursor& last);
        

        // Direct mutations.
        static String8 assemble_line(Arena::Arena* arena, const BufferCollection* buffers, const BufferMeta& meta, const StorageTree& node, Line line);
        
        Piece build_piece(String8 txt);
        void combine_pieces(NodePosition existing_piece, Piece new_piece);
        void remove_node_range(NodePosition first, Length length);
        void compute_buffer_meta();
        void append_undo(const StorageTree& old_root, CharOffset op_offset);

        BufferCollection buffers;
        //Buffers buffers;
        //CharBuffer mod_buffer;
        RatchetPieceTree::StorageTree root;
        //LineStarts scratch_starts;
        BufferCursor last_insert;
        // Note: This is absolute position.  Initialize to nonsense value.
        CharOffset end_last_insert = CharOffset::Sentinel;
        BufferMeta meta;
        UndoStack undo_stack;
        RedoStack redo_stack;
        UndoRedoEntry* free_undo_list{};
    };

    // Tree building.
    struct ImmutableBufferNode
    {
        ImmutableBufferNode* next;
        CharBuffer buffer;
    };

    struct ImmutableBufferList
    {
        ImmutableBufferNode* first;
        ImmutableBufferNode* last;
        uint64_t count;
    };

    struct TreeBuilder
    {
        Arena::Arena* immutable_buf_arena;
        Arena::Arena* undo_redo_stack_arena;
        Arena::Arena* mut_buf_starts_arena;
        Arena::Arena* mut_buf_arena;
        ImmutableBufferList buffers;
    };

    // Building/release.
    TreeBuilder tree_builder_start(Arena::Arena* buffer_arena);
    void tree_builder_accept(Arena::Arena* arena, TreeBuilder* builder, String8 txt);
    Tree* tree_builder_finish(TreeBuilder* builder);
    Tree* tree_builder_empty(Arena::Arena* buffer_arena);
    void release_tree(Tree* tree);

    class OwningSnapshot
    {
    public:
        explicit OwningSnapshot(Arena::Arena* mut_buf_arena, const Tree* tree);
        explicit OwningSnapshot(Arena::Arena* mut_buf_arena, const Tree* tree, const StorageTree& dt);
        OwningSnapshot() = delete;
        OwningSnapshot(const OwningSnapshot&) = delete;
        OwningSnapshot &operator =(const OwningSnapshot&) = delete;

        // Queries.
        String8 get_line_content(Arena::Arena* arena, Line line) const;
        [[nodiscard]] IncompleteCRLF get_line_content_crlf(Arena::Arena* arena, String8* buf, Line line) const;
        Line line_at(CharOffset offset) const;
        LineRange get_line_range(Line line) const;
        LineRange get_line_range_crlf(Line line) const;
        LineRange get_line_range_with_newline(Line line) const;
        bool is_empty() const
        {
            return meta.total_content_length == Length{};
        }

        Length line_count() const
        {
            return Length{ rep(meta.lf_count) + 1 };
        }
        Length length() const
        {
            return meta.total_content_length;
        }
        uint64_t depth()
        {
            return root.depth();
        }
        // Note, this will not increment refs.  That must be done by the caller.
        BufferCollection buffer_collection_no_ref() const;
    private:
        friend class TreeWalker;
        friend class ReverseTreeWalker;

        StorageTree root;
        BufferMeta meta;
        // This should be fairly lightweight.  The original buffers
        // will retain the majority of the memory consumption.
        BufferCollection buffers;
    };

    void release_owning_snap(OwningSnapshot* snap);

    class ReferenceSnapshot
    {
    public:
        explicit ReferenceSnapshot(const Tree* tree);
        explicit ReferenceSnapshot(const Tree* tree, const StorageTree& dt);
        ReferenceSnapshot(const ReferenceSnapshot&);
        ReferenceSnapshot& operator=(const ReferenceSnapshot&);
        ~ReferenceSnapshot();

        // Queries.
        String8 get_line_content(Arena::Arena* arena, Line line) const;
        [[nodiscard]] IncompleteCRLF get_line_content_crlf(Arena::Arena* arena, String8* buf, Line line) const;
        Line line_at(CharOffset offset) const;
        LineRange get_line_range(Line line) const;
        LineRange get_line_range_crlf(Line line) const;
        LineRange get_line_range_with_newline(Line line) const;
        bool is_empty() const
        {
            return meta.total_content_length == Length{};
        }

        Length line_count() const
        {
            return Length{ rep(meta.lf_count) + 1 };
        }
    private:
        friend class TreeWalker;
        friend class ReverseTreeWalker;

        StorageTree root;
        BufferMeta meta;
        // A reference to the underlying tree buffers.
        BufferCollection buffers;
    };

    class TreeWalker
    {
    public:
        TreeWalker(Arena::Arena* arena, const Tree* tree, CharOffset offset = CharOffset{ });
        TreeWalker(Arena::Arena* arena,const OwningSnapshot* snap, CharOffset offset = CharOffset{ });
        TreeWalker(Arena::Arena* arena,const ReferenceSnapshot* snap, CharOffset offset = CharOffset{ });
        TreeWalker(Arena::Arena* arena, const BufferCollection* buffers, const BufferMeta& meta, const StorageTree& root, CharOffset offset = CharOffset{ });
        TreeWalker(const TreeWalker&) = delete;

        char current() const;
        char next();
        void seek(CharOffset offset);
        bool exhausted() const;
        Length remaining() const;
        CharOffset offset() const
        {
            return total_offset;
        }

        // For Iterator-like behavior.
        TreeWalker& operator++()
        {
            next();
            return *this;
        }

        char operator*() const
        {
            return current();
        }
        
        bool operator==(const TreeWalker& other) const
        {
            return root.root_ptr() == other.root.root_ptr() && total_offset == other.total_offset;
        }

        struct StackEntry
        {
            RatchetPieceTree::StorageTree::NodePtr node;
            size_t index = 0;
        };
    private:
        void populate_ptrs();
        void fast_forward_to(CharOffset offset);

        const BufferCollection* buffers;
        StorageTree root;
        BufferMeta meta;
        StackEntry* stack;
        uint64_t stackCount;
        CharOffset total_offset = CharOffset{ 0 };
        //std::vector<StackEntry> stack;
        const char* first_ptr = nullptr;
        const char* last_ptr = nullptr;
    };

    class ReverseTreeWalker
    {
    public:
        ReverseTreeWalker(Arena::Arena* arena, const Tree* tree, CharOffset offset = CharOffset{ });
        ReverseTreeWalker(Arena::Arena* arena, const OwningSnapshot* snap, CharOffset offset = CharOffset{ });
        ReverseTreeWalker(Arena::Arena* arena, const ReferenceSnapshot* snap, CharOffset offset = CharOffset{ });
        ReverseTreeWalker(const TreeWalker&) = delete;

        char current() const;
        char next();
        void seek(CharOffset offset);
        bool exhausted() const;
        Length remaining() const;
        CharOffset offset() const
        {
            return total_offset;
        }

        // For Iterator-like behavior.
        ReverseTreeWalker& operator++()
        {
            next();
            return *this;
        }

        char operator*()
        {
            return current();
        }
        
        bool operator==(const ReverseTreeWalker& other) const
        {
            return root.root_ptr() == other.root.root_ptr() && total_offset == other.total_offset;
        }
        
        struct StackEntry
        {
            RatchetPieceTree::StorageTree::NodePtr node;
            size_t index = 0;
        };
        StackEntry* stack;
        uint64_t stackCount;
        const char* first_ptr = nullptr;
        const char* last_ptr = nullptr;
    private:
        void populate_ptrs();
        void fast_forward_to(CharOffset offset);

        enum class Direction { Left, Center, Right };


        const BufferCollection* buffers;
        StorageTree root;
        BufferMeta meta;
        CharOffset total_offset = CharOffset{ 0 };
    };

    enum class EmptySelection : bool { No, Yes };

    struct SelectionMeta
    {
        OwningSnapshot snap;
        Offset first;
        Offset last;
        EmptySelection empty;
    };
    
    

} // namespace RatchetPieceTree