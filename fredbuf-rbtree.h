#pragma once

#include "macros.h"
#include "types.h"

// The concept for the RB tree is borrowed from
// https://bartoszmilewski.com/2013/11/25/functional-data-structures-in-c-trees/ (Bartosz Milewski)
// but some other pieces were assembled together from other sources, in particular deletion was
// gathered from https://github.com/dotnwat/persistent-rbtree after having implemented the deletion
// described by Bartosz, it was discovered that the algorithm was not always preserving the RB
// invariants.

// This enables the old node removal code by Bartosz.  Perhaps I'll revisit it at a later time.
//#define EXPERIMENTAL_REMOVE

namespace PieceTree
{
    
    
#ifdef COUNT_ALLOC
    extern size_t alloc_count;
    extern size_t dealloc_count;
#define NEW_NODE_ALLOC() alloc_count++
#define NEW_NODE_DEALLOC() dealloc_count++
#else
#define NEW_NODE_ALLOC() do{}while(0)
#define NEW_NODE_DEALLOC() do{}while(0)
#endif
    
    enum class BufferIndex : size_t
    {
        ModBuf = sentinel_for<BufferIndex>
    };

    enum class Line : size_t
    {
        IndexBeginning,
        Beginning
    };

    using Editor::CharOffset;
    using Editor::Length;
    using Editor::Column;

    enum class LFCount : size_t { };

    struct BufferCursor
    {
        // Relative line in the current buffer.
        Line line = { };
        // Column into the current line.
        Column column = { };

        bool operator==(const BufferCursor&) const = default;
    };

    struct Piece
    {
        BufferIndex index = { }; // Index into a buffer in PieceTree.  This could be an immutable buffer or the mutable buffer.
        BufferCursor first = { };
        BufferCursor last = { };
        Length length = { };
        LFCount newline_count = { };
    };

    using Offset = PieceTree::CharOffset;

    struct NodeData
    {
        PieceTree::Piece piece;

        PieceTree::Length left_subtree_length = { };
        PieceTree::LFCount left_subtree_lf_count = { };
    };

    class RedBlackTree;

    NodeData attribute(const NodeData& data, const RedBlackTree& left);

    enum class Color
    {
        Red,
        Black,
        DoubleBlack
    };

    inline const char* to_string(Color c)
    {
        switch (c)
        {
        case Color::Red:         return "Red";
        case Color::Black:       return "Black";
        case Color::DoubleBlack: return "DoubleBlack";
        }
        return "unknown";
    }

    struct RBNodeCounted;
    struct RBTreeBlock;

    struct RBNodeBlock
    {
        RBTreeBlock* base_blk;
        uint64_t ref_count;
    };

    struct RBNodePayload
    {
        const RBNodeCounted* left;
        NodeData data;
        const RBNodeCounted* right;
        Color color;
    };

    struct RBNodeCounted
    {
        union
        {
            RBNodePayload payload;
            RBNodeCounted* free_next;
        };
        RBNodeBlock* blk;
    };

    inline read_only RBNodeCounted null_node_inst = {
        .payload = {
            .left = &null_node_inst,
            .data = NodeData{},
            .right = &null_node_inst,
            .color = Color::Black,
        },
        .blk = nullptr,
    };

    // Counted node management.
    void dec_node_ref(const RBNodeCounted* node);
    const RBNodeCounted* take_node_ref(const RBNodeCounted* node);
    const RBNodeCounted* make_node(RBTreeBlock* blk, Color c, const RBNodeCounted* lft, const NodeData& data, const RBNodeCounted* rgt);

    class RedBlackTree
    {
    public:
        struct ColorTree;

        explicit RedBlackTree() = default;
        RedBlackTree(RedBlackTree&&);
        RedBlackTree& operator=(const RedBlackTree&) = delete;
        RedBlackTree& operator=(RedBlackTree&&);
        ~RedBlackTree();

        // Queries.
        const RBNodeCounted* root_ptr() const;
        bool is_empty() const;
        const NodeData& root() const;
        RedBlackTree left() const;
        RedBlackTree right() const;
        Color root_color() const;

        // Helpers.
        bool operator==(const RedBlackTree&) const = default;

        // Mutators.
        RedBlackTree insert(RBTreeBlock* blk, const NodeData& x, Offset at) const;
        RedBlackTree remove(RBTreeBlock* blk, Offset at) const;

        // Duplication.
        RedBlackTree dup() const;
    private:
        RedBlackTree(RBTreeBlock* blk,
                    Color c,
                    const RedBlackTree& lft,
                    const NodeData& val,
                    const RedBlackTree& rgt);

        RedBlackTree(const RBNodeCounted* node);

        // Removal.
#ifdef EXPERIMENTAL_REMOVE
        ColorTree rem(Offset at, Offset total) const;
        ColorTree remove_node() const;
        static ColorTree remove_double_black(Color c, ColorTree const &lft, const NodeData& x, ColorTree const &rgt);
#else
        static RedBlackTree fuse(RBTreeBlock* blk, const RedBlackTree& left, const RedBlackTree& right);
        static RedBlackTree balance(RBTreeBlock* blk, const RedBlackTree& node);
        static RedBlackTree balance_left(RBTreeBlock* blk, const RedBlackTree& left);
        static RedBlackTree balance_right(RBTreeBlock* blk, const RedBlackTree& right);
        static RedBlackTree remove_left(RBTreeBlock* blk, const RedBlackTree& root, Offset at, Offset total);
        static RedBlackTree remove_right(RBTreeBlock* blk, const RedBlackTree& root, Offset at, Offset total);
        static RedBlackTree rem(RBTreeBlock* blk, const RedBlackTree& root, Offset at, Offset total);
#endif // EXPERIMENTAL_REMOVE

        // Insertion.
        RedBlackTree ins(RBTreeBlock* blk, const NodeData& x, Offset at, Offset total_offset) const;
        static RedBlackTree balance(RBTreeBlock* blk, Color c, const RedBlackTree& lft, const NodeData& x, const RedBlackTree& rgt);
        bool doubled_left() const;
        bool doubled_right() const;

        // General.
        RedBlackTree paint(RBTreeBlock* blk, Color c) const;

        const RBNodeCounted* root_node = &null_node_inst;
    };

    // Global queries.
    PieceTree::Length tree_length(const RedBlackTree& root);
    PieceTree::LFCount tree_lf_count(const RedBlackTree& root);
} // namespace PieceTree