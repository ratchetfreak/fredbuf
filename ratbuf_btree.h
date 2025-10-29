#pragma once

#include <memory>
#include <variant>
#include <vector>
#include <array>
#include <string>
#include <forward_list>


#include "macros.h"
#include "types.h"


#ifndef NDEBUG
//#define LOG_ALGORITHM
#endif // NDEBUG
namespace RatchetPieceTree
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
    struct NodeData;
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
        BufferIndex index = { }; // Index into a buffer in RatchetPieceTree.  This could be an immutable buffer or the mutable buffer.
        BufferCursor first = { };
        BufferCursor last = { };
        Length length = { };
        LFCount newline_count = { };
    };

    using Offset = RatchetPieceTree::CharOffset;

    struct NodeData
    {
        RatchetPieceTree::Piece piece;

    };

    struct BNodeCounted;
    
    struct BTreeBlock;

    
    struct BNodeBlock
    {
        BTreeBlock* base_blk;
        uint64_t ref_count;
    };
    
    enum NodeType{
        INTERNAL, LEAF
    };

    struct BNodeCounted{
        BNodeBlock* blk;
        BNodeCounted* next;
        NodeType type;
    };

    template <size_t MaxChildren>
    struct BNodeCountedGeneric
    {
        BNodeBlock* blk;
        BNodeCountedGeneric<MaxChildren>* next;
        NodeType type;
        std::array<Length, MaxChildren> offsets;
        std::array<LFCount, MaxChildren> lineFeeds;
        size_t childCount;
        
        Length subTreeLength() const
        {
            return offsets[childCount-1];
        }
        
        LFCount subTreeLineFeeds() const
        {
            return lineFeeds[childCount-1];
        }
        bool isLeaf ()const{
            return type == NodeType::LEAF;
        }
    };

    template <size_t MaxChildren>
    struct BNodeCountedInternal
    {
        BNodeBlock* blk;
        BNodeCountedInternal<MaxChildren>* next;
        NodeType type;
        std::array<Length, MaxChildren> offsets;
        std::array<LFCount, MaxChildren> lineFeeds;
        size_t childCount;
        
        BNodeCountedGeneric<MaxChildren> *children[MaxChildren];
        
        
        Length subTreeLength() const
        {
            return offsets[childCount-1];
        }
        
        LFCount subTreeLineFeeds() const
        {
            return lineFeeds[childCount-1];
        }
        
    };
    
    template <size_t MaxChildren>
    struct BNodeCountedLeaf
    {
        BNodeBlock* blk;
        BNodeCountedLeaf<MaxChildren>* next;
        NodeType type;
        std::array<Length, MaxChildren> offsets;
        std::array<LFCount, MaxChildren> lineFeeds;
        size_t childCount;
        
        std::array<NodeData, MaxChildren> children;
        
        Length subTreeLength() const
        {
            return offsets[childCount-1];
        }
        
        LFCount subTreeLineFeeds() const
        {
            return lineFeeds[childCount-1];
        }
    };

    BNodeCounted* nil_node()
    {
        return nullptr;
    }
    
    bool nil_node(BNodeCounted* node)
    {
        return node == nullptr;
    }

    // Counted node management.
    void dec_node_ref(const BNodeCounted* node);
    template<size_t MaxChildren>
    const BNodeCounted* take_node_ref(const BNodeCountedGeneric<MaxChildren>* node);
    
    const BNodeCounted* take_node_ref(const BNodeCounted* node);
    //const BNodeCounted* make_node(BTreeBlock* blk, Color c, const BNodeCounted* lft, const NodeData& data, const BNodeCounted* rgt);

    struct BufferCollection;

    template <size_t MaxChildren>
    class B_Tree
    {


    public:
        using NodePtr = BNodeCountedGeneric<MaxChildren>*;
        using InternalNodePtr = BNodeCountedInternal<MaxChildren>*;
        using LeafNodePtr = BNodeCountedLeaf<MaxChildren>*;
        
        using NodeVector = BNodeCountedGeneric<MaxChildren>**;
        using ChildVector =BNodeCountedInternal<MaxChildren>**;
        using LeafVector  =BNodeCountedLeaf<MaxChildren>**;
    #if 0
        struct Node;
        
        
        using NodePtr = std::shared_ptr<const Node>;
        using ChildArray =std::array<NodePtr, MaxChildren>;
        using LeafArray =std::array<NodeData, MaxChildren>;
        struct Node :public std::enable_shared_from_this<Node>
        {
            Node(std::array<NodePtr, MaxChildren> chld,
                std::array<Length, MaxChildren> offsets,
                std::array<LFCount, MaxChildren> lineFeeds,
                size_t childCount):
                    children(std::move(chld)),
                    offsets(std::move(offsets)),
                    lineFeeds(lineFeeds),
                    childCount(childCount)
            {
                NEW_NODE_ALLOC();
                if(childCount < MaxChildren)
                {
                    this->offsets.back() = offsets[childCount-1];
                    this->lineFeeds.back() = lineFeeds[childCount-1];
                }
            }
            Node(std::array<NodeData, MaxChildren> chld,
                std::array<Length, MaxChildren> offsets,
                std::array<LFCount, MaxChildren> lineFeeds,
                size_t childCount):
                    children(std::move(chld)),
                    offsets(std::move(offsets)),
                    lineFeeds(lineFeeds),
                    childCount(childCount)
            {
                NEW_NODE_ALLOC();
                if(childCount < MaxChildren)
                {
                    this->offsets.back() = offsets[childCount-1];
                    this->lineFeeds.back() = lineFeeds[childCount-1];
                }
            }
            std::variant<ChildArray, LeafArray > children;
            std::array<Length, MaxChildren> offsets;
            std::array<LFCount, MaxChildren> lineFeeds;
            size_t childCount;

#ifdef COUNT_ALLOC
            ~Node()
            {
                NEW_NODE_DEALLOC();
            }
#endif

            bool isLeaf ()const{
                return children.index() != 0;
            }

            Length subTreeLength() const
            {
                return offsets.back();
            }
            
            LFCount subTreeLineFeeds() const
            {
                return lineFeeds.back();
            }
        };
        #endif
        explicit B_Tree() = default;

        NodePtr root_ptr() const;
        bool is_empty() const;
        Length length() const
        {
            return root_node?root_node->subTreeLength():Length{0};
        }
        LFCount lf_count() const
        {
            return root_node?root_node->subTreeLineFeeds():LFCount{0};
        }

        // Helpers.
        bool operator==(const B_Tree&) const = default;

        // Mutators.
        
        static B_Tree construct_from(BTreeBlock* blk, NodeData* leafNodes, size_t leafCount);
        B_Tree insert(BufferCollection* blk, const NodeData& x, Offset at) const;
        B_Tree remove(BufferCollection* blk, Offset at, Length len) const;

        // Duplication.
        B_Tree dup() const;
        uint64_t depth() const
        {
            return tree_depth;
        }
    private:
        B_Tree(NodePtr root, uint32_t depth);

        struct TreeManipResult{
            NodeVector nodes;
            size_t count;
            uint32_t depth;
        };
        TreeManipResult insertInto(Arena::Arena *arena, BufferCollection* blk, const NodePtr node, const NodeData& x, Length at) const;
        TreeManipResult insertInto_leaf(Arena::Arena *arena, BufferCollection* blk, LeafNodePtr node, const NodeData& x, Length at) const;
        
        TreeManipResult remove_from(Arena::Arena *arena, BufferCollection* blk, NodePtr a, NodePtr b, NodePtr c, Length at, Length len) const;
        TreeManipResult remove_from_leafs(Arena::Arena *arena, BufferCollection* blk, LeafNodePtr a, LeafNodePtr b, LeafNodePtr c, Length at, Length len) const;
        
        static NodePtr construct_leaf(BTreeBlock* blk, const NodeData* data, size_t begin, size_t end) ;
        static NodePtr construct_internal(BTreeBlock* blk, NodeVector data, size_t begin, size_t end);

        NodePtr root_node;
        uint32_t tree_depth=0;

    };

    template<size_t C>
    class B_TreeWalker
    {
    public:
        B_TreeWalker(const B_Tree<C>* tree, CharOffset offset = CharOffset{ });
        B_TreeWalker(const B_TreeWalker&) = delete;

        char current();
        char next();
        void seek(CharOffset offset);
        bool exhausted() const;
        Length remaining() const;
        CharOffset offset() const;

        // For Iterator-like behavior.
        B_TreeWalker & operator++()
        {
            next();
            return *this;
        }

        char operator*()
        {
            return current();
        }
        struct StackEntry
        {
            B_Tree<C>::NodePtr node;
            size_t index;
            Length offset;
        };
        std::vector<StackEntry> stack;
    };
#ifdef LOG_ALGORITHM
    enum class MarkReason : size_t { None, Traverse, Collect, Made, Skip };
    struct algo_marker
    {
        B_Tree<10>::NodePtr node;
        MarkReason reason;
    };
    extern std::vector<algo_marker> algorithm;

#define algo_mark(node, reason) algorithm.push_back({node, MarkReason::reason})
#else
#define algo_mark(node, reason) do{}while(0)
#endif
    
};
