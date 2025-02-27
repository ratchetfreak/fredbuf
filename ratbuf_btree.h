#pragma once

#include <memory>
#include <variant>
#include <vector>
#include <array>
#include <string>
#include <forward_list>


#include "types.h"


#ifndef NDEBUG
#define LOG_ALGORITHM
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

    struct BufferCollection;
    using Editor::CharOffset;
    using Editor::Length;
    using Editor::Column;
    struct NodeData;
    enum class LFCount : size_t { };
        enum class BufferIndex : size_t
    {
        ModBuf = sentinel_for<BufferIndex>
    };

        enum class Line : size_t
    {
        IndexBeginning,
        Beginning
    };
    template <size_t MaxChildren>
    class B_Tree ;

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

    template <size_t MaxChildren>
    class B_Tree
    {


    public:
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
        explicit B_Tree() = default;

        const Node* root_ptr() const;
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
        B_Tree insert(const NodeData& x, Offset at, BufferCollection* buffers) const;
        B_Tree construct_from(std::vector<NodeData> leafNodes) const;
        B_Tree remove(Offset at, Length len, BufferCollection* buffers) const;

    private:
        B_Tree(NodePtr root);

        using ChildVector =std::vector<NodePtr>;
        using LeafVector  =std::vector<NodeData>;
        struct TreeManipResult{
            ChildVector nodes;
            
        };
        TreeManipResult insertInto(const Node* node, const NodeData& x, Length at, BufferCollection* buffers) const;
        TreeManipResult insertInto_leaf(const Node* node, const NodeData& x, Length at, BufferCollection* buffers) const;
        
        ChildVector remove_from(const Node* a, const Node* b, const Node* c, Length at, Length len, BufferCollection* buffers) const;
        ChildVector remove_from_leafs(const Node* a, const Node* b, const Node* c, Length at, Length len, BufferCollection* buffers) const;
        
        NodePtr construct_leaf(const LeafVector &data, size_t begin, size_t end) const ;
        NodePtr construct_internal(const ChildVector &data, size_t begin, size_t end) const ;

        NodePtr root_node;


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
