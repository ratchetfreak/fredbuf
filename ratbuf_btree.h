#pragma once

#include <memory>
#include <variant>
#include <vector>
#include <array>
#include <string>
#include <forward_list>


#include "types.h"


namespace PieceTree
{

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
                size_t childCount, LFCount subTreeLineFeeds):
                    children(std::move(chld)),
                    offsets(std::move(offsets)),
                    childCount(childCount),
                    subTreeLineFeeds(subTreeLineFeeds)
            {
                if(childCount < MaxChildren)
                {
                    this->offsets.back() = offsets[childCount-1];
                }
            }
            Node(std::array<NodeData, MaxChildren> chld,
                std::array<Length, MaxChildren> offsets,
                size_t childCount, LFCount subTreeLineFeeds):
                    children(std::move(chld)),
                    offsets(std::move(offsets)),
                    childCount(childCount),
                    subTreeLineFeeds(subTreeLineFeeds)
            {
                if(childCount < MaxChildren)
                {
                    this->offsets.back() = offsets[childCount-1];
                }
            }
            std::variant<ChildArray, LeafArray > children;
            std::array<Length, MaxChildren> offsets;
            LFCount subTreeLineFeeds;
            size_t childCount;

            bool isLeaf ()const{
                return children.index() != 0;
            }

            Length subTreeLength() const
            {
                return offsets.back();
            }
        };
        explicit B_Tree() = default;

        const Node* root_ptr() const;
        bool is_empty() const;


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
            std::variant<ChildVector, LeafVector> children;
            size_t violated_invariant;

            std::vector<TreeManipResult> invalid;
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


};
