#pragma once

#include <memory>
#include <variant>
#include <vector>
#include <array>


#include "types.h"


namespace PieceTree
{
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

    };
    
    template <size_t MaxChildren>
    class B_Tree 
    {

        struct Node;
        using NodePtr = std::shared_ptr<const Node>;
        
        struct Node :public std::enable_shared_from_this<Node>
        {
            Node(std::array<NodePtr, MaxChildren> chld, 
                std::array<Length, MaxChildren> offsets, 
                size_t childCount):
                    children(std::move(chld)), 
                    offsets(std::move(offsets)), 
                    childCount(childCount)
            {
                
            }
            Node(std::array<NodeData, MaxChildren> chld, 
                std::array<Length, MaxChildren> offsets, 
                size_t childCount):
                    children(std::move(chld)), 
                    offsets(std::move(offsets)), 
                    childCount(childCount)
            {
                
            } 
            std::variant<std::array<NodePtr, MaxChildren>,
                        std::array<NodeData, MaxChildren> > children;
            std::array<Length, MaxChildren> offsets;
            size_t childCount;
            
            bool isLeaf ()const{
                return children.index() != 0;
            }
            
            Length subTreeLength() const
            {
                return offsets.back();
            }
        };
    public:
        explicit B_Tree() = default;
        
        const Node* root_ptr() const;
        bool is_empty() const;
        
        
        // Helpers.
        bool operator==(const B_Tree&) const = default;

        // Mutators.
        B_Tree insert(const NodeData& x, Offset at) const;
        B_Tree remove(Offset at) const;
        B_Tree removeRange(Offset at, Length len) const;
    private:
        B_Tree(NodePtr root);
        struct TreeManipResult{
            std::variant<std::vector<NodePtr>,std::vector<NodeData> > children;
            std::vector<Length> offsets;
            bool preserved_invariant;
        };
        TreeManipResult insertInto(const Node* node, const NodeData& x, Length at) const;
        TreeManipResult insertInto_leaf(const Node* node, const NodeData& x, Length at) const;
        NodePtr construct_leaf(std::vector<NodeData> data, std::vector<Length> offsets,size_t begin, size_t end) const ;
        NodePtr construct_internal(std::vector<NodePtr> data, std::vector<Length> offsets, size_t begin, size_t end) const ;
        
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
            
        };
        std::vector<StackEntry> stack;
    };
    
    struct WalkSentinel { };

    template<size_t C>
    inline B_TreeWalker<C> begin(const B_Tree<C>& tree)
    {
        return B_TreeWalker{ &tree };
    }
    template<size_t C>
    constexpr WalkSentinel end(const B_Tree<C>&)
    {
        return WalkSentinel{ };
    }
    template<size_t C>
    inline bool operator==(const B_TreeWalker<C>& walker, WalkSentinel)
    {
        return walker.exhausted();
    }
}