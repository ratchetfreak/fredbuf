#include "ratbuf.h"
#include "ratbuf_btree.h"


#include <cassert>

//#include <memory>
//#include <string_view>
//#include <string>
//#include <vector>
//#include <array>
//#include <sstream>
#include <bit>

#include "types.h"
#include "enum-utils.h"
#include "scope-guard.h"
#include "util.h"


template<size_t MaxChildren>
void print_tree(RatchetPieceTree::BNodeCountedGeneric<MaxChildren>* root, const RatchetPieceTree::Tree* tree, int level = 0, size_t node_offset = 0);

namespace RatchetPieceTree
{
#ifdef LOG_ALGORITHM
    std::vector<algo_marker> algorithm;
    constexpr LFCount operator+(LFCount lhs, LFCount rhs)
    {
        return LFCount{ rep(lhs) + rep(rhs) };
    }
#endif
#ifdef COUNT_ALLOC
     size_t alloc_count;
     size_t dealloc_count;
#endif
    RatchetPieceTree::LFCount tree_lf_count(const StorageTree& root)
    {
        if (root.is_empty())
            return { };
        return root.root_ptr()->subTreeLineFeeds();
    }
    namespace
    {

        RatchetPieceTree::Length tree_length(const StorageTree& root)
        {
            if (root.is_empty())
                return { };
            return root.root_ptr()->subTreeLength();
        }
    } // namespace [anon]

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::B_Tree(NodePtr root, uint32_t depth):root_node(root), tree_depth(depth){

    }
    
    
    
    template<size_t MaxChildren>
    B_Tree<MaxChildren> B_Tree<MaxChildren>::dup() const
    {
        
        return B_Tree(take_node_ref(root_node), tree_depth);
    }
    template<size_t MaxChildren>
    B_Tree<MaxChildren> B_Tree<MaxChildren>::construct_from(BTreeBlock* blk, NodeData* leafNodes, size_t leafCount)
    {
        if(leafCount == 0)
        {
            return B_Tree<MaxChildren>();
        }
        if(leafCount <= MaxChildren)
        {
            return B_Tree(reinterpret_cast<NodePtr>(construct_leaf(blk, leafNodes, 0, leafCount)), 1);
        }
        B_Tree<MaxChildren> result;
        Arena::Temp scratch = Arena::scratch_begin(Arena::no_conflicts);
        NodeVector nodes = Arena::push_array<NodePtr>(scratch.arena, leafCount/MaxChildren + 4);
        size_t nodeCount = 0;
        size_t i = 0;
        for(; i+ MaxChildren*2 < leafCount; i+=MaxChildren)
        {
            nodes[nodeCount++]=(construct_leaf(blk, leafNodes, i, i+MaxChildren));
            
        }
        size_t remaining = leafCount - i;
        assert(nodeCount>0);
        
        {

            nodes[nodeCount++]=(construct_leaf(blk, leafNodes, i, i+remaining/2));
            i+=remaining/2;
            nodes[nodeCount++]=(construct_leaf(blk, leafNodes, i, leafCount));
        }
        uint32_t depth = 1;
        NodeVector childNodes = Arena::push_array<NodePtr>(scratch.arena, leafCount/MaxChildren + 4);
        while(nodeCount > MaxChildren)
        {
            size_t oldCount = nodeCount;
            nodeCount = 0;
            
            i = 0;
            for(; i+ MaxChildren*2 < oldCount; i+=MaxChildren)
            {
                childNodes[nodeCount] = (construct_internal(blk, nodes, i, i+MaxChildren));
            }
            remaining = oldCount - i;
            assert((remaining <= (MaxChildren*2) && remaining >= MaxChildren));
            childNodes[nodeCount++] = (construct_internal(blk, nodes, i, i+remaining/2));
            i+=remaining/2;
            childNodes[nodeCount++] = (construct_internal(blk, nodes, i, oldCount));
            
            NodeVector t = nodes;
            nodes = childNodes;
            childNodes = t;
            
            depth++;
        }
        return B_Tree(construct_internal(blk, nodes, 0, nodeCount), depth+1);
    }

    template<size_t MaxChildren>
    bool B_Tree<MaxChildren>::is_empty() const
    {
        return not root_node;
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren> B_Tree<MaxChildren>::insert(BufferCollection* blk, const NodeData& x, Offset at) const
    {
        Arena::Temp scratch = scratch_begin(Arena::no_conflicts);
        NodePtr root = root_node;
        auto result = insertInto(scratch.arena, blk, root, x, Editor::distance(Offset(0), at));
        size_t newNumChildren = result.count;
        if(newNumChildren > 1)
        {
            NodePtr new_root = construct_internal(blk->rb_tree_blk, result.nodes, 0, newNumChildren);
            Arena::scratch_end(scratch);
            return B_Tree<MaxChildren>(new_root, result.depth+1);
        }
        else
        {
            if(result.count == 0)
            {
                Arena::scratch_end(scratch);
                return B_Tree<MaxChildren>();
            }
            else
            {
                
                Arena::scratch_end(scratch);
                return B_Tree<MaxChildren>(std::move(result.nodes[0]), result.depth);
            }
        }
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren> B_Tree<MaxChildren>::remove(BufferCollection* blk, Offset at, Length len) const
    {
        Arena::Temp scratch = scratch_begin(Arena::no_conflicts);
        TreeManipResult result = remove_from(scratch.arena, blk, root_node, nullptr, nullptr, distance(Offset{0},at), len);
        
        B_Tree<MaxChildren> res;

        if(result.count == 0)
        {
            res = B_Tree<MaxChildren>();
        }
        else if(result.count == 1)
        {
            res = B_Tree<MaxChildren>((result.nodes[0]), result.depth);
        }
        else
        {
            assert(result.count <= MaxChildren);
            NodePtr new_root = construct_internal(blk->rb_tree_blk, result.nodes, 0, result.count);
            
            res = B_Tree<MaxChildren>(new_root, result.depth+1);
        }
        Arena::scratch_end(scratch);
        return res;
    }

    BufferCursor buffer_position(const BufferCollection* buffers, const Piece& piece, Length remainder)
    {
        const LineStarts* starts = &buffers->buffer_at(piece.index)->line_starts;
        auto start_offset = rep(starts->starts[rep(piece.first.line)]) + rep(piece.first.column);
        auto offset = start_offset + rep(remainder);

        // Binary search for 'offset' between start and ending offset.
        auto low = rep(piece.first.line);
        auto high = rep(piece.last.line);

        size_t mid = 0;
        size_t mid_start = 0;
        size_t mid_stop = 0;

        while (low <= high)
        {
            mid = low + ((high - low) / 2);
            mid_start = rep(starts->starts[mid]);

            if (mid == high)
                break;
            mid_stop = rep(starts->starts[mid + 1]);

            if (offset < mid_start)
            {
                high = mid - 1;
            }
            else if (offset >= mid_stop)
            {
                low = mid + 1;
            }
            else
            {
                break;
            }
        }

        return { .line = Line{ mid },
                    .column = Column{ offset - mid_start } };
    }

    LFCount line_feed_count(const BufferCollection* buffers, BufferIndex index, const BufferCursor& start, const BufferCursor& end)
    {
        // If the end position is the beginning of a new line, then we can just return the difference in lines.
        if (rep(end.column) == 0)
            return LFCount{ rep(retract(end.line, rep(start.line))) };
        const LineStarts* starts = &buffers->buffer_at(index)->line_starts;
        // It means, there is no LF after end.
        if (end.line == Line{ starts->count - 1})
            return LFCount{ rep(retract(end.line, rep(start.line))) };
        // Due to the check above, we know that there's at least one more line after 'end.line'.
        auto next_start_offset = starts->starts[rep(extend(end.line))];
        auto end_offset = rep(starts->starts[rep(end.line)]) + rep(end.column);
        // There are more than 1 character after end, which means it can't be LF.
        if (rep(next_start_offset) > end_offset + 1)
            return LFCount{ rep(retract(end.line, rep(start.line))) };
        // This must be the case.  next_start_offset is a line down, so it is
        // not possible for end_offset to be < it at this point.
        assert(end_offset + 1 == rep(next_start_offset));
        return LFCount{ rep(retract(end.line, rep(start.line))) };
    }

    Piece trim_piece_right(const BufferCollection* buffers, const Piece& piece, const BufferCursor& pos)
    {
        auto orig_end_offset = buffers->buffer_offset(piece.index, piece.last);

        auto new_end_offset = buffers->buffer_offset(piece.index, pos);
        auto new_lf_count = line_feed_count(buffers, piece.index, piece.first, pos);

        auto len_delta = distance(new_end_offset, orig_end_offset);
        auto new_len = retract(piece.length, rep(len_delta));

        auto new_piece = piece;
        new_piece.last = pos;
        new_piece.newline_count = new_lf_count;
        new_piece.length = new_len;

        return new_piece;
    }

    Piece trim_piece_left(const BufferCollection* buffers, const Piece& piece, const BufferCursor& pos)
    {
        auto orig_start_offset = buffers->buffer_offset(piece.index, piece.first);

        auto new_start_offset = buffers->buffer_offset(piece.index, pos);
        auto new_lf_count = line_feed_count(buffers, piece.index, pos, piece.last);

        auto len_delta = distance(orig_start_offset, new_start_offset);
        auto new_len = retract(piece.length, rep(len_delta));

        auto new_piece = piece;
        new_piece.first = pos;
        new_piece.newline_count = new_lf_count;
        new_piece.length = new_len;

        return new_piece;
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::TreeManipResult B_Tree<MaxChildren>::insertInto_leaf(Arena::Arena *arena, BufferCollection* blk, LeafNodePtr node, const NodeData& x, Length at) const
    {
        if(node)algo_mark(node->shared_from_this(), Traverse);
        if(node==nullptr)
        {
            B_Tree<MaxChildren>::TreeManipResult res;
            res.nodes = Arena::push_array<NodePtr>(arena, 1);
            res.count = 1;
            res.depth = 1;
            res.nodes[0] = construct_leaf(blk->rb_tree_blk, &x, 0, 1);
            
            return res;
        }
        Arena::Temp scratch = Arena::scratch_begin({&arena, 1});
        auto offsets_end = node->offsets.begin()+node->childCount;
        auto insertPoint = std::lower_bound(node->offsets.begin(), offsets_end, at);
        auto insertIndex = insertPoint - node->offsets.begin();
        
        NodeData* resultch = Arena::push_array<NodeData>(scratch.arena, node->childCount+2);
        size_t resultCount = 0;
        //std::vector<NodeData> resultch;

        auto& children = (node->children);

        auto child_it = children.begin();
        auto child_insert = children.begin()+insertIndex;
        auto child_end = children.begin()+node->childCount;

        Length insert_child_start {0};
        if(insertIndex > 0)
        {
            insert_child_start  = node->offsets[insertIndex-1];
        }

        Length split_offset = at - insert_child_start;
        for(; child_it!=child_insert; ++child_it)
        {
            resultch[resultCount++] = (*child_it);
        }
        auto splitting_piece = (*child_it).piece;
        if(split_offset > Length{0} && split_offset < splitting_piece.length)
        {
            BufferCollection* buffers = blk;

            auto insert_pos = buffer_position(buffers, splitting_piece, split_offset);

            auto new_len_right = distance(buffers->buffer_offset(splitting_piece.index, insert_pos),
                                        buffers->buffer_offset(splitting_piece.index, splitting_piece.last));
            auto new_piece_right = splitting_piece;
            new_piece_right.first = insert_pos;
            new_piece_right.length = new_len_right;
            new_piece_right.newline_count = line_feed_count(buffers, splitting_piece.index, insert_pos, splitting_piece.last);

            // Remove the original node tail.
            auto new_piece_left = trim_piece_right(buffers, splitting_piece, insert_pos);

            auto new_piece = x.piece;

            NodeData first{
                .piece = new_piece_left
            };
            resultch[resultCount++] = (first);

            resultch[resultCount++] = (x);

            NodeData second = NodeData
            {
                .piece = new_piece_right
            };

            resultch[resultCount++] = (second);
            ++child_it;
        }
        else
        {
            //TODO(ratchetfreak): combine pieces with modbuf?
            if(split_offset == Length{0})
            {
                resultch[resultCount++] = (x);
                resultch[resultCount++] = (*child_it);
                ++child_it;
            }
            else
            {
                auto node_data = *child_it;
                if(node_data.piece.index == x.piece.index && node_data.piece.last == x.piece.first)
                {
                    Piece old_piece = node_data.piece;
                    NodeData d = x;
                    Piece &new_piece = d.piece;
                    new_piece.first = old_piece.first;
                    new_piece.newline_count = LFCount{rep(new_piece.newline_count) + rep(old_piece.newline_count)};
                    new_piece.length = new_piece.length + old_piece.length;
                    resultch[resultCount++] = (d);
                    ++child_it;
                }
                else
                {
                    resultch[resultCount++] = (*child_it);
                    ++child_it;
                    resultch[resultCount++] = (x);
                }
            }
        }

        for(; child_it!=child_end; ++child_it)
        {
            resultch[resultCount++] = (*child_it);
        }
        
        B_Tree<MaxChildren>::TreeManipResult res;
        if(resultCount > MaxChildren)
        {
            res.nodes = Arena::push_array<NodePtr>(arena, 2);
            size_t mid = resultCount/2;
            res.nodes[0] = (construct_leaf(blk->rb_tree_blk, resultch, 0, mid));
            res.nodes[1] = (construct_leaf(blk->rb_tree_blk, resultch, mid, resultCount));
            res.count = 2;
            res.depth = 1;
        }
        else
        {
            res.nodes = Arena::push_array<NodePtr>(arena, 1);
            res.nodes[0] = (construct_leaf(blk->rb_tree_blk, resultch, 0, resultCount));
            res.count = 1;
            res.depth = 1;
        }
        Arena::scratch_end(scratch);
        return res;
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::TreeManipResult B_Tree<MaxChildren>::remove_from_leafs(Arena::Arena *arena, BufferCollection* blk, LeafNodePtr a, LeafNodePtr b, LeafNodePtr c, Length at, Length len) const
    {
        Arena::Temp scratch = Arena::scratch_begin({&arena, 1});
        // b or c might be nullptr

        NodeData *allLeafs = Arena::push_array<NodeData>(scratch.arena, MaxChildren*3);
        size_t leaflength = 0;
        {
            auto ch = (a->children);
            for(auto it = ch.begin(); it != ch.begin()+b->childCount;++it)
            {
                allLeafs[leaflength++]=(*it);
            }
        }
        if(b!=nullptr)
        {
            auto ch = (b->children);
            for(auto it = ch.begin(); it != ch.begin()+b->childCount;++it)
            {
                allLeafs[leaflength++]=(*it);
            }
        }
        if(c!=nullptr)
        {
            auto ch = (c->children);
            for(auto it = ch.begin(); it != ch.begin()+c->childCount;++it)
            {
                allLeafs[leaflength++]=(*it);
            }
        }
        size_t numLeafs = leaflength;
        (void)numLeafs;

        
        NodeData *allChildren = Arena::push_array<NodeData>(scratch.arena, MaxChildren*3 + 3);
        size_t resultCount = 0;

        int i = 0;

        for(;i < leaflength && allLeafs[i].piece.length <= at;i++)
        {
            auto current_leaf = allLeafs[i];
            allChildren[resultCount++] = (current_leaf);
            at = at - current_leaf.piece.length;
        }

        //trim or split allLeafs[i].piece
        if(at+len <= allLeafs[i].piece.length)
        {
            auto piece_to_split = allLeafs[i].piece;
            auto start_split_pos = buffer_position(blk, piece_to_split, at);

            auto end_split_pos = buffer_position(blk, piece_to_split, at+len);
            auto piece_left = trim_piece_left(blk, piece_to_split, end_split_pos);
            Piece piece_right = trim_piece_right(blk, piece_to_split, start_split_pos);

            if(rep(piece_right.length) > 0)
                allChildren[resultCount++] = {piece_right};
            if(rep(piece_left.length) > 0)
                allChildren[resultCount++] = {piece_left};

            //split
            i++;
        }
        else
        {
            if(rep(at)>0)
            {
                auto start_split_pos = buffer_position(blk, allLeafs[i].piece, at);
                auto trimright = trim_piece_right(blk, allLeafs[i].piece, start_split_pos);
                allChildren[resultCount++] = {trimright};
            }
            len = len -  (allLeafs[i].piece.length - at);
            i++;
            for(;i < leaflength && allLeafs[i].piece.length <= len;i++)
            {
                len  = len - allLeafs[i].piece.length;
            }
            if(rep(len) > 0 && i < leaflength)
            {
                auto end_split_pos = buffer_position(blk, allLeafs[i].piece, len);

                auto piece_left = trim_piece_left(blk, allLeafs[i].piece, end_split_pos);
                allChildren[resultCount++] = {piece_left};
                i++;
            }
        }

        //add rest

        for(;i< leaflength;i++)
        {
            allChildren[resultCount++] = allLeafs[i];
        }

        TreeManipResult result;
        result.depth = 1;
        if(resultCount > MaxChildren*3)
        {
            
            size_t from = 0;
            size_t perChild = resultCount/4;
            result.nodes = Arena::push_array<NodePtr>(arena, 4);
            result.count = 4;

            result.nodes[0] = (construct_leaf(blk->rb_tree_blk, allChildren, from, from+perChild));
            from+=perChild;
            if(resultCount%4 > 1)
            {
                perChild+=1;
            }
            result.nodes[1] = (construct_leaf(blk->rb_tree_blk, allChildren, from, from+perChild));
            from+=perChild;
            result.nodes[2] = (construct_leaf(blk->rb_tree_blk, allChildren, from, from+perChild));
            from+=perChild;
            result.nodes[3] = (construct_leaf(blk->rb_tree_blk, allChildren, from, resultCount));
        }
        else if(resultCount > MaxChildren*2)
        {
            size_t from = 0;
            size_t perChild = resultCount/3;
            result.nodes = Arena::push_array<NodePtr>(arena, 3);
            result.count = 3;

            result.nodes[0] = (construct_leaf(blk->rb_tree_blk, allChildren, from, from+perChild));
            from+=perChild;
            if(resultCount%3 > 1)
            {
                perChild+=1;
            }
            result.nodes[1] = (construct_leaf(blk->rb_tree_blk, allChildren, from, from+perChild));
            from+=perChild;
            result.nodes[2] = (construct_leaf(blk->rb_tree_blk, allChildren, from, resultCount));
        }
        else if(resultCount > MaxChildren)
        {
            
            size_t from = 0;
            size_t mid = resultCount/2;
            result.nodes = Arena::push_array<NodePtr>(arena, 2);
            result.count = 2;

            result.nodes[0] = (construct_leaf(blk->rb_tree_blk, allChildren, from, from+mid));

            result.nodes[1] = (construct_leaf(blk->rb_tree_blk, allChildren, mid, resultCount));

        }
        else if(resultCount > 0)
        {
            result.nodes = Arena::push_array<NodePtr>(arena, 1);
            result.count = 1;

            //assert(allChildren.size() >= MaxChildren/2);
            result.nodes[0] = (construct_leaf(blk->rb_tree_blk, allChildren, 0, resultCount));
        }
        return result;
    }

    template<typename It, typename T, typename Cmp>
    It branchless_lower_bound(It begin, It end, const T & value, Cmp && compare)
    {
        size_t length = end - begin;
        if (length == 0)
            return end;
        size_t step = std::bit_floor(length);
        if (step != length && compare(begin[step], value))
        {
            length -= step + 1;
            if (length == 0)
                return end;
            step = std::bit_ceil(length);
            begin = end - step;
        }
        for (step /= 2; step != 0; step /= 2)
        {
            if (compare(begin[step], value))
                begin += step;
        }
        return begin + compare(*begin, value);
    }
    template<typename It, typename T>
    It branchless_lower_bound(It begin, It end, const T & value)
    {
        return branchless_lower_bound(begin, end, value, std::less<>{});
    }

    void Tree::insert(CharOffset offset, String8 txt, SuppressHistory suppress_history)
    {
        if (txt.size)
            return;
        // This allows us to undo blocks of code.
        if (is_no(suppress_history)
            and (end_last_insert != offset or root.is_empty()))
        {
            append_undo(root, offset);
        }
        internal_insert(offset, txt);
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::TreeManipResult B_Tree<MaxChildren>::insertInto(Arena::Arena *arena, BufferCollection* blk, const NodePtr node, const NodeData& x, Length at) const
    {
        if(node)algo_mark(node->shared_from_this(), Traverse);
        //TODO(ratchetfreak) : change result to be 1 or more ChildPtrs which is exactly the x node with the insert
        if(node==nullptr||node->isLeaf())
        {
            return insertInto_leaf(arena, blk, reinterpret_cast<LeafNodePtr>(node), x, at);
        }
        else
        {
            Arena::Temp scratch = Arena::scratch_begin({&arena, 1});
            InternalNodePtr in = reinterpret_cast<InternalNodePtr>(node);
            NodeVector node_children = (in->children);
            auto offsets_end = node->offsets.begin()+node->childCount;
            auto insertPoint = std::lower_bound(node->offsets.begin(), offsets_end, at);
            size_t insertIndex = insertPoint - node->offsets.begin();
            auto insertChIt = node_children+insertIndex;
            auto insertChItEnd = node_children+node->childCount;

            NodeVector resultch = Arena::push_array<NodePtr>(scratch.arena, in->childCount + 2);
            size_t resultCount = 0;
            auto child_it = (node_children);

            for(; child_it!=insertChIt; ++child_it)
            {
                resultch[resultCount++] = take_node_ref(*child_it);
                
            }

            Length childInsert = at;
            if(insertIndex>0)
            {
                childInsert = childInsert - node->offsets[insertIndex - 1];
            }
            auto insert_child = node_children[insertIndex];
            auto insert_result = insertInto(scratch.arena, blk, insert_child, x, childInsert);

            size_t newNumChildren = insert_result.count;

            for(int i = 0; i< newNumChildren;i++)
            {
                resultch[resultCount++] = (insert_result.nodes[i]);
            }
            child_it++;
            for(; child_it!=insertChItEnd;  ++child_it)
            {
                resultch[resultCount++] = take_node_ref(*child_it);
            }

            TreeManipResult manipresult{};
            manipresult.nodes = Arena::push_array<NodePtr>(arena, in->childCount + 2);
            if(resultCount > MaxChildren)
            {
                //split the to-be inserted node into two
                size_t numLChild = resultCount/2;

                NodePtr leftChild = construct_internal(blk->rb_tree_blk, resultch, 0, numLChild);
                NodePtr rightChild = construct_internal(blk->rb_tree_blk,resultch, numLChild, resultCount);

                manipresult.nodes[manipresult.count++] = leftChild;
                manipresult.nodes[manipresult.count++] = rightChild;

                //return manipresult;
            }
            else
            {
                auto newChild = construct_internal(blk->rb_tree_blk, resultch
                , 0, resultCount);
                manipresult.nodes[manipresult.count++] = newChild;
                
            }
            Arena::scratch_end(scratch);
            return manipresult;
        }
        // return {};
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::TreeManipResult B_Tree<MaxChildren>::remove_from(Arena::Arena *arena, BufferCollection* blk, NodePtr a, NodePtr b, NodePtr c, Length at, Length len) const
    {
        // at least one of a and c should not participate in the remove
        // that way the result is always big enough to make at least 1 node
        Arena::Temp scratch = Arena::scratch_begin({&arena, 1});
        if(a)algo_mark(a->shared_from_this(), Collect);
        if(b)algo_mark(b->shared_from_this(), Collect);
        if(c)algo_mark(c->shared_from_this(), Collect);

        if(a->isLeaf())
        {
            assert(!b || b->isLeaf());
            assert(!c || c->isLeaf());
            Arena::scratch_end(scratch);
            return remove_from_leafs(arena, blk, reinterpret_cast<LeafNodePtr>(a), reinterpret_cast<LeafNodePtr>(b), reinterpret_cast<LeafNodePtr>(c), at, len);
        }
        else
        {
            size_t totalChildCount = (a?a->childCount:0)+(b?b->childCount:0)+(c?c->childCount:0);
            assert(!b || !b->isLeaf());
            assert(!c || !c->isLeaf());
            NodeVector resultChildren = Arena::push_array<NodePtr>(scratch.arena, totalChildCount+3);
            size_t childCount = 0;
            NodeVector allChildren = Arena::push_array<NodePtr>(scratch.arena, totalChildCount);

            {
                InternalNodePtr ina = reinterpret_cast<InternalNodePtr>(a);
                NodeVector chA = (ina->children);
                auto outputIt = allChildren;
                outputIt = std::copy_n(chA, a->childCount, outputIt);
                totalChildCount = a->childCount;
                if(b!=nullptr)
                {
                    InternalNodePtr inb = reinterpret_cast<InternalNodePtr>(b);
                    NodeVector chB = (inb->children);
                    outputIt = std::copy_n(chB, b->childCount, outputIt);
                    totalChildCount = totalChildCount + b->childCount;
                }
                if(c!=nullptr)
                {
                    InternalNodePtr inc = reinterpret_cast<InternalNodePtr>(c);
                    NodeVector chC = (inc->children);
                    outputIt = std::copy_n(chC, c->childCount, outputIt);
                    totalChildCount = totalChildCount + c->childCount;
                }
            }
            NodePtr recA = nullptr;
            NodePtr recB = nullptr;
            NodePtr recC = nullptr;

            Length offset = at;

            Length aplusbplusc {0};
            int i = 0;
            for(; i<totalChildCount &&
                    allChildren[i]->subTreeLength() + aplusbplusc < offset;i++)
            {
                if(recA)
                {
                    offset = offset - recA->subTreeLength();
                    resultChildren[childCount++] = take_node_ref(recA);
                }
                recA = recB;
                recB = recC;
                recC = allChildren[i];
                aplusbplusc = (recA?recA->subTreeLength():Length{0})+
                        (recB?recB->subTreeLength():Length{0})+
                        recC->subTreeLength();
            }
            //now allChildren[i] is the first node that needs removing anything
            
            if(recA)
            {
                offset = offset - recA->subTreeLength();
                resultChildren[childCount++] = take_node_ref(recA);
            }
            recA = recB;
            recB = recC;
            recC = allChildren[i];
            aplusbplusc = (recA?recA->subTreeLength():Length{0})+
                        (recB?recB->subTreeLength():Length{0})+
                        recC->subTreeLength();
            Length to_remove_from_first_found = aplusbplusc - offset;
            i++;
            TreeManipResult res;
            
            algo_mark(recC, Traverse);

            if(len < to_remove_from_first_found)
            {
                //everything to be removed is in recC, ensure recA and recB are filled

                while(!recA)
                {
                    if(i >= totalChildCount)
                    {
                        //end of array and not enough to fill a node
                        assert(c==nullptr);
                        if(recB)
                        {
                            Arena::scratch_end(scratch);
                            return remove_from(arena, blk, recB, recC, nullptr, offset, len);
                        }
                        else
                        {
                            Arena::scratch_end(scratch);
                            return remove_from(arena, blk, recC, nullptr, nullptr, offset, len);
                        }
                    }
                    recA = recB;
                    recB = recC;
                    recC = allChildren[i];
                    i++;
                    algo_mark(recC, Traverse);

                }
                res = remove_from(scratch.arena, blk, recA, recB, recC, offset, len);
            }
            else
            {
                Length to_remove_from_rest = len - to_remove_from_first_found;

                for(;i< totalChildCount &&
                        allChildren[i]->subTreeLength() < to_remove_from_rest;i++)
                {
                    to_remove_from_rest = to_remove_from_rest - allChildren[i]->subTreeLength();
                    algo_mark(allChildren[i], Skip);
                }

                //now allChildren[i] is the last child to remove anything from

                if(i >= totalChildCount && !recA)
                {
                    //end of array and not enough to fill a node
                    assert(c==nullptr); // can only come from root
                    if(recB)
                    {
                        Arena::scratch_end(scratch);
                        return remove_from(arena, blk, recB, recC, nullptr, offset, to_remove_from_first_found + to_remove_from_rest);
                    }
                    else
                    {
                        Arena::scratch_end(scratch);
                        return remove_from(arena, blk, recC, nullptr, nullptr, offset, to_remove_from_first_found + to_remove_from_rest);
                    }
                }

                if(i < totalChildCount)
                {
                    if(recA)
                    {
                        offset = offset - recA->subTreeLength();
                        resultChildren[childCount++] = recA;
                    }
                    recA = recB;
                    recB = recC;
                    recC = allChildren[i];
                    i++;
                    
                    algo_mark(recC, Traverse);
                }
                //recA and recB might still be nullptr
                while(!recA)
                {
                    if(i >= totalChildCount)
                    {
                        //end of array and not enough to fill a node
                        assert(c==nullptr);
                        if(recB)
                        {
                            Arena::scratch_end(scratch);
                            return remove_from(arena, blk, recB, recC, nullptr, offset, to_remove_from_first_found + to_remove_from_rest);
                        }
                        else
                        {
                            Arena::scratch_end(scratch);
                            return remove_from(arena, blk, recC, nullptr, nullptr, offset, to_remove_from_first_found + to_remove_from_rest);
                        }
                    }
                    recA = recB;
                    recB = recC;
                    recC = allChildren[i];
                    i++;
                    
                    algo_mark(recC, Traverse);

                }
                res = remove_from(scratch.arena, blk, recA, recB, recC, offset, to_remove_from_first_found + to_remove_from_rest);
            }

            for EachIndex(it, res.count)
            {
                resultChildren[childCount++] = res.nodes[it];
            }

            for(;i<totalChildCount;i++)
            {
                resultChildren[childCount++] = take_node_ref(allChildren[i]);
            }

            TreeManipResult result;
            result.depth = res.depth+1;
            if(childCount > MaxChildren*3)
            {
                size_t from = 0;
                size_t perChild = childCount/4;
                result.nodes = Arena::push_array<NodePtr>(arena, 4);
                result.count = 4;

                result.nodes[0] = (construct_internal(blk->rb_tree_blk, resultChildren, from, from+perChild));
                from+=perChild;
                if(childCount%4 > 1)
                {
                    perChild+=1;
                }
                result.nodes[1] = (construct_internal(blk->rb_tree_blk, resultChildren, from, from+perChild));
                from+=perChild;
                result.nodes[2] = (construct_internal(blk->rb_tree_blk, resultChildren, from, from+perChild));
                from+=perChild;
                result.nodes[3] = (construct_internal(blk->rb_tree_blk, resultChildren, from, childCount));
            }
            else if(childCount > MaxChildren*2)
            {
                size_t from = 0;
                size_t perChild = childCount/3;

                result.nodes = Arena::push_array<NodePtr>(arena, 3);
                result.count = 3;
                result.nodes[0] = (construct_internal(blk->rb_tree_blk, resultChildren, from, from+perChild));
                from+=perChild;
                if(childCount%3 > 1)
                {
                    perChild+=1;
                }
                result.nodes[1] = (construct_internal(blk->rb_tree_blk, resultChildren, from, from+perChild));
                from+=perChild;
                result.nodes[2] = (construct_internal(blk->rb_tree_blk, resultChildren, from, childCount));
            }
            else if(childCount > MaxChildren)
            {
                size_t from = 0;
                size_t mid = childCount/2;
                result.nodes = Arena::push_array<NodePtr>(arena, 2);
                result.count = 2;
                result.nodes[0] = (construct_internal(blk->rb_tree_blk, resultChildren, from, from+mid));

                result.nodes[1] = (construct_internal(blk->rb_tree_blk, resultChildren, mid, childCount));

            }
            else
            {
                assert(childCount >= MaxChildren/2 || c==nullptr);
                result.nodes = Arena::push_array<NodePtr>(arena, 1);
                result.count = 1;
                result.nodes[0] = (construct_internal(blk->rb_tree_blk, resultChildren, 0, childCount));
            }
            Arena::scratch_end(scratch);
            return result;
        }
    }


    template<size_t MaxChildren>
    BNodeCountedGeneric<MaxChildren>* take_node_ref( BNodeCountedGeneric<MaxChildren>* node)
    {
        if (node != nullptr)
        {
            FRED_UNUSED_RESULT(os_atomic_u64_inc_eval(&node->blk->ref_count));
        }
        return node;
    }
    
    template<size_t MaxChildren>
    BNodeCountedGeneric<MaxChildren>* take_node_ref( BNodeCountedInternal<MaxChildren>* node)
    {
        return take_node_ref(reinterpret_cast<BNodeCountedGeneric<MaxChildren>*>(node));
    }
    
    template<size_t MaxChildren>
    BNodeCountedGeneric<MaxChildren>* take_node_ref( BNodeCountedLeaf<MaxChildren>* node)
    {
        return take_node_ref(reinterpret_cast<BNodeCountedGeneric<MaxChildren>*>(node));
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::NodePtr B_Tree<MaxChildren>::construct_leaf(BTreeBlock* blk, const NodeData* data, size_t begin, size_t end) 
    {
        LeafNodePtr node = nullptr;
        BNodeBlock* node_blk = nullptr;
        // Try to fetch a node from the free list atomically.
        {
            BNodeFreeList old_head;
            BNodeFreeList next_head{};
            os_atomic_u128_eval(&blk->free_list.leaf, &old_head.leaf);
            do
            {
                if (nil_node(old_head.leaf.head))
                {
                    node = nullptr;
                    break;
                }
                node = reinterpret_cast<LeafNodePtr>(old_head.leaf.head);
                next_head.leaf.head = old_head.leaf.head->next;
                next_head.leaf.tag = old_head.leaf.tag + 1;
            } while (not os_atomic_u128_eval_cond_assign(&blk->free_list.leaf, next_head.leaf, &old_head.leaf));
            if (node != nullptr)
            {
                node_blk = node->blk;
            }
        }

        if (node==nullptr)
        {
            node = Arena::push_array_no_zero<BNodeCountedLeaf<MaxChildren>>(blk->alloc_arena, 1);
            node_blk = Arena::push_array<BNodeBlock>(blk->alloc_arena, 1);
        }
        
        zero_bytes(node);
        zero_bytes(node_blk);
        
        node->blk = node_blk;
        
        take_node_ref(node);
        node->type = NodeType::LEAF;

        size_t numChild = end-begin;
        assert(numChild <= MaxChildren);
        auto result = reinterpret_cast<NodePtr>(node);
        std::array<NodeData, MaxChildren>& new_left_children = node->children;
        std::array<Length, MaxChildren>&  new_left_offsets = result->offsets;
        std::array<LFCount, MaxChildren>&  new_left_linefeed = result->lineFeeds;
        result->childCount = numChild;
        Length acc{0};
        LFCount linefeed{0};
        for(int i = 0; i < numChild; i++)
        {
            new_left_children[i] =data[begin+i];
            acc = acc + data[begin+i].piece.length;
            new_left_offsets[i] = acc;
            linefeed =LFCount {rep(linefeed) + rep(data[begin+i].piece.newline_count)};
            new_left_linefeed[i] = linefeed;
        }
        algo_mark(result, Made);
        return result;
    }


    template<size_t MaxChildren>
    B_Tree<MaxChildren>::NodePtr B_Tree<MaxChildren>::construct_internal(BTreeBlock* blk, B_Tree<MaxChildren>::NodeVector data, size_t begin, size_t end) 
    {
        InternalNodePtr node = nullptr;
        BNodeBlock* node_blk = nullptr;
        // Try to fetch a node from the free list atomically.
        {
            BNodeFreeList old_head;
            BNodeFreeList next_head{};
            os_atomic_u128_eval(&blk->free_list.leaf, &old_head.leaf);
            do
            {
                if (nil_node(old_head.leaf.head))
                {
                    node = nullptr;
                    break;
                }
                node = reinterpret_cast<InternalNodePtr>(old_head.leaf.head);
                next_head.leaf.head = old_head.leaf.head->next;
                next_head.leaf.tag = old_head.leaf.tag + 1;
            } while (not os_atomic_u128_eval_cond_assign(&blk->free_list.leaf, next_head.leaf, &old_head.leaf));
            if (node!=nullptr)
            {
                node_blk = node->blk;
            }
        }

        if (node==nullptr)
        {
            node = Arena::push_array_no_zero<BNodeCountedInternal<MaxChildren>>(blk->alloc_arena, 1);
            node_blk = Arena::push_array<BNodeBlock>(blk->alloc_arena, 1);
        }
        
        zero_bytes(node);
        zero_bytes(node_blk);
        
        node->blk = node_blk;
        
        take_node_ref(node);
        node->type = NodeType::INTERNAL;

        size_t numChild = end-begin;
        assert(numChild <= MaxChildren);
        auto result = reinterpret_cast<NodePtr>(node);
        
        assert(numChild <= MaxChildren);
        NodeVector new_left_children = &node->children[0];
        std::array<Length, MaxChildren>&  new_left_offsets = result->offsets;
        std::array<LFCount, MaxChildren>&  new_left_linefeed = result->lineFeeds;
        Length acc{0};
        LFCount linefeed{0};
        for(int i = 0; i < numChild; i++)
        {
            new_left_children[i] = data[begin+i];
            //take_node_ref(data[begin+i]);
             acc = acc + data[begin+i]->subTreeLength();
            new_left_offsets[i] = acc;
            linefeed =LFCount {rep(linefeed) + rep(data[begin+i]->subTreeLineFeeds())};
            new_left_linefeed[i] = linefeed;

        }

        algo_mark(result, Made);
        return result;
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::NodePtr B_Tree<MaxChildren>::root_ptr() const
    {
        return root_node;
    }

#ifdef TEXTBUF_DEBUG
    template<size_t MaxChildren>
    void satisfies_btree_invariant(const B_Tree<MaxChildren>& root)
    {
        // 1. every node other than root has >= MaxChildren/2 children 
        // 1.5. nodes are hardcoded to contain <= MaxChildren
        // 2. all leafNodes are at the same depth
        
        typedef B_Tree<MaxChildren>::NodePtr NPtr;
        typedef B_Tree<MaxChildren>::InternalNodePtr INPtr;
        typedef B_Tree<MaxChildren>::LeafNodePtr LNPtr;
        //typedef B_Tree<MaxChildren>::ChildArray  ChArr;
        //typedef B_Tree<MaxChildren>::LeafArray  LeafArr;
        std::vector<NPtr> layer;
        std::vector<NPtr> next_layer;
        
        if(root.is_empty() || root.root_ptr()->isLeaf())
        {
            assert(root.root_ptr()->childCount <= MaxChildren);
            return;
        }
        INPtr rn = reinterpret_cast<INPtr>(root.root_ptr());
        auto& children = (rn->children);
        next_layer.assign(&children[0], &children[0]+root.root_ptr()->childCount);
        while(!next_layer.front()->isLeaf())
        {
            layer = std::move(next_layer);
            next_layer.clear();
            for(auto nodeIt = layer.begin(); nodeIt != layer.end();++nodeIt)
            {
        
                assert(!(*nodeIt)->isLeaf());
                assert((*nodeIt)->childCount >= MaxChildren/2);
                INPtr in = reinterpret_cast<INPtr>(*nodeIt);
                auto& node_children = (in->children);
                assert((*nodeIt)->offsets.back() == (*nodeIt)->offsets[(*nodeIt)->childCount-1]);
                assert((*nodeIt)->lineFeeds.back() == (*nodeIt)->lineFeeds[(*nodeIt)->childCount-1]);
                
                LFCount prevLineEnd{};
                Length prevOffset{};
                for(size_t childIndex = 0; childIndex < (*nodeIt)->childCount; childIndex++)
                {
                    auto& node = node_children[childIndex];
                    auto offsetDifference = rep((*nodeIt)->offsets[childIndex]) - rep(prevOffset);
                    assert(rep(node->offsets.back()) == offsetDifference);
                    prevOffset = (*nodeIt)->offsets[childIndex];
                    
                    auto lineDifference = rep((*nodeIt)->lineFeeds[childIndex]) - rep(prevLineEnd);
                    assert(rep(node->lineFeeds.back()) == lineDifference);
                    prevLineEnd = (*nodeIt)->lineFeeds[childIndex];
                }
                next_layer.insert(next_layer.end(), &node_children[0], &node_children[0]+(*nodeIt)->childCount);
            }
        }
        layer = std::move(next_layer);
        for(auto nodeIt = layer.begin(); nodeIt != layer.end();++nodeIt)
        {
            assert((*nodeIt)->isLeaf());
                
            LNPtr ln = reinterpret_cast<LNPtr>(*nodeIt);
            auto& node_children = ((ln)->children);
            LFCount prevLineEnd{};
            Length prevOffset{};
            for(size_t childIndex = 0; childIndex < (*nodeIt)->childCount; childIndex++)
                {
                    auto& piece = node_children[childIndex].piece;
                    auto offsetDifference = rep((*nodeIt)->offsets[childIndex]) - rep(prevOffset);
                    assert(rep(piece.length) == offsetDifference);
                    prevOffset = (*nodeIt)->offsets[childIndex];
                    
                    auto lineDifference = rep((*nodeIt)->lineFeeds[childIndex]) - rep(prevLineEnd);
                    auto bufferline = rep(piece.last.line) - rep(piece.first.line);
                    assert(rep(piece.newline_count) == lineDifference);
                    assert(bufferline == lineDifference);
                    prevLineEnd = (*nodeIt)->lineFeeds[childIndex];
                }
            assert((*nodeIt)->isLeaf());
            assert((*nodeIt)->childCount >= MaxChildren/2);
        }
    }
#endif
}

namespace RatchetPieceTree
{
        namespace
    {
        struct LineStartsNode
        {
            LineStartsNode* next;
            LineStart start;
        };

        struct LineStartsList
        {
            LineStartsNode* first;
            LineStartsNode* last;
            uint64_t count;
        };

        void push_line_starts_node(Arena::Arena* arena, LineStartsList* lst, LineStart start)
        {
            LineStartsNode* node = Arena::push_array<LineStartsNode>(arena, 1);
            node->start = start;
            SLLQueuePush(lst->first, lst->last, node);
            ++lst->count;
        }

        LineStarts join_line_starts_list(Arena::Arena* arena, const LineStartsList& lst)
        {
            LineStarts result{};
            result.starts = Arena::push_array_no_zero<LineStart>(arena, lst.count);
            result.count = lst.count;
            uint64_t i = 0;
            for EachNode(n, lst.first)
            {
                result.starts[i++] = n->start;
            }
            return result;
        }

        void populate_line_starts(Arena::Arena* arena, LineStarts* starts, String8 buf)
        {
            LineStartsList lst{};
            LineStart start { };
            auto scratch = Arena::scratch_begin({ &arena, 1 });
            push_line_starts_node(scratch.arena, &lst, start);
            for EachIndex(i, buf.size)
            {
                char c = buf.str[i];
                if (c == '\n')
                {
                    start = LineStart{ i + 1 };
                    push_line_starts_node(scratch.arena, &lst, start);
                }
            }
            *starts = join_line_starts_list(arena, lst);
            Arena::scratch_end(scratch);
        }

        void compute_buffer_meta(BufferMeta* meta, const StorageTree& root)
        {
            meta->lf_count = tree_lf_count(root);
            meta->total_content_length = tree_length(root);
        }

        void append_mut_buf_start(BufferCollection* collection, LineStart start)
        {
            LineStart* new_starts = Arena::push_array_no_zero_aligned<LineStart>(collection->mut_buf_starts_arena, 1, Arena::Alignment{ alignof(LineStart) });
            LineStarts* starts = &collection->mod_buffer.line_starts;
            assert(starts->starts + starts->count == new_starts);
            new_starts[0] = start;
            ++starts->count;
        }

        void grow_mut_buf(BufferCollection* collection, uint64_t grow_by)
        {
            if (grow_by == 0)
                return;
            char* buf = Arena::push_array_no_zero_aligned<char>(collection->mut_buf_arena, grow_by, Arena::Alignment{ alignof(char) });
            FRED_UNUSED(buf);
            // When this buffer is created, it was originally designated a null-terminator slot at the beginning, so new buffers we
            // allocate will need a null-terminator appended.
            assert(collection->mod_buffer.buffer.str + collection->mod_buffer.buffer.size + 1 == buf);
            // Note: We do not change the base of 'str', only its size.
            collection->mod_buffer.buffer.size += grow_by;
            // Append our null to the newly returned buffer piece.
            collection->mod_buffer.buffer.str[collection->mod_buffer.buffer.size] = 0;
        }
    } // namespace [anon]
    
    const CharBuffer* BufferCollection::buffer_at(BufferIndex index) const
    {
        if (index == BufferIndex::ModBuf)
            return &mod_buffer;
        return &orig_buffers.buffers[rep(index)];
    }

    CharOffset BufferCollection::buffer_offset(BufferIndex index, const BufferCursor& cursor) const
    {
        LineStart* starts = buffer_at(index)->line_starts.starts;
        return CharOffset{ rep(starts[rep(cursor.line)]) + rep(cursor.column) };
    }

    // Buffer collection management.
    void dec_buffer_ref(BufferCollection* collection)
    {
        uint64_t count = os_atomic_u64_dec_eval(collection->orig_buffers.ref_count);
        if (count == 0)
        {
            // Note: The arena used to allocate nodes is the same one for the immutable buffers,
            // so we do not need to release it.
            Arena::release(collection->mut_buf_starts_arena);
            Arena::release(collection->undo_redo_stack_arena);
            Arena::release(collection->mut_buf_arena);
            Arena::release(collection->immutable_buf_arena);
        }
    }

    BufferCollection take_buffer_ref(const BufferCollection* collection)
    {
        FRED_UNUSED_RESULT(os_atomic_u64_inc_eval(collection->orig_buffers.ref_count));
        return *collection;
    }
    
    Tree::Tree(BufferCollection buffers):
        buffers{ buffers }
    {
        build_tree();
    }

    BufferCollection Tree::buffer_collection_no_ref() const
    {
        return buffers;
    }
    
    BufferCursor Tree::buffer_position(const BufferCollection* buffers, const Piece& piece, Length remainder)
    {
        const LineStarts* starts = &buffers->buffer_at(piece.index)->line_starts;
        auto start_offset = rep(starts->starts[rep(piece.first.line)]) + rep(piece.first.column);
        auto offset = start_offset + rep(remainder);

        // Binary search for 'offset' between start and ending offset.
        auto low = rep(piece.first.line);
        auto high = rep(piece.last.line);

        size_t mid = 0;
        size_t mid_start = 0;
        size_t mid_stop = 0;

        while (low <= high)
        {
            mid = low + ((high - low) / 2);
            mid_start = rep(starts->starts[mid]);

            if (mid == high)
                break;
            mid_stop = rep(starts->starts[mid + 1]);

            if (offset < mid_start)
            {
                high = mid - 1;
            }
            else if (offset >= mid_stop)
            {
                low = mid + 1;
            }
            else
            {
                break;
            }
        }

        return { .line = Line{ mid },
                    .column = Column{ offset - mid_start } };
    }

    void Tree::build_tree()
    {
        take_buffer_ref(&buffers);
        // In order to maintain the invariant of other buffers, the mod_buffer needs a single line-start of 0.
        append_mut_buf_start(&buffers, {});
        last_insert = { };

        const auto buf_count = buffers.orig_buffers.count;
        
        size_t leafCount = 0;
        Arena::Temp scratch = Arena::scratch_begin(Arena::no_conflicts);
        NodeData* leafNodes = Arena::push_array<NodeData>(scratch.arena, buf_count);

        for (size_t i = 0; i < buf_count; ++i)
        {
            const auto& buf = buffers.orig_buffers.buffers[i];
            assert(buf.line_starts.count>0);
            // If this immutable buffer is empty, we can avoid creating a piece for it altogether.
            if (buf.buffer.size == 0)
                continue;
            auto last_line = Line{ buf.line_starts.count - 1 };
            // Create a new node that spans this buffer and retains an index to it.
            // Insert the node into the balanced tree.
            Piece piece {
                .index = BufferIndex{ i },
                .first = { .line = Line{ 0 }, .column = Column{ 0 } },
                .last = { .line = last_line, .column = Column{ buf.buffer.size - rep(buf.line_starts.starts[rep(last_line)]) } },
                .length = Length{ buf.buffer.size },
                // Note: the number of newlines
                .newline_count = LFCount{ rep(last_line) }
            };
            leafNodes[leafCount++]={piece};
        }
        root = root.construct_from(buffers.rb_tree_blk, leafNodes, leafCount);
        
        Arena::scratch_end(scratch);
        compute_buffer_meta();
    }

    void Tree::remove(CharOffset offset, Length count, SuppressHistory suppress_history)
    {
        // Rule out the obvious noop.
        if (rep(count) == 0 or root.is_empty())
            return;
        if (is_no(suppress_history))
        {
            append_undo(root, offset);
        }
        internal_remove(offset, count);
    }

    void Tree::line_end_crlf(CharOffset* offset, const BufferCollection* buffers, StorageTree::NodePtr node, Line line)
    {
        
        assert(line != Line::IndexBeginning);
        auto line_index = rep(retract(line));

        StorageTree::NodePtr n = (node);
        StorageTree::NodePtr prevNode = nullptr;
        
        while(!n->isLeaf())
        {
            StorageTree::InternalNodePtr in = reinterpret_cast<StorageTree::InternalNodePtr>(&node);
            StorageTree::NodeVector children = (&in->children[0]);
            int i;
            for(i = 0; i < n->childCount;i++)
            {
                if(line_index <= rep(n->lineFeeds[i]))
                {
                    if(i>0)
                    {
                        line_index -= rep(n->lineFeeds[i-1]);
                        *offset = *offset + (n->offsets[i-1]);
                    }
                    n = children[i];
                    if(i>0)
                    {
                        prevNode = children[i];
                    }
                    else
                    {
                        if(prevNode!=nullptr)
                        {
                            
                            StorageTree::InternalNodePtr pn = reinterpret_cast<StorageTree::InternalNodePtr>(&node);
                            const StorageTree::NodeVector prevChildren = (&pn->children[0]);
                            prevNode = prevChildren[prevNode->childCount-1];
                        }
                    }
                    i=0;
                    break;
                }
                //line_index -= rep(n->lineFeeds[i]);
                //*offset = *offset + (n->offsets[i]);
                
            }
            if(i==n->childCount){
                i--;
                line_index -= rep(n->lineFeeds[i-1]);
                *offset = *offset + (n->offsets[i-1]);

                n = children[i];
            }
        }
        StorageTree::LeafNodePtr ln = reinterpret_cast<StorageTree::LeafNodePtr>(n);

        NodeData* children = (&ln->children[0]);
        int i;
        for(i = 0; i < n->childCount;i++)
        {
            if(line_index <= rep(children[i].piece.newline_count))
            {
                break;
            }
            line_index -= rep(children[i].piece.newline_count);
            *offset = *offset + children[i].piece.length;
        }
        if(i==n->childCount){
            return;
        }

        Piece piece = children[i].piece;
        Piece prevPiece;
        if(i>0)
        {
            prevPiece = children[i-1].piece;
        }
        else
        {
            if(prevNode != nullptr)
            {
                
                StorageTree::LeafNodePtr pln = reinterpret_cast<StorageTree::LeafNodePtr>(prevNode);

                NodeData* prevChildren = (&pln->children[0]);
                prevPiece = prevChildren[prevNode->childCount-1].piece;
            }
        }
        Length len  {0};
        if (line_index != 0)
        {
            len = len + accumulate_value_no_lf(buffers, piece, Line{ line_index - 1 });
            if(len == Length{})
            {
                if(prevPiece.length != Length{})
                    assert(false);
            }
            else
            {
                auto* charbuffer = buffers->buffer_at(piece.index);
                auto buf_offset = buffers->buffer_offset(piece.index, piece.first)+len;
                const char* p = charbuffer->buffer.str + rep(buf_offset);
                if(*p == '\n')
                {
                    p--;
                    
                    if(*p == '\r')
                    {
                        p--;
                        len = retract(len);
                    }
                }
            }
        }
        *offset = *offset + len;
    }

    LineRange Tree::get_line_range(Line line) const
    {
        LineRange range{ };
        line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        line_start<&Tree::accumulate_value_no_lf>(&range.last, &buffers, root, extend(line));
        return range;
    }
    LineRange Tree::get_line_range_crlf(Line line) const
    {
        LineRange range{ };
        line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        line_end_crlf(&range.last, &buffers, (root.root_ptr()), extend(line));
        return range;
    }

    LineRange Tree::get_line_range_with_newline(Line line) const
    {
        LineRange range{ };
        line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        line_start<&Tree::accumulate_value>(&range.last, &buffers, root, extend(line));
        return range;
    }
    OwningSnapshot* Tree::owning_snap(Arena::Arena* arena) const
    {
        uint8_t* blob = Arena::push_array_aligned<uint8_t>(arena, sizeof(OwningSnapshot), Arena::Alignment{ alignof(OwningSnapshot) });
        OwningSnapshot* snap = new (blob) OwningSnapshot{ arena, this };
        return snap;
    }
    ReferenceSnapshot Tree::ref_snap() const
    {
        return ReferenceSnapshot{ this };
    }
    
    char Tree::at(CharOffset offset) const
    {
        auto result = node_at(&buffers, root, offset);
        if (result.node == nullptr)
            return '\0';
        auto* buffer = buffers.buffer_at(result.node->piece.index);
        auto buf_offset = buffers.buffer_offset(result.node->piece.index, result.node->piece.first);
        const char* p = buffer->buffer.str + rep(buf_offset) + rep(result.remainder);
        return *p;
    }
    
    Line Tree::line_at(CharOffset offset) const
    {
        if (is_empty())
            return Line::Beginning;
        auto result = node_at(&buffers, root, offset);
        return result.line;
    }

    LFCount Tree::line_feed_count(const BufferCollection* buffers, BufferIndex index, const BufferCursor& start, const BufferCursor& end)
    {
        // If the end position is the beginning of a new line, then we can just return the difference in lines.
        if (rep(end.column) == 0)
            return LFCount{ rep(retract(end.line, rep(start.line))) };
        auto& starts = buffers->buffer_at(index)->line_starts;
        // It means, there is no LF after end.
        if (end.line == Line{ starts.count - 1})
            return LFCount{ rep(retract(end.line, rep(start.line))) };
        // Due to the check above, we know that there's at least one more line after 'end.line'.
        auto next_start_offset = starts.starts[rep(extend(end.line))];
        auto end_offset = rep(starts.starts[rep(end.line)]) + rep(end.column);
        // There are more than 1 character after end, which means it can't be LF.
        if (rep(next_start_offset) > end_offset + 1)
            return LFCount{ rep(retract(end.line, rep(start.line))) };
        // This must be the case.  next_start_offset is a line down, so it is
        // not possible for end_offset to be < it at this point.
        assert(end_offset + 1 == rep(next_start_offset));
        return LFCount{ rep(retract(end.line, rep(start.line))) };
    }

    String8 Tree::get_line_content(Arena::Arena* arena, Line line) const
    {
        
        // Reset the buffer.
        
        if (line == Line::IndexBeginning)
            return {};
        
        return assemble_line(arena, &buffers, meta, root, line);
        
    }


    // Direct history manipulation.
    void Tree::commit_head(CharOffset offset)
    {
        append_undo(root, offset);
    }

    StorageTree Tree::head() const
    {
        return root;
    }
    void Tree::snap_to(const StorageTree& new_root)
    {
        root = new_root;
        compute_buffer_meta();
    }

    String8 Tree::assemble_line(Arena::Arena* arena, const BufferCollection* buffers, const BufferMeta& meta, const StorageTree& node, Line line) 
    {
        String8 res = str8_empty;
        if(node.is_empty())
            return res;
        Arena::Temp scratch = Arena::scratch_begin({&arena, 1});
        CharOffset line_offset{ };
        line_start<&Tree::accumulate_value>(&line_offset, buffers, node, line);
        TreeWalker walker{ scratch.arena, buffers, meta, node, line_offset };
        String8List serial_lst{};
        str8_serial_begin(scratch.arena, &serial_lst);
        while (not walker.exhausted())
        {
            char c = walker.next();
            if (c == '\n')
                break;
            str8_serial_push_char(scratch.arena, &serial_lst, c);
        }
        res = str8_serial_end(arena, serial_lst);
        Arena::scratch_end(scratch);
        return res;
    }

    void print_piece(const RatchetPieceTree::Piece& piece, const RatchetPieceTree::Tree* tree, int level)
    {
        const char* levels = "|||||||||||||||||||||||||||||||";
        printf("%.*s  :idx{%zd}, first{l{%zd}, c{%zd}}, last{l{%zd}, c{%zd}}, len{%zd}, lf{%zd}\n",
                level, levels,
                rep(piece.index), rep(piece.first.line), rep(piece.first.column),
                                  rep(piece.last.line), rep(piece.last.column),
                    rep(piece.length), rep(piece.newline_count));
        auto* buffer = tree->buffers.buffer_at(piece.index);
        auto offset = tree->buffers.buffer_offset(piece.index, piece.first);
        printf("%.*sPiece content: %.*s\n", level, levels, static_cast<int>(piece.length), buffer->buffer.str + rep(offset));
    }
    void print_tree(const RatchetPieceTree::Tree& tree)
    {
        ::print_tree((tree.root.root_ptr()), &tree);
    }

    Piece Tree::build_piece(String8 txt)
    {
        auto start_offset = buffers.mod_buffer.buffer.size;
        auto scratch = Arena::scratch_begin(Arena::no_conflicts);
        LineStarts scratch_starts{};
        populate_line_starts(scratch.arena, &scratch_starts, txt);
        auto start = last_insert;
        // TODO: Handle CRLF (where the new buffer starts with LF and the end of our buffer ends with CR).
        // Offset the new starts relative to the existing buffer.
        for EachIndex(i, scratch_starts.count)
        {
            LineStart* new_start = &scratch_starts.starts[i];
            *new_start = extend(*new_start, start_offset);
        }
        // Append new starts.
        auto new_starts_end = scratch_starts.count;
        // Note: we can drop the first start because the algorithm always adds an empty start.
        for (size_t i = 1; i < new_starts_end; ++i)
        {
            append_mut_buf_start(&buffers, scratch_starts.starts[i]);
        }
        auto old_size = buffers.mod_buffer.buffer.size;
        grow_mut_buf(&buffers, txt.size);
        char* insert_at = buffers.mod_buffer.buffer.str + old_size;
        memcpy(insert_at, txt.str, txt.size);
        Arena::scratch_end(scratch);

        // Build the new piece for the inserted buffer.
        auto end_offset = buffers.mod_buffer.buffer.size;
        auto end_index = buffers.mod_buffer.line_starts.count - 1;
        auto end_col = end_offset - rep(buffers.mod_buffer.line_starts.starts[end_index]);
        BufferCursor end_pos = { .line = Line{ end_index }, .column = Column{ end_col } };
        Piece piece = { .index = BufferIndex::ModBuf,
                        .first = start,
                        .last = end_pos,
                        .length = Length{ end_offset - start_offset },
                        .newline_count = line_feed_count(&buffers, BufferIndex::ModBuf, start, end_pos) };
        // Update the last insertion.
        last_insert = end_pos;
        return piece;
    }

    NodePosition Tree::node_at(const BufferCollection* buffers, StorageTree tree, CharOffset off)
    {
        if (tree.is_empty())
            return { };
        Offset node_start_offset {};
        Line newline_count {};
        StorageTree::NodePtr node = tree.root_ptr();
        if(!node)
        {
            NodePosition result { };
            return result;
        }
        
        while(!node->isLeaf())
        {
            StorageTree::InternalNodePtr in = reinterpret_cast<StorageTree::InternalNodePtr>(node);
            StorageTree::NodeVector children = (&in->children[0]);
            int i = 0;
            for(; i<node->childCount; i++)
            {
                if(rep(distance(Offset{node_start_offset}, off)) < rep(node->offsets[i]))
                {
                    if(i>0)
                    {
                        node_start_offset = node_start_offset + node->offsets[i-1];
                        newline_count = extend(newline_count, rep(node->lineFeeds[i-1]));
                    }
                    node = children[i];
                    break;
                }
                //node_start_offset += rep(node->offsets[i]);
                //newline_count += rep(node->lineFeeds[i]);
            }
            if(i==node->childCount){
                if(node->childCount>1)
                {
                    node_start_offset = node_start_offset+ node->offsets[node->childCount-2];
                    newline_count = extend(newline_count, rep(node->lineFeeds[node->childCount-2]));
                }
                node = children[node->childCount-1];
            }
        }
        StorageTree::LeafNodePtr ln = reinterpret_cast<StorageTree::LeafNodePtr>(node);
            
        auto& children = (ln->children);
        int i = 0;
        for(; i<node->childCount; i++)
        {
            if(rep(distance(Offset{node_start_offset}, off)) < rep(node->offsets[i]))
            {
                if(i>0)
                {
                    node_start_offset = node_start_offset + node->offsets[i-1];
                    newline_count = extend(newline_count, rep(node->lineFeeds[i-1]));
                }
                break;
            }
            
        }
        if(i>=node->childCount)
        {
            i--;
            if(i>0)
            {
                node_start_offset = node_start_offset + node->offsets[i-1];
                newline_count = extend(newline_count, rep(node->lineFeeds[i-1]));
            }
        }
        Piece result_piece =  children[i].piece;
        //node = children[i].get();

        // Now we find the line within this piece.
        auto remainder = distance (Offset{node_start_offset}, off);
        auto pos = buffer_position(buffers, result_piece, remainder);
        // Note: since buffer_position will return us a newline relative to the buffer itself, we need
        // to retract it by the starting line of the piece to get the real difference.
        newline_count = extend(newline_count, rep(retract(pos.line, rep(result_piece.first.line))));

        NodePosition result { .node = &children[i],
                    .remainder = remainder,
                    .start_offset = CharOffset{ node_start_offset },
                    .line = Line{ extend(newline_count) } };
        return result;
    }
    void Tree::compute_buffer_meta()
    {
        ::RatchetPieceTree::compute_buffer_meta(&meta, root);
    }


    namespace
    {
        void push_ur_node(Arena::Arena* arena, UndoRedoEntry** free_list, UndoRedoList* lst, const StorageTree& root, CharOffset op_offset)
        {
            UndoRedoEntry* entry = nullptr;
            if (*free_list != nullptr)
            {
                entry = *free_list;
                SLLStackPop(*free_list);
            }
            else
            {
                // Since the RB tree has a non-trivial dtor, we must allocate this as a blob.
                uint8_t* blob = Arena::push_array_no_zero<uint8_t>(arena, sizeof(UndoRedoEntry));
                entry = new (blob) UndoRedoEntry{ .root = StorageTree{} };
            }
            zero_bytes(entry);
            entry->root = root.dup();
            entry->op_offset = op_offset;
            SLLQueuePushFront(lst->first, lst->last, entry);
            ++lst->count;
        }

        UndoRedoEntry* pop_ur_node(UndoRedoList* lst)
        {
            UndoRedoEntry* entry = lst->first;
            SLLQueuePop(lst->first, lst->last);
            --lst->count;
            entry->next = nullptr;
            // Now we invoke the dtor to ensure the RB tree signals the release.
            entry->~UndoRedoEntry();
            return entry;
        }
    } // namespace [anon]


    void Tree::append_undo(const StorageTree& old_root, CharOffset op_offset)
    {
        // Can't redo if we're creating a new undo entry.
        if (redo_stack.count != 0)
        {
            do
            {
                UndoRedoEntry* e = pop_ur_node(&redo_stack);
                SLLStackPush(free_undo_list, e);
            } while (redo_stack.first != nullptr);
        }
        push_ur_node(buffers.undo_redo_stack_arena, &free_undo_list, &undo_stack, old_root, op_offset);
    }

    UndoRedoResult Tree::try_undo(CharOffset op_offset)
    {
        if (undo_stack.count == 0)
            return { .success = false, .op_offset = CharOffset{ } };
        push_ur_node(buffers.undo_redo_stack_arena, &free_undo_list, &redo_stack, root, op_offset);
        auto [nx, node, undo_offset] = static_cast<UndoRedoEntry&&>(*undo_stack.first);
        root = node.dup();
        UndoRedoEntry* e = pop_ur_node(&undo_stack);
        SLLStackPush(free_undo_list, e);
        compute_buffer_meta();
        return { .success = true, .op_offset = undo_offset };
    }

    UndoRedoResult Tree::try_redo(CharOffset op_offset)
    {
        if (redo_stack.count == 0)
            return { .success = false, .op_offset = CharOffset{ } };
        push_ur_node(buffers.undo_redo_stack_arena, &free_undo_list, &undo_stack, root, op_offset);
        auto [nx, node, redo_offset] = static_cast<UndoRedoEntry&&>(*redo_stack.first);
        root = node.dup();
        UndoRedoEntry* e = pop_ur_node(&redo_stack);
        SLLStackPush(free_undo_list, e);
        compute_buffer_meta();
        return { .success = true, .op_offset = redo_offset };
    }

    void Tree::internal_insert(CharOffset offset, String8 txt)
    {
        assert(txt.size>0);
        ScopeGuard guard{ [&] {
            compute_buffer_meta();
#ifdef TEXTBUF_DEBUG
            satisfies_btree_invariant(root);
#endif
        } };
        end_last_insert = extend(offset, txt.size);

        StorageTree old_tree = root;
        if (root.is_empty())
        {
            auto piece = build_piece(txt);
            root = root.insert(&buffers, { piece }, CharOffset{ 0 });
            
        }
        else
        {
            auto piece = build_piece(txt);
            root = root.insert(&buffers, { piece }, offset);
        }
        //FIXME (ratchetfreak): release tree
    }

    void Tree::internal_remove(CharOffset offset, Length count)
    {
        assert(rep(count) != 0 and not root.is_empty());
        ScopeGuard guard{ [&] {
            compute_buffer_meta();
#ifdef TEXTBUF_DEBUG
            satisfies_btree_invariant(root);
#endif
        } };
        
        StorageTree old_tree = root;
        root = root.remove(&buffers, offset, count);
        //FIXME (ratchetfreak): release tree
    }

    // Fetches the length of the piece starting from the first line to 'index' or to the end of
    // the piece.
    Length Tree::accumulate_value_no_lf(const BufferCollection* buffers, const Piece& piece, Line index)
    {
        auto* buffer = buffers->buffer_at(piece.index);
        auto& line_starts = buffer->line_starts;
        // Extend it so we can capture the entire line content including newline.
        auto expected_start = extend(piece.first.line, rep(index) + 1);
        auto first = rep(line_starts.starts[rep(piece.first.line)]) + rep(piece.first.column);
        if (expected_start > piece.last.line)
        {
            auto last = rep(line_starts.starts[rep(piece.last.line)]) + rep(piece.last.column);
            if (last == first)
                return Length{ };
            if (buffer->buffer.str[last - 1] == '\n')
                return Length{ last - 1 - first };
            return Length{ last - first };
        }
        auto last = rep(line_starts.starts[rep(expected_start)]);
        if (last == first)
            return Length{ };
        if (buffer->buffer.str[last - 1] == '\n')
            return Length{ last - 1 - first };
        return Length{ last - first };
    }

    template <Tree::Accumulator accumulate>
    void Tree::line_start(CharOffset* offset, const BufferCollection* buffers, const RatchetPieceTree::StorageTree& node, Line line)
    {
        if (node.is_empty())
            return;
        assert(line != Line::IndexBeginning);
        auto line_index = rep(retract(line));

        StorageTree::NodePtr n = node.root_ptr();
        
        while(!n->isLeaf())
        {
            StorageTree::InternalNodePtr in = reinterpret_cast<StorageTree::InternalNodePtr>(n);
            StorageTree::NodeVector children = (in->children);
            int i;
            for(i = 0; i < n->childCount;i++)
            {
                if(line_index <= rep(n->lineFeeds[i]))
                {
                    if(i>0)
                    {
                        line_index -= rep(n->lineFeeds[i-1]);
                        *offset = *offset + (n->offsets[i-1]);
                    }
                    n = children[i];
                    i=0;
                    break;
                }
                //line_index -= rep(n->lineFeeds[i]);
                //*offset = *offset + (n->offsets[i]);
                
            }
            if(i==n->childCount){
                i--;
                line_index -= rep(n->lineFeeds[i-1]);
                *offset = *offset + (n->offsets[i-1]);

                n = children[i];
            }
        }
        StorageTree::LeafNodePtr ln = reinterpret_cast<StorageTree::LeafNodePtr>(n);
            
        NodeData* children = (&ln->children[0]);
        int i;
        for(i = 0; i < n->childCount;i++)
        {
            if(line_index <= rep(children[i].piece.newline_count))
            {
                break;
            }
            line_index -= rep(children[i].piece.newline_count);
            *offset = *offset + children[i].piece.length;
        }
        if(i==n->childCount){
            return;
        }

        Piece piece = children[i].piece;
        Length len  {0};
        if (line_index != 0)
        {
            len = len + (*accumulate)(buffers, piece, Line{ line_index - 1 });
        }
        *offset = *offset + len;
    }

    // Fetches the length of the piece starting from the first line to 'index' or to the end of
    // the piece.
    Length Tree::accumulate_value(const BufferCollection* buffers, const Piece& piece, Line index)
    {
        auto* buffer = buffers->buffer_at(piece.index);
        auto& line_starts = buffer->line_starts;
        // Extend it so we can capture the entire line content including newline.
        auto expected_start = extend(piece.first.line, rep(index) + 1);
        auto first = rep(line_starts.starts[rep(piece.first.line)]) + rep(piece.first.column);
        if (expected_start > piece.last.line)
        {
            auto last = rep(line_starts.starts[rep(piece.last.line)]) + rep(piece.last.column);
            return Length{ last - first };
        }
        auto last = rep(line_starts.starts[rep(expected_start)]);
        return Length{ last - first };
    }


    namespace
    {
        void tree_builder_push_immut_buf_node(Arena::Arena* arena, TreeBuilder* builder, String8 txt)
        {
            ImmutableBufferNode* node = Arena::push_array<ImmutableBufferNode>(arena, 1);
            String8 persisted_txt = str8_copy(builder->immutable_buf_arena, txt);
            LineStarts starts{};
            populate_line_starts(builder->immutable_buf_arena, &starts, txt);
            node->buffer = CharBuffer{ .buffer = persisted_txt, .line_starts = starts };
            SLLQueuePush(builder->buffers.first, builder->buffers.last, node);
            ++builder->buffers.count;
        }
    } // namespace [anon]

    TreeBuilder tree_builder_start(Arena::Arena* buffer_arena)
    {
        Arena::Arena** buffer_arenas = Arena::push_array<Arena::Arena*>(buffer_arena, 4);
        buffer_arenas[0] = buffer_arena;
        buffer_arenas[1] = Arena::alloc(Arena::default_params);
        buffer_arenas[2] = Arena::alloc(Arena::default_params);
        buffer_arenas[3] = Arena::alloc(Arena::default_params);
        TreeBuilder result{
            .immutable_buf_arena = buffer_arenas[0],
            .undo_redo_stack_arena = buffer_arenas[1],
            .mut_buf_starts_arena = buffer_arenas[2],
            .mut_buf_arena = buffer_arenas[3],
            .buffers = {},
        };
        return result;
    }

    void tree_builder_accept(Arena::Arena* arena, TreeBuilder* builder, String8 txt)
    {
        tree_builder_push_immut_buf_node(arena, builder, txt);
    }

    Tree* tree_builder_finish(TreeBuilder* builder)
    {
        // We're going to join the list and construct the basic buffer object.
        ImmutableBufferArray immut_buffers{};
        CharBuffer* buffers_arr = Arena::push_array<CharBuffer>(builder->immutable_buf_arena, builder->buffers.count);
        uint64_t i = 0;
        for EachNode(n, builder->buffers.first)
        {
            buffers_arr[i++] = n->buffer;
        }
        immut_buffers.buffers = buffers_arr;
        immut_buffers.count = builder->buffers.count;
        // Allocate the atomic count.
        immut_buffers.ref_count = Arena::push_array<uint64_t>(builder->immutable_buf_arena, 1);

        // Allocate the red-black tree block.
        BTreeBlock* rb_tree_blk = Arena::push_array<BTreeBlock>(builder->immutable_buf_arena, 1);
        rb_tree_blk->free_list.leaf.head = nil_node();
        rb_tree_blk->free_list.internal.head = nil_node();
        rb_tree_blk->alloc_arena = builder->immutable_buf_arena;

        BufferCollection buffers{
            .immutable_buf_arena = builder->immutable_buf_arena,
            .undo_redo_stack_arena = builder->undo_redo_stack_arena,
            .mut_buf_starts_arena = builder->mut_buf_starts_arena,
            .mut_buf_arena = builder->mut_buf_arena,
            .orig_buffers = immut_buffers,
            .mod_buffer = {},
            .rb_tree_blk = rb_tree_blk,
        };
        // Allocate the base of the mod buffer.
        // Note: Because we're wanting to build an endlessly growing array, we need to allocate the buffer ourselves and aligned
        // to 1 byte.
        // Note: We give the mod buffer string a +1 for the null.
        buffers.mod_buffer.buffer.str = Arena::push_array_no_zero_aligned<char>(buffers.mut_buf_arena, 1, Arena::Alignment{ alignof(char) });
        buffers.mod_buffer.buffer.str[0] = 0;
        buffers.mod_buffer.line_starts.starts = Arena::push_array<LineStart>(buffers.mut_buf_starts_arena, 0);
        uint8_t* blob = Arena::push_array_aligned<uint8_t>(buffers.immutable_buf_arena, sizeof(Tree), Arena::Alignment{ alignof(Tree) });
        Tree* tree = new (blob) Tree{ buffers };
        return tree;
    }

    Tree* tree_builder_empty(Arena::Arena* buffer_arena)
    {
        Arena::Arena** buffer_arenas = Arena::push_array<Arena::Arena*>(buffer_arena, 4);
        buffer_arenas[0] = buffer_arena;
        buffer_arenas[1] = Arena::alloc(Arena::default_params);
        buffer_arenas[2] = Arena::alloc(Arena::default_params);
        buffer_arenas[3] = Arena::alloc(Arena::default_params);
        TreeBuilder result{
            .immutable_buf_arena = buffer_arenas[0],
            .undo_redo_stack_arena = buffer_arenas[1],
            .mut_buf_starts_arena = buffer_arenas[2],
            .mut_buf_arena = buffer_arenas[3],
            .buffers = {},
        };
        return tree_builder_finish(&result);
    }

    void release_tree(Tree* tree)
    {
        BufferCollection buffers = tree->buffer_collection_no_ref();
        tree->~Tree();
        // This might also clear the arena.
        dec_buffer_ref(&buffers);
    }


    OwningSnapshot::OwningSnapshot(Arena::Arena* mut_buf_arena, const Tree* tree):
        root{ tree->root },
        meta{ tree->meta },
        buffers{ tree->buffers } 
    {
        // Copy the mut buf and place it in the buffers.  Since deletion only erases the
        // arenas, we can overwrite the mut buf and its line endings.
        LineStarts starts{};
        starts.count = buffers.mod_buffer.line_starts.count;
        starts.starts = Arena::push_array_no_zero<LineStart>(mut_buf_arena, starts.count);
        memcpy(starts.starts, buffers.mod_buffer.line_starts.starts, sizeof(LineStart) * starts.count);
        String8 buf = str8_copy(mut_buf_arena, buffers.mod_buffer.buffer);
        buffers.mod_buffer.line_starts = starts;
        buffers.mod_buffer.buffer = buf;
    }

    OwningSnapshot::OwningSnapshot(Arena::Arena* mut_buf_arena, const Tree* tree, const StorageTree& dt):
        OwningSnapshot{ mut_buf_arena, tree }
    {
        root = dt.dup();
        // Compute the buffer meta for 'dt'.
        compute_buffer_meta(&meta, dt);
    }


    String8 OwningSnapshot::get_line_content(Arena::Arena* arena, Line line) const
    {
        String8 result = str8_empty;
        if (line == Line::IndexBeginning)
            return result;
        result = Tree::assemble_line(arena, &buffers, meta, root, line);
        return result;
    }
    
    Line OwningSnapshot::line_at(CharOffset offset) const
    {
        if (is_empty())
            return Line::Beginning;
        auto result = Tree::node_at(&buffers, root, offset);
        return result.line;
    }

    LineRange OwningSnapshot::get_line_range(Line line) const
    {
        LineRange range{ };
        Tree::line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        Tree::line_start<&Tree::accumulate_value_no_lf>(&range.last, &buffers, root, extend(line));
        return range;
    }
    LineRange OwningSnapshot::get_line_range_crlf(Line line) const
    {
        LineRange range{ };
        Tree::line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        Tree::line_end_crlf(&range.last, &buffers, root.root_ptr(), extend(line));
        return range;
    }

    LineRange OwningSnapshot::get_line_range_with_newline(Line line) const
    {
        LineRange range{ };
        Tree::line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        Tree::line_start<&Tree::accumulate_value>(&range.last, &buffers, root, extend(line));
        return range;
    }

    BufferCollection OwningSnapshot::buffer_collection_no_ref() const
    {
        return buffers;
    }

    void release_owning_snap(OwningSnapshot* snap)
    {
        BufferCollection buffers = snap->buffer_collection_no_ref();
        snap->~OwningSnapshot();
        // Now we can decrement the buffers.
        dec_buffer_ref(&buffers);
    }

    ReferenceSnapshot::ReferenceSnapshot(const Tree* tree):
        root{ tree->root },
        meta{ tree->meta },
        buffers{ take_buffer_ref(&tree->buffers) } { }

    ReferenceSnapshot::ReferenceSnapshot(const Tree* tree, const StorageTree& dt):
        root{ dt },
        meta{ tree->meta },
        buffers{ take_buffer_ref(&tree->buffers) }
    {
        // Compute the buffer meta for 'dt'.
        compute_buffer_meta(&meta, dt);
    }
    
    ReferenceSnapshot::ReferenceSnapshot(const ReferenceSnapshot& other):
        root{ other.root.dup() },
        meta{ other.meta },
        buffers{ take_buffer_ref(&other.buffers) }
    {
    }

    ReferenceSnapshot& ReferenceSnapshot::operator=(const ReferenceSnapshot& other)
    {
        root = other.root.dup();
        meta = other.meta;
        buffers = take_buffer_ref(&other.buffers);
        return *this;
    }

    ReferenceSnapshot::~ReferenceSnapshot()
    {
        // Reset the root since releasing the buffer below could cause the underlying nodes to be destroyed as well.
        root = StorageTree{};
        dec_buffer_ref(&buffers);
    }

    String8 ReferenceSnapshot::get_line_content(Arena::Arena* arena, Line line) const
    {
        String8 result = str8_empty;
        if (line == Line::IndexBeginning)
            return result;
        result = Tree::assemble_line(arena, &buffers, meta, root, line);
        return result;
    }
    
    namespace
    {
        template <typename TreeT>
        [[nodiscard]] IncompleteCRLF trim_crlf(Arena::Arena* arena, String8* buf, TreeT* tree, CharOffset line_offset)
        {
            auto scratch = Arena::scratch_begin({ &arena, 1 });
            IncompleteCRLF result = IncompleteCRLF::No;
            TreeWalker walker{ scratch.arena, tree, line_offset };
            String8List serial_lst{};
            str8_serial_begin(scratch.arena, &serial_lst);
            String8 prev_str = str8_empty;
            char prev_char = 0;
            char c = 0;
            while (not walker.exhausted())
            {
                c = walker.next();
                if (c == '\n')
                {
                    if (prev_char == '\r')
                    {
                        result = IncompleteCRLF::No;
                        break;
                    }
                    result = IncompleteCRLF::Yes;
                    break;
                }
                str8_serial_push_str8(scratch.arena, &serial_lst, prev_str);
                prev_char = c;
                prev_str = str8(&prev_char, 1);
            }
            // If the prev_char was anything other than a '\r', we want to add it to the buffer.  This does not,
            // however, imply that CRLF endings are incomplete.  This might simply the the last line of the buffer.
            // Note: The prev_char will only be valid if the string was also set.
            if (prev_str.size != 0 and prev_char != '\r')
            {
                str8_serial_push_char(scratch.arena, &serial_lst, prev_char);
            }
            // End of the buffer is not an incomplete CRLF.
            *buf = str8_serial_end(arena, serial_lst);
            Arena::scratch_end(scratch);
            return result;
        }
    } // namespace [anon]

    IncompleteCRLF Tree::get_line_content_crlf(Arena::Arena* arena, String8* buf, Line line) const
    {
        *buf = str8_empty;
        if (line == Line::IndexBeginning)
            return IncompleteCRLF::No;
        if (root.is_empty())
            return IncompleteCRLF::No;
        // Trying this new logic for now.
        CharOffset line_offset{ };
        line_start<&Tree::accumulate_value>(&line_offset, &buffers, root, line);
        return trim_crlf(arena, buf, this, line_offset);
    }
    
    Line ReferenceSnapshot::line_at(CharOffset offset) const
    {
        if (is_empty())
            return Line::Beginning;
        auto result = Tree::node_at(&buffers, root, offset);
        return result.line;
    }

    LineRange ReferenceSnapshot::get_line_range(Line line) const
    {
        LineRange range{ };
        Tree::line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        Tree::line_start<&Tree::accumulate_value_no_lf>(&range.last, &buffers, root, extend(line));
        return range;
    }
    LineRange ReferenceSnapshot::get_line_range_crlf(Line line) const
    {
        LineRange range{ };
        Tree::line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        Tree::line_end_crlf(&range.last, &buffers, root.root_ptr(), extend(line));
        return range;
    }

    LineRange ReferenceSnapshot::get_line_range_with_newline(Line line) const
    {
        LineRange range{ };
        Tree::line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        Tree::line_start<&Tree::accumulate_value>(&range.last, &buffers, root, extend(line));
        return range;
    }

    TreeWalker::TreeWalker(Arena::Arena* arena, const Tree* tree, CharOffset offset):
        buffers{ &tree->buffers },
        root{ tree->root },
        meta{ tree->meta },
        stack{ nullptr },
        stackCount{ 1 },
        total_offset{ offset }
    {
        stack = Arena::push_array<StackEntry>(arena, root.depth());
        fast_forward_to(offset);
    }

    TreeWalker::TreeWalker(Arena::Arena* arena, const OwningSnapshot* snap, CharOffset offset):
        buffers{ &snap->buffers },
        root{ snap->root },
        meta{ snap->meta },
        stack{ nullptr },
        stackCount{ 1 },
        total_offset{ offset }
    {
        stack = Arena::push_array<StackEntry>(arena, root.depth());
        fast_forward_to(offset);
    }

    TreeWalker::TreeWalker(Arena::Arena* arena, const ReferenceSnapshot* snap, CharOffset offset):
        buffers{ &snap->buffers },
        root{ snap->root },
        meta{ snap->meta },
        stack{ nullptr },
        stackCount{ 1 },
        total_offset{ offset }
    {
        stack = Arena::push_array<StackEntry>(arena, root.depth());
        fast_forward_to(offset);
    }


    TreeWalker::TreeWalker(Arena::Arena* arena, const BufferCollection* buffers, const BufferMeta& meta, const StorageTree& root, CharOffset offset):
        buffers{ buffers },
        root{ root.dup() },
        meta{ meta },
        total_offset{ offset }
    {
        // Populate initial stack state.
        stack = Arena::push_array<StackEntry>(arena, root.depth());
        fast_forward_to(offset);
    }


    char TreeWalker::next()
    {
        if (first_ptr == last_ptr)
        {
            populate_ptrs();
            // If this is exhausted, we're done.
            if (exhausted())
                return '\0';
            // Catchall.
            if (first_ptr == last_ptr)
                return next();
        }
        total_offset = total_offset + Length{ 1 };
        auto result = *first_ptr++;
        if (first_ptr == last_ptr)
        {
            populate_ptrs();
            // If this is exhausted, we're done.
            
        }
        return result;
    }

    char TreeWalker::current() const
    {
        if (exhausted())
            return '\0';
        return *first_ptr;
    }

    bool TreeWalker::exhausted() const
    {
        if (stackCount==0)
            return true;
        // If we have not exhausted the pointers, we're still active.
        if (first_ptr != last_ptr)
            return false;
        // If there's more than one entry on the stack, we're still active.
        //if (stack.size() > 1)
        //    return false;
        for(int i = 0; i < stackCount; i++)
        {
            auto index = stack[i].index;
            auto childCount = stack[i].node->childCount;
            if(index  < childCount)
                return false;
        }

        return true;
    }

    void TreeWalker::seek(CharOffset offset)
    {
        stackCount = 0;
        if(!root.root_ptr())return;
        stack[stackCount++] = { root.root_ptr() };
        total_offset = offset;

        Length accumulated {0};
        if(rep(offset) >= rep(stack[stackCount-1].node->subTreeLength()))
        {
            stackCount = 0;
            return;
        }
        while(!stack[stackCount-1].node->isLeaf())
        {
            algo_mark(stack[stackCount-1].node->shared_from_this(), Collect);
            auto &stack_entry = stack[stackCount-1];
            StorageTree::InternalNodePtr in = reinterpret_cast<StorageTree::InternalNodePtr>(stack_entry.node);
            StorageTree::NodeVector children = (&in->children[0]);
            Length sublen {0};
            for(stack_entry.index = 0; stack_entry.index< stack_entry.node->childCount; stack_entry.index++)
            {
                auto subtreelen = stack_entry.node->offsets[stack_entry.index];
                if(rep(offset) < rep(subtreelen)+ rep(accumulated))
                {
                    stack[stackCount++] = {children[stack_entry.index++]};
                    accumulated = accumulated + sublen;
                    break;
                }
                algo_mark(children[stack_entry.index]->shared_from_this(), Traverse);
                sublen = subtreelen;
            }
        }
        StorageTree::LeafNodePtr ln = reinterpret_cast<StorageTree::LeafNodePtr>(stack[stackCount-1].node);
        NodeData* children = (&ln->children[0]);
        algo_mark(stack[stackCount-1].node->shared_from_this(), Collect);
        Length sublen {0};
        for(stack[stackCount-1].index = 0; stack[stackCount-1].index< stack[stackCount-1].node->childCount;stack[stackCount-1].index++)
        {
            auto subtreelen = stack[stackCount-1].node->offsets[stack[stackCount-1].index];
            if(rep(offset) < rep(subtreelen) + rep(accumulated))
            {
                accumulated = accumulated + sublen;
                break;
            }
            sublen = subtreelen;
        }
        if(stack[stackCount-1].index== stack[stackCount-1].node->childCount)
        {
            stackCount = 0;
            first_ptr=nullptr;
            last_ptr=nullptr;
            return;
        }
        auto& piece = children[stack[stackCount-1].index++].piece;
        auto* buffer = buffers->buffer_at(piece.index);
        auto first_offset = buffers->buffer_offset(piece.index, piece.first);
        auto last_offset = buffers->buffer_offset(piece.index, piece.last);
        first_ptr = buffer->buffer.str + rep(first_offset) + rep(offset) - rep(accumulated);
        last_ptr = buffer->buffer.str + rep(last_offset);
    }

    void TreeWalker::fast_forward_to(CharOffset offset)
    {
        seek(offset);
    }

    void TreeWalker::populate_ptrs()
    {
        if (exhausted())
            return;
        while (stack[stackCount-1].node->childCount == stack[stackCount-1].index)
        {
            stackCount--;
            if(stackCount == 0)return;
        }
        while(!stack[stackCount-1].node->isLeaf())
        {
            algo_mark(stack[stackCount-1].node->shared_from_this(), Traverse);
            
            StorageTree::InternalNodePtr in = reinterpret_cast<StorageTree::InternalNodePtr>(stack[stackCount-1].node);
            StorageTree::NodeVector children = (&in->children[0]);
            size_t childIndex = stack[stackCount-1].index++;
            stack[stackCount++] = {children[childIndex], 0};
        }
        algo_mark(stack[stackCount-1].node->shared_from_this(), Traverse);
        StorageTree::LeafNodePtr ln = reinterpret_cast<StorageTree::LeafNodePtr>(stack[stackCount-1].node);
        NodeData* leafs = (&ln->children[0]);
        
        auto& piece = leafs[stack[stackCount-1].index++].piece;
        auto* buffer = buffers->buffer_at(piece.index);
        auto first_offset = buffers->buffer_offset(piece.index, piece.first);
        auto last_offset = buffers->buffer_offset(piece.index, piece.last);
        first_ptr = buffer->buffer.str + rep(first_offset);
        last_ptr = buffer->buffer.str + rep(last_offset);
    }

    Length TreeWalker::remaining() const
    {
        return meta.total_content_length - distance(CharOffset{}, total_offset);
    }

    ReverseTreeWalker::ReverseTreeWalker(Arena::Arena* arena, const Tree* tree, CharOffset offset):
        buffers{ &tree->buffers },
        root{ tree->root },
        meta{ tree->meta },
        stack{ nullptr },
        stackCount{ 1 },
        total_offset{ offset }
    {
        stack = Arena::push_array<StackEntry>(arena, root.depth());
        fast_forward_to(offset);
    }

    ReverseTreeWalker::ReverseTreeWalker(Arena::Arena* arena, const OwningSnapshot* snap, CharOffset offset):
        buffers{ &snap->buffers },
        root{ snap->root },
        meta{ snap->meta },
        stack{ nullptr },
        stackCount{ 1 },
        total_offset{ offset }
    {
        stack = Arena::push_array<StackEntry>(arena, root.depth());
        fast_forward_to(offset);
    }

    ReverseTreeWalker::ReverseTreeWalker(Arena::Arena* arena, const ReferenceSnapshot* snap, CharOffset offset):
        buffers{ &snap->buffers },
        root{ snap->root },
        meta{ snap->meta },
        stack{ nullptr },
        stackCount{ 1 },
        total_offset{ offset }
    {
        stack = Arena::push_array<StackEntry>(arena, root.depth());
        fast_forward_to(offset);
    }
    char ReverseTreeWalker::next()
    {
        if (first_ptr == last_ptr)
        {
            populate_ptrs();
            // If this is exhausted, we're done.
            if (exhausted())
                return '\0';
            // Catchall.
            if (first_ptr == last_ptr)
                return next();
        }
        
        // Since CharOffset is unsigned, this will end up wrapping, both 'exhausted' and
        // 'remaining' will return 'true' and '0' respectively.
        total_offset = retract(total_offset);
        // A dereference is the pointer value _before_ this actual pointer, just like
        // STL reverse iterator models.
        auto result = *(--first_ptr);
        if (first_ptr == last_ptr)
        {
            populate_ptrs();
        }
        return result;
    }

    char ReverseTreeWalker::current() const
    {
        //    populate_ptrs();
            // If this is exhausted, we're done.
        if (exhausted())
            return '\0';

        return *(first_ptr-1);
    }

    Length ReverseTreeWalker::remaining() const
    {
        return distance(CharOffset{}, extend(total_offset));
    }

    bool ReverseTreeWalker::exhausted() const
    {
        if (stackCount == 0)
            return true;
        // If we have not exhausted the pointers, we're still active.
        if (first_ptr != last_ptr)
            return false;
        // If there's more than one entry on the stack, we're still active.
        for(int i =0; i < stackCount; i++)
        {
            auto index = stack[i].index;
            auto childCount = stack[i].node->childCount;
            if(index < childCount)
                return false;
        }
        return true;
    }

    void ReverseTreeWalker::populate_ptrs()
    {
        if (exhausted())
            return;
        while (stack[stackCount-1].node->childCount == stack[stackCount-1].index)
        {
            stackCount--;
            if(stackCount == 0)return;
        }
        while(!stack[stackCount-1].node->isLeaf())
        {
            StorageTree::InternalNodePtr in = reinterpret_cast<StorageTree::InternalNodePtr>(stack[stackCount-1].node);
            StorageTree::NodeVector children = (&in->children[0]);
            stack[stackCount-1].index++;
            stack[stackCount++]={children[stack[stackCount].node->childCount - stack[stackCount].index], 0};
        }

        StorageTree::LeafNodePtr ln = reinterpret_cast<StorageTree::LeafNodePtr>(stack[stackCount].node);
        NodeData* leafs = (&ln->children[0]);

        stack[stackCount-1].index++;
        auto& piece = leafs[stack[stackCount-1].node->childCount - stack[stackCount-1].index].piece;
        auto* buffer = buffers->buffer_at(piece.index);
        auto first_offset = buffers->buffer_offset(piece.index, piece.first);
        auto last_offset = buffers->buffer_offset(piece.index, piece.last);
        last_ptr = buffer->buffer.str + rep(first_offset);
        first_ptr = buffer->buffer.str + rep(last_offset);


    }

    void ReverseTreeWalker::fast_forward_to(CharOffset offset)
    {
        seek(offset);
    }


    void ReverseTreeWalker::seek(CharOffset offset)
    {
        stackCount = 0;
        if(!root.root_ptr())return;
        stack[stackCount++] = { root.root_ptr() };
        total_offset = offset;

        Length accumulated {0};

        while(!stack[stackCount-1].node->isLeaf())
        {
            algo_mark(stack[stackCount-1].node->shared_from_this(), Collect);
            auto &stack_entry = stack[stackCount-1];
            StorageTree::InternalNodePtr in = reinterpret_cast<StorageTree::InternalNodePtr>(stack_entry.node);
            StorageTree::NodeVector children = (&in->children[0]);
            Length sublen {0};
            for(stack_entry.index = 0; stack_entry.index< stack_entry.node->childCount; stack_entry.index++)
            {
                auto subtreelen = stack_entry.node->offsets[stack_entry.index];
                if(rep(offset) < rep(subtreelen)+ rep(accumulated))
                {
                    auto index = stack_entry.index++;
                    stack_entry.index = stack_entry.node->childCount - index;
                    stack[stackCount++] = {children[index]};
                    accumulated = accumulated + sublen;
                    break;
                }
                algo_mark(children[stack_entry.index]->shared_from_this(), Traverse);
                sublen = subtreelen;
            }
        }
        StorageTree::LeafNodePtr ln = reinterpret_cast<StorageTree::LeafNodePtr>(stack[stackCount-1].node);
        NodeData* children = (&ln->children[0]);
        algo_mark(stack[stackCount-1].node->shared_from_this(), Collect);
        for(stack[stackCount-1].index = 0; stack[stackCount-1].index< stack[stackCount-1].node->childCount;stack[stackCount-1].index++)
        {
            auto subtreelen = children[stack[stackCount-1].index].piece.length;
            if(rep(offset) < rep(subtreelen) + rep(accumulated))
            {
                break;
            }
            accumulated = accumulated + subtreelen;
        }

        auto& piece = children[stack[stackCount-1].index++].piece;
        stack[stackCount-1].index = stack[stackCount-1].node->childCount - stack[stackCount-1].index+1;
        auto* buffer = buffers->buffer_at(piece.index);
        auto first_offset = buffers->buffer_offset(piece.index, piece.first);
        //auto last_offset = buffers->buffer_offset(piece.index, piece.last);
        first_ptr  = buffer->buffer.str + rep(first_offset) + rep(offset)- rep(accumulated) +1;
        last_ptr = buffer->buffer.str + rep(first_offset);
    }
}


template<size_t MaxChildren>
void print_tree(RatchetPieceTree::BNodeCountedGeneric<MaxChildren>* root, const RatchetPieceTree::Tree* tree, int level, size_t node_offset)
{

    const char* levels = "|||||||||||||||||||||||||||||||";
    //auto this_offset = node_offset;
    printf("%.*sme: %p, numch: %zd leaf: %s\n",level, levels, root, root->childCount, root->isLeaf()?"yes":"no");
    //printf("%.*s  :\n",level, levels);
    if(root->isLeaf()){
        for(int i = 0; i < root->childCount;i++){
            RatchetPieceTree::StorageTree::LeafNodePtr ln = reinterpret_cast<RatchetPieceTree::StorageTree::LeafNodePtr>(root);
            print_piece(ln->children[i].piece, tree, level+1);
        }
    }
    else
    {
        RatchetPieceTree::StorageTree::InternalNodePtr in = reinterpret_cast<RatchetPieceTree::StorageTree::InternalNodePtr>(root);
            
        for(int i = 0; i < root->childCount;i++){
             RatchetPieceTree::StorageTree::NodePtr childptr = (in->children)[i];
            RatchetPieceTree::Length sublen = root->offsets[i];
            printf("%p, %d,", childptr, (int)rep(sublen));
        }
        printf("\n");
        for(int i = 0; i < root->childCount;i++){
             RatchetPieceTree::StorageTree::NodePtr childptr = (in->children)[i];

            RatchetPieceTree::Length sublen = root->offsets[i];
            ::print_tree(childptr , tree, level + 1, node_offset+rep(sublen));
        }
    }
    //printf("%.*sleft_len{%zd}, left_lf{%zd}, node_offset{%zd}\n", level, levels, rep(root.root().left_subtree_length), rep(root.root().left_subtree_lf_count), this_offset);
}

void print_buffer(const RatchetPieceTree::Tree* tree)
{
    Arena::Temp scratch = Arena::scratch_begin(Arena::no_conflicts);
    printf("--- Entire Buffer ---\n");
    RatchetPieceTree::TreeWalker walker{ scratch.arena, tree };
    std::string buf;
    while (not walker.exhausted())
    {
        buf.push_back(walker.next());
    }

    for (size_t i = 0; i < buf.size(); ++i)
    {
        printf("|%2zu", i);
    }
    printf("\n");
    for (char c : buf)
    {
        if (c == '\n')
            printf("|\\n");
        else
            printf("| %c", c);
    }
    printf("\n");
    Arena::scratch_end(scratch);
}