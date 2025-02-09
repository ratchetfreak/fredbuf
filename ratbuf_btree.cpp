#include "ratbuf.h"


#include <cassert>

#include <memory>
#include <string_view>
#include <string>
#include <vector>
#include <array>

#include "types.h"
#include "enum-utils.h"
#include "scope-guard.h"

void print_tree(const RatchetPieceTree::StorageTree::Node& root, const RatchetPieceTree::Tree* tree, int level = 0, size_t node_offset = 0);

namespace RatchetPieceTree
{
    std::vector<algo_marker> algorithm;
    constexpr LFCount operator+(LFCount lhs, LFCount rhs)
    {
        return LFCount{ rep(lhs) + rep(rhs) };
    }

    RatchetPieceTree::LFCount tree_lf_count(const StorageTree& root)
    {
        if (root.is_empty())
            return { };
        return root.root_ptr()->subTreeLineFeeds();
    }
    namespace
    {
        void populate_line_starts(LineStarts* starts, std::string_view buf)
        {
            starts->clear();
            LineStart start { };
            starts->push_back(start);
            const auto len = buf.size();
            for (size_t i = 0; i < len; ++i)
            {
                char c = buf[i];
                if (c == '\n')
                {
                    start = LineStart{ i + 1 };
                    starts->push_back(start);
                }
            }
        }

        RatchetPieceTree::Length tree_length(const StorageTree& root)
        {
            if (root.is_empty())
                return { };
            return root.root_ptr()->subTreeLength();
        }

        void compute_buffer_meta(BufferMeta* meta, const StorageTree& root)
        {
            meta->lf_count = tree_lf_count(root);
            meta->total_content_length = tree_length(root);
        }
    } // namespace [anon]

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::B_Tree(NodePtr root):root_node(root){

    }
    template<size_t MaxChildren>
    B_Tree<MaxChildren> B_Tree<MaxChildren>::construct_from(std::vector<NodeData> leafNodes) const
    {
        if(leafNodes.empty())
        {
            return B_Tree<MaxChildren>();
        }
        std::vector<NodePtr> nodes;
        size_t i = 0;
        for(; i+ MaxChildren*2 < leafNodes.size(); i+=MaxChildren)
        {
            nodes.push_back(construct_leaf(leafNodes, i, i+MaxChildren));
        }
        size_t remaining = leafNodes.size() - i;
        assert(nodes.empty() ||
            (remaining <= (MaxChildren*2) && remaining >= MaxChildren));
        if(nodes.empty() && remaining < MaxChildren)
        {
            return B_Tree(construct_leaf(leafNodes, 0, remaining));
        }
        else
        {

            nodes.push_back(construct_leaf(leafNodes, i, i+remaining/2));
            i+=remaining/2;
            nodes.push_back(construct_leaf(leafNodes, i, leafNodes.size()));
        }
        while(nodes.size() > MaxChildren)
        {
            std::vector<NodePtr> childNodes;
            i = 0;
            for(; i+ MaxChildren*2 < nodes.size(); i+=MaxChildren)
            {
                childNodes.push_back(construct_internal(nodes, i, i+MaxChildren));
            }
            remaining = nodes.size() - i;
            assert((remaining <= (MaxChildren*2) && remaining >= MaxChildren));
            childNodes.push_back(construct_internal(nodes, i, i+remaining/2));
            i+=remaining/2;
            childNodes.push_back(construct_internal(nodes, i, nodes.size()));
            nodes = std::move(childNodes);
        }
        return B_Tree(construct_internal(nodes, 0, nodes.size()));
    }

    template<size_t MaxChildren>
    bool B_Tree<MaxChildren>::is_empty() const
    {
        return not root_node;
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren> B_Tree<MaxChildren>::insert(const NodeData& x, Offset at, BufferCollection* buffers) const
    {
        const Node* root = root_node.get();
        auto result = insertInto(root, x, Editor::distance(Offset(0), at), buffers);
        size_t newNumChildren = result.nodes.size();
        if(newNumChildren > 1)
        {
            NodePtr new_root = construct_internal(result.nodes, 0, newNumChildren);
            return B_Tree<MaxChildren>(new_root);
        }
        else
        {
            if(result.nodes.empty())
            {

                return B_Tree<MaxChildren>();
            }
            else
            {

                return B_Tree<MaxChildren>(std::move(result.nodes.front()));
            }
        }
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren> B_Tree<MaxChildren>::remove(Offset at, Length len, BufferCollection* buffers) const
    {
        std::vector<NodePtr> result = remove_from(root_node.get(), nullptr, nullptr, distance(Offset{0},at), len, buffers);

        if(result.empty())
        {
            return B_Tree<MaxChildren>();
        }
        else if(result.size()== 1)
        {
            return B_Tree<MaxChildren>(std::move(result[0]));
        }
        else
        {
            assert(result.size() < MaxChildren);
            NodePtr new_root = construct_internal(result, 0, result.size());
            return B_Tree<MaxChildren>(new_root);
        }
    }

    BufferCursor buffer_position(const BufferCollection* buffers, const Piece& piece, Length remainder)
    {
        auto& starts = buffers->buffer_at(piece.index)->line_starts;
        auto start_offset = rep(starts[rep(piece.first.line)]) + rep(piece.first.column);
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
            mid_start = rep(starts[mid]);

            if (mid == high)
                break;
            mid_stop = rep(starts[mid + 1]);

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
        auto& starts = buffers->buffer_at(index)->line_starts;
        // It means, there is no LF after end.
        if (end.line == Line{ starts.size() - 1})
            return LFCount{ rep(retract(end.line, rep(start.line))) };
        // Due to the check above, we know that there's at least one more line after 'end.line'.
        auto next_start_offset = starts[rep(extend(end.line))];
        auto end_offset = rep(starts[rep(end.line)]) + rep(end.column);
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
    B_Tree<MaxChildren>::TreeManipResult B_Tree<MaxChildren>::insertInto_leaf(const Node* node, const NodeData& x, Length at, BufferCollection* buffers) const
    {
        if(node)algo_mark(node->shared_from_this(), Traverse);
        if(node==nullptr)
        {
            B_Tree<MaxChildren>::TreeManipResult res;
            LeafVector r;
            r.push_back(std::move(x));
            res.nodes.push_back(construct_leaf(r, 0, 1));
            return res;
        }
        auto offsets_end = node->offsets.begin()+node->childCount;
        auto insertPoint = std::lower_bound(node->offsets.begin(), offsets_end, at);
        auto insertIndex = insertPoint - node->offsets.begin();
        std::vector<NodeData> resultch;

        auto children = std::get<std::array<NodeData, MaxChildren>>(node->children);

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
            resultch.push_back(*child_it);
        }
        auto splitting_piece = (*child_it).piece;
        if(split_offset > Length{0} && split_offset < splitting_piece.length)
        {

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
            resultch.push_back(first);

            resultch.push_back(x);

            NodeData second = NodeData
            {
                .piece = new_piece_right
            };

            resultch.push_back(second);
            ++child_it;
        }
        else
        {
            //TODO(ratchetfreak): combine pieces with modbuf?
            if(split_offset == Length{0})
            {
                resultch.push_back(x);
                resultch.push_back(*child_it);
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
                    new_piece.newline_count = new_piece.newline_count + old_piece.newline_count;
                    new_piece.length = new_piece.length + old_piece.length;
                    resultch.push_back(d);
                    ++child_it;
                }
                else
                {
                    resultch.push_back(*child_it);
                    ++child_it;
                    resultch.push_back(x);
                }
            }
        }

        for(; child_it!=child_end; ++child_it)
        {
            resultch.push_back(*child_it);
        }
        B_Tree<MaxChildren>::TreeManipResult res;
        if(resultch.size()>MaxChildren)
        {
            size_t mid = resultch.size()/2;
            res.nodes.push_back(construct_leaf(resultch, 0, mid));
            res.nodes.push_back(construct_leaf(resultch, mid, resultch.size()));
        }
        else
        {
            res.nodes.push_back(construct_leaf(resultch, 0, resultch.size()));
        }
        return res;
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::ChildVector B_Tree<MaxChildren>::remove_from_leafs(const Node* a, const Node* b, const Node* c, Length at, Length len, BufferCollection* buffers) const
    {
        // b or c might be nullptr

        LeafVector allLeafs;
        {
            LeafArray ch = std::get<LeafArray>(a->children);
            for(auto it = ch.begin(); it != ch.begin()+a->childCount;++it)
            {
                allLeafs.push_back(*it);
            }
        }
        if(b!=nullptr)
        {
            LeafArray ch = std::get<LeafArray>(b->children);
            for(auto it = ch.begin(); it != ch.begin()+b->childCount;++it)
            {
                allLeafs.push_back(*it);
            }
        }
        if(c!=nullptr)
        {
            LeafArray ch = std::get<LeafArray>(c->children);
            for(auto it = ch.begin(); it != ch.begin()+c->childCount;++it)
            {
                allLeafs.push_back(*it);
            }
        }
        size_t numLeafs = allLeafs.size();
        (void)numLeafs;

        LeafVector allChildren;

        int i = 0;

        for(;i < allLeafs.size() && allLeafs[i].piece.length <= at;i++)
        {
            auto current_leaf = allLeafs[i];
            allChildren.push_back(current_leaf);
            at = at - current_leaf.piece.length;
        }

        //trim or split allLeafs[i].piece
        if(at+len <= allLeafs[i].piece.length)
        {
            auto piece_to_split = allLeafs[i].piece;
            auto start_split_pos = buffer_position(buffers, piece_to_split, at);

            auto end_split_pos = buffer_position(buffers, piece_to_split, at+len);
            auto piece_left = trim_piece_left(buffers, piece_to_split, end_split_pos);
            Piece piece_right = trim_piece_right(buffers, piece_to_split, start_split_pos);

            if(rep(piece_right.length) > 0)
                allChildren.push_back({piece_right});
            if(rep(piece_left.length) > 0)
                allChildren.push_back({piece_left});

            //split
            i++;
        }
        else
        {
            if(rep(at)>0)
            {
                auto start_split_pos = buffer_position(buffers, allLeafs[i].piece, at);
                auto trimright = trim_piece_right(buffers, allLeafs[i].piece, start_split_pos);
                allChildren.push_back({trimright});
            }
            len = len -  (allLeafs[i].piece.length - at);
            i++;
            for(;i < allLeafs.size() && allLeafs[i].piece.length <= len;i++)
            {
                len  = len - allLeafs[i].piece.length;
            }
            if(rep(len) > 0 && i < allLeafs.size())
            {
                auto end_split_pos = buffer_position(buffers, allLeafs[i].piece, len);

                auto piece_left = trim_piece_left(buffers, allLeafs[i].piece, end_split_pos);
                allChildren.push_back({piece_left});
                i++;
            }
        }


        //add rest

        for(;i<allLeafs.size();i++)
        {
            allChildren.push_back(allLeafs[i]);
        }


        ChildVector result;
        if(allChildren.size() > MaxChildren*3)
        {
            
            size_t from = 0;
            size_t perChild = allChildren.size()/4;

            result.push_back(construct_leaf(allChildren, from, from+perChild));
            from+=perChild;
            if(allChildren.size()%4 > 1)
            {
                perChild+=1;
            }
            result.push_back(construct_leaf(allChildren, from, from+perChild));
            from+=perChild;
            result.push_back(construct_leaf(allChildren, from, from+perChild));
            from+=perChild;
            result.push_back(construct_leaf(allChildren, from, allChildren.size()));
        }
        else if(allChildren.size() > MaxChildren*2)
        {
            size_t from = 0;
            size_t perChild = allChildren.size()/3;

            result.push_back(construct_leaf(allChildren, from, from+perChild));
            from+=perChild;
            if(allChildren.size()%3 > 1)
            {
                perChild+=1;
            }
            result.push_back(construct_leaf(allChildren, from, from+perChild));
            from+=perChild;
            result.push_back(construct_leaf(allChildren, from, allChildren.size()));
        }
        else if(allChildren.size() > MaxChildren)
        {
            size_t from = 0;
            size_t mid = allChildren.size()/2;
            result.push_back(construct_leaf(allChildren, from, from+mid));

            result.push_back(construct_leaf(allChildren, mid, allChildren.size()));

        }
        else if(allChildren.size() > 0)
        {
            //assert(allChildren.size() >= MaxChildren/2);
            result.push_back(construct_leaf(allChildren, 0, allChildren.size()));
        }

        return result;

    }

    void Tree::insert(CharOffset offset, std::string_view txt, SuppressHistory suppress_history)
    {
        if (txt.empty())
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
    B_Tree<MaxChildren>::TreeManipResult B_Tree<MaxChildren>::insertInto(const Node* node, const NodeData& x, Length at, BufferCollection* buffers) const
    {
        if(node)algo_mark(node->shared_from_this(), Traverse);
        //TODO(ratchetfreak) : change result to be 1 or more ChildPtrs which is exactly the x node with the insert
        if(node==nullptr||node->isLeaf())
        {
            return insertInto_leaf(node, x, at, buffers);
        }
        else
        {
            const std::array<NodePtr, MaxChildren> &node_children = std::get<std::array<NodePtr, MaxChildren>>(node->children);
            auto offsets_end = node->offsets.begin()+node->childCount;
            auto insertPoint = std::lower_bound(node->offsets.begin(), offsets_end, at);
            size_t insertIndex = insertPoint - node->offsets.begin();
            auto insertChIt = node_children.begin()+insertIndex;
            auto insertChItEnd = node_children.begin()+node->childCount;

            std::vector<NodePtr> resultch;
            auto child_it = (node_children).begin();

            for(; child_it!=insertChIt; ++child_it)
            {
                resultch.push_back(*child_it);
            }


            Length childInsert = at;
            if(insertIndex>0)
            {
                childInsert = childInsert - node->offsets[insertIndex - 1];
            }
            auto insert_child = node_children[insertIndex];
            auto insert_result = insertInto(insert_child.get(), x, childInsert, buffers);

            size_t newNumChildren = insert_result.nodes.size();

            for(int i = 0; i< newNumChildren;i++)
            {
                resultch.push_back(insert_result.nodes[i]);
            }
            child_it++;
            for(; child_it!=insertChItEnd;  ++child_it)
            {
                resultch.push_back(*child_it);
            }

            if(resultch.size() > MaxChildren)
            {
                //split the to-be inserted node into two
                size_t numLChild = resultch.size()/2;

                NodePtr leftChild = construct_internal(resultch, 0, numLChild);
                NodePtr rightChild = construct_internal(resultch, numLChild, resultch.size());

                TreeManipResult manipresult;
                manipresult.nodes.push_back(std::move(leftChild));

                manipresult.nodes.push_back(std::move(rightChild));

                return manipresult;
            }
            else
            {

                auto newChild = construct_internal(resultch
                , 0, resultch.size());
                TreeManipResult manipresult;
                manipresult.nodes.push_back(std::move(newChild));
                return manipresult;
            }
        }
        // return {};
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::ChildVector B_Tree<MaxChildren>::remove_from(const Node* a, const Node* b, const Node* c, Length at, Length len, BufferCollection* buffers) const
    {
        // at least one of a and c should not participate in the remove
        // that way the result is always big enough to make at least 1 node
        
        if(a)algo_mark(a->shared_from_this(), Collect);
        if(b)algo_mark(b->shared_from_this(), Collect);
        if(c)algo_mark(c->shared_from_this(), Collect);

        if(a->isLeaf())
        {
            return remove_from_leafs(a, b, c, at, len, buffers);
        }
        else
        {
            ChildVector resultChildren;
            std::array<NodePtr, MaxChildren * 3> allChildren;
            size_t totalChildCount;

            {
                const ChildArray& chA = std::get<ChildArray>(a->children);
                auto outputIt = allChildren.begin();
                outputIt = std::copy_n(chA.begin(), a->childCount, outputIt);
                totalChildCount = a->childCount;
                if(b!=nullptr)
                {
                    const ChildArray& chB = std::get<ChildArray>(b->children);
                    outputIt = std::copy_n(chB.begin(), b->childCount, outputIt);
                    totalChildCount = totalChildCount + b->childCount;
                }
                if(c!=nullptr)
                {
                    const ChildArray& chC = std::get<ChildArray>(c->children);
                    outputIt = std::copy_n(chC.begin(), c->childCount, outputIt);
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
                    resultChildren.push_back(std::move(recA));
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
                resultChildren.push_back(std::move(recA));
            }
            recA = recB;
            recB = recC;
            recC = allChildren[i];
            aplusbplusc = (recA?recA->subTreeLength():Length{0})+
                        (recB?recB->subTreeLength():Length{0})+
                        recC->subTreeLength();
            Length to_remove_from_first_found = aplusbplusc - offset;
            i++;
            ChildVector res;
            
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
                            return remove_from(recB.get(), recC.get(), nullptr, offset, len, buffers);
                        }
                        else
                        {
                            return remove_from(recC.get(), nullptr, nullptr, offset, len, buffers);
                        }
                    }
                    recA = recB;
                    recB = recC;
                    recC = allChildren[i];
                    i++;
                    algo_mark(recC, Traverse);

                }
                res = remove_from(recA.get(), recB.get(), recC.get(), offset, len, buffers);
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
                        return remove_from(recB.get(), recC.get(), nullptr, offset, to_remove_from_first_found + to_remove_from_rest, buffers);
                    }
                    else
                    {
                        return remove_from(recC.get(), nullptr, nullptr, offset, to_remove_from_first_found + to_remove_from_rest, buffers);
                    }
                }

                if(i < totalChildCount)
                {
                    if(recA)
                    {
                        offset = offset - recA->subTreeLength();
                        resultChildren.push_back(std::move(recA));
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
                            return remove_from(recB.get(), recC.get(), nullptr, offset, to_remove_from_first_found + to_remove_from_rest, buffers);
                        }
                        else
                        {
                            return remove_from(recC.get(), nullptr, nullptr, offset, to_remove_from_first_found + to_remove_from_rest, buffers);
                        }
                    }
                    recA = recB;
                    recB = recC;
                    recC = allChildren[i];
                    i++;
                    
                    algo_mark(recC, Traverse);

                }
                res = remove_from(recA.get(), recB.get(), recC.get(), offset, to_remove_from_first_found + to_remove_from_rest, buffers);
            }

            for(auto it = res.begin(); it != res.end();++it){
                resultChildren.push_back(std::move(*it));
            }

            for(;i<totalChildCount;i++)
            {
                resultChildren.push_back(allChildren[i]);
            }

            ChildVector result;
            if(resultChildren.size() > MaxChildren*3)
            {
                size_t from = 0;
                size_t perChild = resultChildren.size()/4;

                result.push_back(construct_internal(resultChildren, from, from+perChild));
                from+=perChild;
                if(resultChildren.size()%4 > 1)
                {
                    perChild+=1;
                }
                result.push_back(construct_internal(resultChildren, from, from+perChild));
                from+=perChild;
                result.push_back(construct_internal(resultChildren, from, from+perChild));
                from+=perChild;
                result.push_back(construct_internal(resultChildren, from, resultChildren.size()));
            }
            else if(resultChildren.size() > MaxChildren*2)
            {
                size_t from = 0;
                size_t perChild = resultChildren.size()/3;

                result.push_back(construct_internal(resultChildren, from, from+perChild));
                from+=perChild;
                if(resultChildren.size()%3 > 1)
                {
                    perChild+=1;
                }
                result.push_back(construct_internal(resultChildren, from, from+perChild));
                from+=perChild;
                result.push_back(construct_internal(resultChildren, from, resultChildren.size()));
            }
            else if(resultChildren.size() > MaxChildren)
            {
                size_t from = 0;
                size_t mid = resultChildren.size()/2;
                result.push_back(construct_internal(resultChildren, from, from+mid));

                result.push_back(construct_internal(resultChildren, mid, resultChildren.size()));

            }
            else
            {
                assert(result.size() >= MaxChildren/2 || c==nullptr);
                result.push_back(construct_internal(resultChildren, 0, resultChildren.size()));
            }

            return result;
        }
    }

    template<size_t MaxChildren>
    B_Tree<MaxChildren>::NodePtr B_Tree<MaxChildren>::construct_leaf(const LeafVector &data, size_t begin, size_t end) const
    {

        size_t numChild = end-begin;
        assert(numChild <= MaxChildren);
        std::array<NodeData, MaxChildren> new_left_children;
        std::array<Length, MaxChildren>  new_left_offsets;
        std::array<LFCount, MaxChildren>  new_left_linefeed;
        Length acc{0};
        LFCount linefeed{0};
        for(int i = 0; i < numChild; i++)
        {
            new_left_children[i] =data[begin+i];
            acc = acc + data[begin+i].piece.length;
            new_left_offsets[i] = acc;
            linefeed = linefeed + data[begin+i].piece.newline_count;
            new_left_linefeed[i] = linefeed;
        }
        auto result = std::make_shared<Node>(new_left_children, new_left_offsets, new_left_linefeed, end-begin);
        algo_mark(result, Made);
        return result;
    }


    template<size_t MaxChildren>
    B_Tree<MaxChildren>::NodePtr B_Tree<MaxChildren>::construct_internal(const ChildVector &data, size_t begin, size_t end) const
    {
        size_t numChild = end-begin;
        assert(numChild <= MaxChildren);
        std::array<NodePtr, MaxChildren> new_left_children;
        std::array<Length, MaxChildren>  new_left_offsets;
        std::array<LFCount, MaxChildren>  new_left_linefeed;
        Length acc{0};
        LFCount linefeed{0};
        for(int i = 0; i < numChild; i++)
        {
            new_left_children[i] =data[begin+i];
             acc = acc + data[begin+i]->subTreeLength();
            new_left_offsets[i] = acc;
            linefeed = linefeed + data[begin+i]->subTreeLineFeeds();
            new_left_linefeed[i] = linefeed;

        }

        auto result = std::make_shared<Node>(new_left_children, new_left_offsets, new_left_linefeed, end-begin);
        algo_mark(result, Made);
        return result;
    }

    template<size_t MaxChildren>
    const B_Tree<MaxChildren>::Node* B_Tree<MaxChildren>::root_ptr() const
    {
        return root_node.get();
    }

#ifdef TEXTBUF_DEBUG
    template<size_t MaxChildren>
    void satisfies_btree_invariant(const B_Tree<MaxChildren>& root)
    {
        // 1. every node other than root has >= MaxChildren/2 children 
        // 1.5. nodes are hardcoded to contain <= MaxChildren
        // 2. all leafNodes are at the same depth
        
        typedef B_Tree<MaxChildren>::NodePtr NPtr;
        typedef B_Tree<MaxChildren>::ChildArray  ChArr;
        std::vector<NPtr> layer;
        std::vector<NPtr> next_layer;
        
        if(root.is_empty() || root.root_ptr()->isLeaf())
            return;
        
        auto children = std::get<ChArr>(root.root_ptr()->children);
        next_layer.assign(children.begin(), children.begin()+root.root_ptr()->childCount);
        while(!next_layer.front()->isLeaf())
        {
            layer = std::move(next_layer);
            next_layer.clear();
            for(auto nodeIt = layer.begin(); nodeIt != layer.end();++nodeIt)
            {
                assert(!(*nodeIt)->isLeaf());
                assert((*nodeIt)->childCount >= MaxChildren/2);
                auto node_children = std::get<ChArr>((*nodeIt)->children);
                next_layer.insert(next_layer.end(), node_children.begin(), node_children.begin()+(*nodeIt)->childCount);
            }
        }
        layer = std::move(next_layer);
        for(auto nodeIt = layer.begin(); nodeIt != layer.end();++nodeIt)
        {
            assert((*nodeIt)->isLeaf());
            assert((*nodeIt)->childCount >= MaxChildren/2);
        }
    }
#endif

    Tree::Tree():
        buffers{ }
    {
        build_tree();
    }

    Tree::Tree(Buffers&& buffers):
        buffers{ std::move(buffers) }
    {
        build_tree();
    }

    void Tree::build_tree()
    {
        buffers.mod_buffer.line_starts.clear();
        buffers.mod_buffer.buffer.clear();
        // In order to maintain the invariant of other buffers, the mod_buffer needs a single line-start of 0.
        buffers.mod_buffer.line_starts.push_back({});
        last_insert = { };

        const auto buf_count = buffers.orig_buffers.size();
        std::vector<NodeData> leafNodes;

        for (size_t i = 0; i < buf_count; ++i)
        {
            const auto& buf = *buffers.orig_buffers[i];
            assert(not buf.line_starts.empty());
            // If this immutable buffer is empty, we can avoid creating a piece for it altogether.
            if (buf.buffer.empty())
                continue;
            auto last_line = Line{ buf.line_starts.size() - 1 };
            // Create a new node that spans this buffer and retains an index to it.
            // Insert the node into the balanced tree.
            Piece piece {
                .index = BufferIndex{ i },
                .first = { .line = Line{ 0 }, .column = Column{ 0 } },
                .last = { .line = last_line, .column = Column{ buf.buffer.size() - rep(buf.line_starts[rep(last_line)]) } },
                .length = Length{ buf.buffer.size() },
                // Note: the number of newlines
                .newline_count = LFCount{ rep(last_line) }
            };
            leafNodes.push_back({piece});
        }
        root = root.construct_from(leafNodes);

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

    LineRange Tree::get_line_range(Line line) const
    {
        LineRange range{ };
        line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        line_start<&Tree::accumulate_value_no_lf>(&range.last, &buffers, root, extend(line));
        return range;
    }
    OwningSnapshot Tree::owning_snap() const
    {
        return OwningSnapshot{ this };
    }
    ReferenceSnapshot Tree::ref_snap() const
    {
        return ReferenceSnapshot{ this };
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
        if (end.line == Line{ starts.size() - 1})
            return LFCount{ rep(retract(end.line, rep(start.line))) };
        // Due to the check above, we know that there's at least one more line after 'end.line'.
        auto next_start_offset = starts[rep(extend(end.line))];
        auto end_offset = rep(starts[rep(end.line)]) + rep(end.column);
        // There are more than 1 character after end, which means it can't be LF.
        if (rep(next_start_offset) > end_offset + 1)
            return LFCount{ rep(retract(end.line, rep(start.line))) };
        // This must be the case.  next_start_offset is a line down, so it is
        // not possible for end_offset to be < it at this point.
        assert(end_offset + 1 == rep(next_start_offset));
        return LFCount{ rep(retract(end.line, rep(start.line))) };
    }

    void Tree::get_line_content(std::string* buf, Line line) const
    {
        // Reset the buffer.
        buf->clear();
        if (line == Line::IndexBeginning)
            return;
        assemble_line(buf, root, line);
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

    void Tree::assemble_line(std::string* buf, const StorageTree& node, Line line) const
    {
        if(node.is_empty())
            return;
                CharOffset line_offset{ };
        line_start<&Tree::accumulate_value>(&line_offset, &buffers, node, line);
        TreeWalker walker{ this, line_offset };
        while (not walker.exhausted())
        {
            char c = walker.next();
            if (c == '\n')
                break;
            buf->push_back(c);
        }
    }

    const CharBuffer* BufferCollection::buffer_at(BufferIndex index) const
    {
        if (index == BufferIndex::ModBuf)
            return &mod_buffer;
        return orig_buffers[rep(index)].get();
    }

    CharOffset BufferCollection::buffer_offset(BufferIndex index, const BufferCursor& cursor) const
    {
        auto& starts = buffer_at(index)->line_starts;
        return CharOffset{ rep(starts[rep(cursor.line)]) + rep(cursor.column) };
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
        printf("%.*sPiece content: %.*s\n", level, levels, static_cast<int>(piece.length), buffer->buffer.data() + rep(offset));
    }
    void print_tree(const RatchetPieceTree::Tree& tree)
    {
        ::print_tree(*(tree.root.root_ptr()), &tree);
    }

    Piece Tree::build_piece(std::string_view txt)
    {
        auto start_offset = buffers.mod_buffer.buffer.size();
        populate_line_starts(&scratch_starts, txt);
        auto start = last_insert;
        // TODO: Handle CRLF (where the new buffer starts with LF and the end of our buffer ends with CR).
        // Offset the new starts relative to the existing buffer.
        for (auto& new_start : scratch_starts)
        {
            new_start = extend(new_start, start_offset);
        }
        // Append new starts.
        // Note: we can drop the first start because the algorithm always adds an empty start.
        auto new_starts_end = scratch_starts.size();
        buffers.mod_buffer.line_starts.reserve(buffers.mod_buffer.line_starts.size() + new_starts_end);
        for (size_t i = 1; i < new_starts_end; ++i)
        {
            buffers.mod_buffer.line_starts.push_back(scratch_starts[i]);
        }
        auto old_size = buffers.mod_buffer.buffer.size();
        buffers.mod_buffer.buffer.resize(buffers.mod_buffer.buffer.size() + txt.size());
        auto insert_at = buffers.mod_buffer.buffer.data() + old_size;
        std::copy(txt.data(), txt.data() + txt.size(), insert_at);

        // Build the new piece for the inserted buffer.
        auto end_offset = buffers.mod_buffer.buffer.size();
        auto end_index = buffers.mod_buffer.line_starts.size() - 1;
        auto end_col = end_offset - rep(buffers.mod_buffer.line_starts[end_index]);
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
        size_t node_start_offset = 0;
        size_t newline_count = 0;
        const StorageTree::Node* node = tree.root_ptr();
        
        
        while(!node->isLeaf())
        {
            auto children = std::get<StorageTree::ChildArray>(node->children);
            int i = 0;
            for(; i<node->childCount; i++)
            {
                if(rep(distance(Offset{node_start_offset}, off)) < rep(node->offsets[i]))
                {
                    if(i>0)
                    {
                        node_start_offset += rep(node->offsets[i-1]);
                        newline_count += rep(node->lineFeeds[i-1]);
                    }
                    node = children[i].get();
                    break;
                }
                node_start_offset += rep(node->offsets[i]);
                newline_count += rep(node->lineFeeds[i]);
            }
            if(i==node->childCount){
                node = children[node->childCount-1].get();
            }
        }
        auto children = std::get<StorageTree::LeafArray>(node->children);
        int i = 0;
        for(; i<node->childCount; i++)
        {
            if(distance(Offset{node_start_offset}, off) < children[i].piece.length)
            {
                break;
            }
            node_start_offset += rep(children[i].piece.length);
            newline_count += rep(children[i].piece.newline_count);
        }
        if(i>=node->childCount)
        {
            i--;
            node_start_offset -= rep(children[i].piece.length);
            newline_count -= rep(children[i].piece.newline_count);
        }
        Piece result_piece =  children[i].piece;
        //node = children[i].get();

        // Now we find the line within this piece.
        auto remainder = distance (Offset{node_start_offset}, off);
        auto pos = buffer_position(buffers, result_piece, remainder);
        // Note: since buffer_position will return us a newline relative to the buffer itself, we need
        // to retract it by the starting line of the piece to get the real difference.
        newline_count += rep(retract(pos.line, rep(result_piece.first.line)));

        NodePosition result { .node = &children[i],
                    .remainder = remainder,
                    .start_offset = CharOffset{ node_start_offset },
                    .line = Line{ newline_count + 1 } };
        return result;
    }
    void Tree::compute_buffer_meta()
    {
        ::RatchetPieceTree::compute_buffer_meta(&meta, root);
    }


    void Tree::append_undo(const StorageTree& old_root, CharOffset op_offset)
    {
        // Can't redo if we're creating a new undo entry.
        if (not redo_stack.empty())
        {
            redo_stack.clear();
        }
        undo_stack.push_front({ .root = old_root, .op_offset = op_offset });
    }

    UndoRedoResult Tree::try_undo(CharOffset op_offset)
    {
        if (undo_stack.empty())
            return { .success = false, .op_offset = CharOffset{ } };
        redo_stack.push_front({ .root = root, .op_offset = op_offset });
        auto [node, undo_offset] = undo_stack.front();
        root = node;
        undo_stack.pop_front();
        compute_buffer_meta();
        return { .success = true, .op_offset = undo_offset };
    }

    UndoRedoResult Tree::try_redo(CharOffset op_offset)
    {
        if (redo_stack.empty())
            return { .success = false, .op_offset = CharOffset{ } };
        undo_stack.push_front({ .root = root, .op_offset = op_offset });
        auto [node, redo_offset] = redo_stack.front();
        root = node;
        redo_stack.pop_front();
        compute_buffer_meta();
        return { .success = true, .op_offset = redo_offset };
    }

    void Tree::internal_insert(CharOffset offset, std::string_view txt)
    {
        assert(not txt.empty());
        ScopeGuard guard{ [&] {
            compute_buffer_meta();
#ifdef TEXTBUF_DEBUG
            satisfies_btree_invariant(root);
#endif
        } };
        end_last_insert = extend(offset, txt.size());

        if (root.is_empty())
        {
            auto piece = build_piece(txt);
            root = root.insert({ piece }, CharOffset{ 0 }, &buffers);
            return;
        }
        auto piece = build_piece(txt);
        root = root.insert({ piece }, offset, &buffers);
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
        root = root.remove(offset, count, &buffers);
    }

    // Fetches the length of the piece starting from the first line to 'index' or to the end of
    // the piece.
    Length Tree::accumulate_value_no_lf(const BufferCollection* buffers, const Piece& piece, Line index)
    {
        auto* buffer = buffers->buffer_at(piece.index);
        auto& line_starts = buffer->line_starts;
        // Extend it so we can capture the entire line content including newline.
        auto expected_start = extend(piece.first.line, rep(index) + 1);
        auto first = rep(line_starts[rep(piece.first.line)]) + rep(piece.first.column);
        if (expected_start > piece.last.line)
        {
            auto last = rep(line_starts[rep(piece.last.line)]) + rep(piece.last.column);
            if (last == first)
                return Length{ };
            if (buffer->buffer[last - 1] == '\n')
                return Length{ last - 1 - first };
            return Length{ last - first };
        }
        auto last = rep(line_starts[rep(expected_start)]);
        if (last == first)
            return Length{ };
        if (buffer->buffer[last - 1] == '\n')
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

        const StorageTree::Node* n = node.root_ptr();
        
        while(!n->isLeaf())
        {
            const StorageTree::ChildArray& children = std::get<StorageTree::ChildArray>(n->children);
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
                    n = children[i].get();
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

                n = children[i].get();
            }
        }
        const StorageTree::LeafArray& children = std::get<StorageTree::LeafArray>(n->children);
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
        auto first = rep(line_starts[rep(piece.first.line)]) + rep(piece.first.column);
        if (expected_start > piece.last.line)
        {
            auto last = rep(line_starts[rep(piece.last.line)]) + rep(piece.last.column);
            return Length{ last - first };
        }
        auto last = rep(line_starts[rep(expected_start)]);
        return Length{ last - first };
    }

    OwningSnapshot::OwningSnapshot(const Tree* tree):
        root{ tree->root },
        meta{ tree->meta },
        buffers{ tree->buffers } { }

    OwningSnapshot::OwningSnapshot(const Tree* tree, const StorageTree& dt):
        root{ tree->root },
        meta{ tree->meta },
        buffers{ tree->buffers }
    {
        // Compute the buffer meta for 'dt'.
        compute_buffer_meta(&meta, dt);
    }

    ReferenceSnapshot::ReferenceSnapshot(const Tree* tree):
        root{ tree->root },
        meta{ tree->meta },
        buffers{ &tree->buffers } { }

    ReferenceSnapshot::ReferenceSnapshot(const Tree* tree, const StorageTree& dt):
        root{ dt },
        meta{ tree->meta },
        buffers{ &tree->buffers }
    {
        // Compute the buffer meta for 'dt'.
        compute_buffer_meta(&meta, dt);
    }

    void TreeBuilder::accept(std::string_view txt)
    {
        populate_line_starts(&scratch_starts, txt);
        buffers.push_back(std::make_shared<CharBuffer>(std::string{ txt }, scratch_starts));
    }

    TreeWalker::TreeWalker(const Tree* tree, CharOffset offset):
        buffers{ &tree->buffers },
        root{ tree->root },
        meta{ tree->meta },
        stack{ { root.root_ptr() } },
        total_offset{ offset }
    {
        fast_forward_to(offset);
    }

    TreeWalker::TreeWalker(const OwningSnapshot* snap, CharOffset offset):
        buffers{ &snap->buffers },
        root{ snap->root },
        meta{ snap->meta },
        stack{ { root.root_ptr() } },
        total_offset{ offset }
    {
        fast_forward_to(offset);
    }

    TreeWalker::TreeWalker(const ReferenceSnapshot* snap, CharOffset offset):
        buffers{ snap->buffers },
        root{ snap->root },
        meta{ snap->meta },
        stack{ { root.root_ptr() } },
        total_offset{ offset }
    {
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
        if (stack.empty())
            return true;
        // If we have not exhausted the pointers, we're still active.
        if (first_ptr != last_ptr)
            return false;
        // If there's more than one entry on the stack, we're still active.
        //if (stack.size() > 1)
        //    return false;
        for(int i =0; i < stack.size(); i++)
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
        stack.clear();
        if(!root.root_ptr())return;
        stack.push_back({ root.root_ptr() });
        total_offset = offset;

        Length accumulated {0};
        if(rep(offset) >= rep(stack.back().node->subTreeLength()))
        {
            stack.clear();
            return;
        }
        while(!stack.back().node->isLeaf())
        {
            algo_mark(stack.back().node->shared_from_this(), Collect);
            auto &stack_entry = stack.back();
            const StorageTree::ChildArray& children = std::get<StorageTree::ChildArray>(stack_entry.node->children);
            Length sublen {0};
            for(stack_entry.index = 0; stack_entry.index< stack_entry.node->childCount; stack_entry.index++)
            {
                auto subtreelen = stack_entry.node->offsets[stack_entry.index];
                if(rep(offset) < rep(subtreelen)+ rep(accumulated))
                {
                    stack.push_back({children[stack_entry.index++].get()});
                    accumulated = accumulated + sublen;
                    break;
                }
                algo_mark(children[stack_entry.index]->shared_from_this(), Traverse);
                sublen = subtreelen;
            }
        }
        const StorageTree::LeafArray& children = std::get<StorageTree::LeafArray>(stack.back().node->children);
        algo_mark(stack.back().node->shared_from_this(), Collect);
        Length sublen {0};
        for(stack.back().index = 0; stack.back().index< stack.back().node->childCount;stack.back().index++)
        {
            auto subtreelen = stack.back().node->offsets[stack.back().index];
            if(rep(offset) < rep(subtreelen) + rep(accumulated))
            {
                accumulated = accumulated + sublen;
                break;
            }
            sublen = subtreelen;
        }
        if(stack.back().index== stack.back().node->childCount)
        {
            stack.clear();
            first_ptr=nullptr;
            last_ptr=nullptr;
            return;
        }
        auto& piece = children[stack.back().index++].piece;
        auto* buffer = buffers->buffer_at(piece.index);
        auto first_offset = buffers->buffer_offset(piece.index, piece.first);
        auto last_offset = buffers->buffer_offset(piece.index, piece.last);
        first_ptr = buffer->buffer.data() + rep(first_offset) + rep(offset) - rep(accumulated);
        last_ptr = buffer->buffer.data() + rep(last_offset);
    }

    void TreeWalker::fast_forward_to(CharOffset offset)
    {
        seek(offset);
    }

    void TreeWalker::populate_ptrs()
    {
        if (exhausted())
            return;
        while (stack.back().node->childCount == stack.back().index)
        {
            stack.pop_back();
            if(stack.empty())return;
        }
        while(!stack.back().node->isLeaf())
        {
            algo_mark(stack.back().node->shared_from_this(), Traverse);
            const StorageTree::ChildArray& children = std::get<StorageTree::ChildArray>(stack.back().node->children);
            size_t childIndex = stack.back().index++;
            stack.push_back({children[childIndex].get(), 0});
        }
        algo_mark(stack.back().node->shared_from_this(), Traverse);
        const StorageTree::LeafArray leafs = std::get<StorageTree::LeafArray>(stack.back().node->children);

        auto& piece = leafs[stack.back().index++].piece;
        auto* buffer = buffers->buffer_at(piece.index);
        auto first_offset = buffers->buffer_offset(piece.index, piece.first);
        auto last_offset = buffers->buffer_offset(piece.index, piece.last);
        first_ptr = buffer->buffer.data() + rep(first_offset);
        last_ptr = buffer->buffer.data() + rep(last_offset);
    }

    Length TreeWalker::remaining() const
    {
        return meta.total_content_length - distance(CharOffset{}, total_offset);
    }

    ReverseTreeWalker::ReverseTreeWalker(const Tree* tree, CharOffset offset):
        buffers{ &tree->buffers },
        root{ tree->root },
        meta{ tree->meta },
        stack{ { root.root_ptr()} },
        total_offset{ offset }
    {
        fast_forward_to(offset);
    }

    ReverseTreeWalker::ReverseTreeWalker(const OwningSnapshot* snap, CharOffset offset):
        buffers{ &snap->buffers },
        root{ snap->root },
        meta{ snap->meta },
        stack{ { root.root_ptr() } },
        total_offset{ offset }
    {
        fast_forward_to(offset);
    }

    ReverseTreeWalker::ReverseTreeWalker(const ReferenceSnapshot* snap, CharOffset offset):
        buffers{ snap->buffers },
        root{ snap->root },
        meta{ snap->meta },
        stack{ { root.root_ptr() } },
        total_offset{ offset }
    {
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
        return *(--first_ptr);
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
        if (stack.empty())
            return true;
        // If we have not exhausted the pointers, we're still active.
        if (first_ptr != last_ptr)
            return false;
        // If there's more than one entry on the stack, we're still active.
        for(int i =0; i < stack.size(); i++)
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
        while (stack.back().node->childCount == stack.back().index)
        {
            stack.pop_back();
            if(stack.empty())return;
        }
        while(!stack.back().node->isLeaf())
        {
            const StorageTree::ChildArray& children = std::get<StorageTree::ChildArray>(stack.back().node->children);
            stack.back().index++;
            stack.push_back({children[stack.back().node->childCount - stack.back().index].get(), 0});
        }

        const StorageTree::LeafArray leafs = std::get<StorageTree::LeafArray>(stack.back().node->children);

        stack.back().index++;
        auto& piece = leafs[stack.back().node->childCount - stack.back().index].piece;
        auto* buffer = buffers->buffer_at(piece.index);
        auto first_offset = buffers->buffer_offset(piece.index, piece.first);
        auto last_offset = buffers->buffer_offset(piece.index, piece.last);
        last_ptr = buffer->buffer.data() + rep(first_offset);
        first_ptr = buffer->buffer.data() + rep(last_offset);


    }

    void ReverseTreeWalker::fast_forward_to(CharOffset offset)
    {
        seek(offset);
    }


    void ReverseTreeWalker::seek(CharOffset offset)
    {
        stack.clear();
        if(!root.root_ptr())return;
        stack.push_back({ root.root_ptr() });
        total_offset = offset;

        Length accumulated {0};

        while(!stack.back().node->isLeaf())
        {
            algo_mark(stack.back().node->shared_from_this(), Collect);
            auto &stack_entry = stack.back();
            const StorageTree::ChildArray& children = std::get<StorageTree::ChildArray>(stack_entry.node->children);
            Length sublen {0};
            for(stack_entry.index = 0; stack_entry.index< stack_entry.node->childCount; stack_entry.index++)
            {
                auto subtreelen = stack_entry.node->offsets[stack_entry.index];
                if(rep(offset) < rep(subtreelen)+ rep(accumulated))
                {
                    auto index = stack_entry.index++;
                    stack_entry.index = stack_entry.node->childCount - index;
                    stack.push_back({children[index].get()});
                    accumulated = accumulated + sublen;
                    break;
                }
                algo_mark(children[stack_entry.index]->shared_from_this(), Traverse);
                sublen = subtreelen;
            }
        }
        const StorageTree::LeafArray& children = std::get<StorageTree::LeafArray>(stack.back().node->children);
        algo_mark(stack.back().node->shared_from_this(), Collect);
        for(stack.back().index = 0; stack.back().index< stack.back().node->childCount;stack.back().index++)
        {
            auto subtreelen = children[stack.back().index].piece.length;
            if(rep(offset) < rep(subtreelen) + rep(accumulated))
            {
                break;
            }
            accumulated = accumulated + subtreelen;
        }

        auto& piece = children[stack.back().index++].piece;
        stack.back().index = stack.back().node->childCount - stack.back().index+1;
        auto* buffer = buffers->buffer_at(piece.index);
        auto first_offset = buffers->buffer_offset(piece.index, piece.first);
        //auto last_offset = buffers->buffer_offset(piece.index, piece.last);
        first_ptr  = buffer->buffer.data() + rep(first_offset) + rep(offset)- rep(accumulated) +1;
        last_ptr = buffer->buffer.data() + rep(first_offset);
    }
}

void print_tree(const RatchetPieceTree::StorageTree::Node& root, const RatchetPieceTree::Tree* tree, int level , size_t node_offset )
{

    const char* levels = "|||||||||||||||||||||||||||||||";
    //auto this_offset = node_offset;
    printf("%.*sme: %p, numch: %zd leaf: %s\n",level, levels, &root, root.childCount, root.isLeaf()?"yes":"no");
    //printf("%.*s  :\n",level, levels);
    if(root.isLeaf()){
        for(int i = 0; i < root.childCount;i++){
            print_piece(std::get<RatchetPieceTree::StorageTree::LeafArray>(root.children)[i].piece, tree, level+1);
        }
    }
    else
    {
        for(int i = 0; i < root.childCount;i++){
             RatchetPieceTree::StorageTree::NodePtr childptr = std::get<RatchetPieceTree::StorageTree::ChildArray>(root.children)[i];
            RatchetPieceTree::Length sublen = root.offsets[i];
            printf("%p, %d,", childptr.get(), (int)rep(sublen));
        }
        printf("\n");
        for(int i = 0; i < root.childCount;i++){
             RatchetPieceTree::StorageTree::NodePtr childptr = std::get<RatchetPieceTree::StorageTree::ChildArray>(root.children)[i];

            RatchetPieceTree::Length sublen = root.offsets[i];
            print_tree(*childptr , tree, level + 1, node_offset+rep(sublen));
        }
    }
    //printf("%.*sleft_len{%zd}, left_lf{%zd}, node_offset{%zd}\n", level, levels, rep(root.root().left_subtree_length), rep(root.root().left_subtree_lf_count), this_offset);
}

void print_buffer(const RatchetPieceTree::Tree* tree)
{
    printf("--- Entire Buffer ---\n");
    RatchetPieceTree::TreeWalker walker{ tree };
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
}