
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

void print_tree(const PieceTree::StorageTree::Node& root, const PieceTree::Tree* tree, int level = 0, size_t node_offset = 0);

namespace PieceTree
{
    constexpr LFCount operator+(LFCount lhs, LFCount rhs)
    {
        return LFCount{ rep(lhs) + rep(rhs) };
    }
    
    PieceTree::LFCount tree_lf_count(const StorageTree& root)
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

        PieceTree::Length tree_length(const StorageTree& root)
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
        std::vector<NodePtr> nodes;
        size_t i = 0;
        for(; i+ MaxChildren*2 < leafNodes.size(); i+=MaxChildren)
        {
            nodes.push_back(construct_leaf(leafNodes, i, i+MaxChildren));
        }
        size_t remaining = leafNodes.size() - i;
        assert(nodes.empty() ||
            (remaining <= (MaxChildren*2) && remaining >= MaxChildren));
        nodes.push_back(construct_leaf(leafNodes, i, i+remaining/2));
        i+=remaining/2;
        nodes.push_back(construct_leaf(leafNodes, i, leafNodes.size()));
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
        size_t newNumChildren = std::visit([](auto v)->size_t{return v.size();}, result.children);
        if(newNumChildren > MaxChildren)
        {
            size_t numLChild = newNumChildren/2;
            
            
            NodePtr left;
            NodePtr right;
            if(std::holds_alternative<std::vector<NodeData>>(result.children))
            {
                left = construct_leaf(std::get<std::vector<NodeData> >(result.children), 0, numLChild);
                right = construct_leaf(std::get<std::vector<NodeData> >(result.children), numLChild, newNumChildren);
            }
            else
            {
                left = construct_internal(std::get<std::vector<NodePtr> >(result.children), 0, numLChild);
                right = construct_internal(std::get<std::vector<NodePtr> >(result.children), numLChild, newNumChildren);
            }
            
            std::array<NodePtr, MaxChildren> new_root_children;
            std::array<Length, MaxChildren> new_root_offsets;
            new_root_children[0] = left;
            new_root_offsets[0] = left->subTreeLength();
            new_root_children[1] = right;
            new_root_offsets[1] = left->subTreeLength()+right->subTreeLength();
            NodePtr new_root = std::make_shared<Node>(new_root_children, new_root_offsets, 2);
            
            return B_Tree<MaxChildren>(new_root);
            
        }
        else 
        {

            if(std::holds_alternative<std::vector<NodeData>>(result.children))
            {
                NodePtr new_root = construct_leaf(std::get<std::vector<NodeData> >(result.children), 0, newNumChildren);
                return B_Tree<MaxChildren>(new_root);
            }
            else
            {
                NodePtr new_root = construct_internal(std::get<std::vector<NodePtr> >(result.children), 0, newNumChildren);
                return B_Tree<MaxChildren>(new_root);
            }
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



    template<size_t MaxChildren>
    B_Tree<MaxChildren>::TreeManipResult B_Tree<MaxChildren>::insertInto_leaf(const Node* node, const NodeData& x, Length at, BufferCollection* buffers) const
    {
        auto offsets_end = node->offsets.begin()+node->childCount;
        auto insertPoint = std::lower_bound(node->offsets.begin(), offsets_end, at);
        auto insertIndex = insertPoint - node->offsets.begin();
        std::vector<NodeData> resultch;
        
        auto children = std::get<std::array<NodeData, MaxChildren>>(node->children);
        
        auto child_it = children.begin();
        auto child_insert = children.begin()+insertIndex;
        auto child_end = children.begin()+node->childCount;
        
        Length acc { 0};
        for(; child_it!=child_insert; ++child_it)
        {
            resultch.push_back(*child_it);
            acc = acc + (*child_it).piece.length;
        }
        Length split_offset = at - acc;
        NodeData second{};
        if(split_offset > Length{0})
        {
        
            auto splitting_piece = (*child_it).piece;
            
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
            second = NodeData
            {
                .piece = new_piece_right
            };
            resultch.push_back(first);
            
        }
        resultch.push_back(x);
        
        if(split_offset > Length{0}){
            //split insert point
            resultch.push_back(second);
            ++child_it;
        }
        
        for(; child_it!=child_end; ++child_it)
        {
            resultch.push_back(*child_it);
        }
        B_Tree<MaxChildren>::TreeManipResult res;
        res.children = std::move(resultch);
        res.violated_invariant = ~size_t(0);
        return res;
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
        
        if(node->isLeaf())
        {
            return insertInto_leaf(node, x, at, buffers);
        }
        else
        {
            std::array<NodePtr, MaxChildren> node_children = std::move(std::get<std::array<NodePtr, MaxChildren>>(node->children));
            auto offsets_end = node->offsets.begin()+node->childCount;
            auto insertPoint = std::lower_bound(node->offsets.begin(), offsets_end, at);
            size_t insertIndex = insertPoint - node->offsets.begin();
        
            
            auto insert_child = node_children[insertIndex];
            
            auto insert_result = insertInto(insert_child.get(), x, *insertPoint - insert_child->subTreeLength(), buffers);
            
            auto insertChIt = node_children.begin()+insertIndex;
            
            std::vector<NodePtr> resultch;
            
            auto child_it = (node_children).begin();
             
            
            for(; child_it!=insertChIt; ++child_it)
            {
                resultch.push_back(*child_it);
                
            }
            
            size_t newNumChildren = std::visit([](auto v){return v.size();}, insert_result.children);
            if(newNumChildren > MaxChildren)
            {
                //split the to-be inserted node into two
                size_t numLChild = newNumChildren/2;
                
                NodePtr leftChild;
                NodePtr rightChild;
                if(std::holds_alternative<std::vector<NodeData>>(insert_result.children))
                {
                    leftChild = construct_leaf( std::get<std::vector<NodeData>>(insert_result.children), 0, numLChild);
                    rightChild = construct_leaf( std::get<std::vector<NodeData>>(insert_result.children), numLChild, newNumChildren);
                }
                else
                {
                    leftChild = construct_internal( std::get<std::vector<NodePtr>>(insert_result.children), 0, numLChild);
                    rightChild = construct_internal( std::get<std::vector<NodePtr>>(insert_result.children), numLChild, newNumChildren);
                }
                
                resultch.push_back(std::move(leftChild));
                
                resultch.push_back(std::move(rightChild));
                
                ++child_it;
                
            }
            else
            {
                auto newChild = construct_internal(std::get<std::vector<NodePtr> >(insert_result.children), 0, newNumChildren);
                resultch.push_back(newChild);
                
                ++child_it;
            }
            
            for(; child_it!=node_children.end();  ++child_it)
            {
                resultch.push_back(*child_it);
                
            }
            B_Tree<MaxChildren>::TreeManipResult res;
            res.children = std::move(resultch);
            
            res.violated_invariant = ~((size_t)0);
            return res;
            
        }
        
        
        // return {};
    }
    
    template<size_t MaxChildren>
    B_Tree<MaxChildren>::NodePtr B_Tree<MaxChildren>::construct_leaf(std::vector<NodeData> data, size_t begin, size_t end) const
    {
        size_t numChild = end-begin;
        std::array<NodeData, MaxChildren> new_left_children;
        std::array<Length, MaxChildren>  new_left_offsets;
        Length acc{0};
        for(int i = 0; i < numChild; i++)
        {
            new_left_children[i] =data[begin+i];
            acc = acc + data[begin+i].piece.length;
            new_left_offsets[i] = acc;
            
        }
        
        return std::make_shared<Node>(new_left_children, new_left_offsets,  end-begin);
    }
    
    
    template<size_t MaxChildren>
    B_Tree<MaxChildren>::NodePtr B_Tree<MaxChildren>::construct_internal(std::vector<NodePtr> data, size_t begin, size_t end) const
    {
        size_t numChild = end-begin;
        std::array<NodePtr, MaxChildren> new_left_children;
        std::array<Length, MaxChildren>  new_left_offsets;
        Length acc{0};
        for(int i = 0; i < numChild; i++)
        {
            new_left_children[i] =data[begin+i];
             acc = acc + data[begin+i]->subTreeLength();
            new_left_offsets[i] = acc;
            
        }
        
        return std::make_shared<Node>(new_left_children, new_left_offsets,  end-begin);
    }
    
    template<size_t MaxChildren>
    const B_Tree<MaxChildren>::Node* B_Tree<MaxChildren>::root_ptr() const
    {
        return root_node.get();
    }
    
    void foo(){
        B_Tree<10> tree{};
        Offset at={
            
        };
        NodeData data={
            .piece = {
                .length =Length{ 5}
            }
        };
        tree.insert(data, at, nullptr);
    }
    

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
        Length offset = { };
        std::vector<NodeData> leafNodes;
        std::vector<Length> offsets;
        
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
            //root = root.insert({ piece }, offset);
            offset = offset + piece.length;
            leafNodes.push_back({piece});
            offsets.push_back(offset);
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
    
    void Tree::get_line_content(std::string* buf, Line line) const
    {
        // Reset the buffer.
        buf->clear();
        if (line == Line::IndexBeginning)
            return;
        assemble_line(buf, root, line);
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
    
    void print_piece(const PieceTree::Piece& piece, const PieceTree::Tree* tree, int level)
    {
        const char* levels = "|||||||||||||||||||||||||||||||";
        printf("%.*sidx{%zd}, first{l{%zd}, c{%zd}}, last{l{%zd}, c{%zd}}, len{%zd}, lf{%zd}\n",
                level, levels,
                rep(piece.index), rep(piece.first.line), rep(piece.first.column),
                                  rep(piece.last.line), rep(piece.last.column),
                    rep(piece.length), rep(piece.newline_count));
        auto* buffer = tree->buffers.buffer_at(piece.index);
        auto offset = tree->buffers.buffer_offset(piece.index, piece.first);
        printf("%.*sPiece content: %.*s\n", level, levels, static_cast<int>(piece.length), buffer->buffer.data() + rep(offset));
    }
    void print_tree(const PieceTree::Tree& tree)
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
    
    void Tree::internal_insert(CharOffset offset, std::string_view txt)
    {
        assert(not txt.empty());
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
}


void print_tree(const PieceTree::StorageTree::Node& root, const PieceTree::Tree* tree, int level , size_t node_offset )
{
    
    const char* levels = "|||||||||||||||||||||||||||||||";
    //auto this_offset = node_offset;
    printf("%.*sme: %p, leaf: %s\n",level, levels, &root, root.isLeaf()?"yes":"no");
    printf("%.*s  :",level, levels);
    if(root.isLeaf()){
        for(int i = 0; i < root.childCount;i++){
            print_piece(std::get<PieceTree::StorageTree::LeafArray>(root.children)[i].piece, tree, level);
        }
        
    }
    else
    {
        for(int i = 0; i < root.childCount;i++){
             PieceTree::StorageTree::NodePtr childptr = std::get<PieceTree::StorageTree::ChildArray>(root.children)[i];
            PieceTree::Length sublen = root.offsets[i];
            printf("%p, %d,", childptr.get(), (int)rep(sublen));
        }
        printf("\n");
        for(int i = 0; i < root.childCount;i++){
             PieceTree::StorageTree::NodePtr childptr = std::get<PieceTree::StorageTree::ChildArray>(root.children)[i];
            
            PieceTree::Length sublen = root.offsets[i];
            print_tree(*childptr , tree, level + 1, node_offset+rep(sublen));
        }
    }
    //printf("%.*sleft_len{%zd}, left_lf{%zd}, node_offset{%zd}\n", level, levels, rep(root.root().left_subtree_length), rep(root.root().left_subtree_lf_count), this_offset);
}

void print_buffer(const PieceTree::Tree* tree)
{
    printf("--- Entire Buffer ---\n");
    PieceTree::TreeWalker walker{ tree };
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