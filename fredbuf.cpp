#include "fredbuf.h"

#include <cassert>

#include "enum-utils.h"
#include "macros.h"
#include "os.h"

#include "scope-guard.h"
#include "util.h"

namespace PieceTree
{
#ifdef COUNT_ALLOC
     size_t alloc_count;
     size_t dealloc_count;
#endif
    constexpr LFCount operator+(LFCount lhs, LFCount rhs)
    {
        return LFCount{ rep(lhs) + rep(rhs) };
    }

    namespace
    {
        bool nil_node(const RBNodeCounted* node)
        {
            return node == &null_node_inst;
        }

        RBNodeCounted* nil_node()
        {
            return &null_node_inst;
        }
    } // namespace [anon]

    // Counted node management.
    void dec_node_ref(const RBNodeCounted* node)
    {
        if (nil_node(node))
            return;
        uint64_t count = os_atomic_u64_dec_eval(&node->blk->ref_count);
        if (count == 0)
        {
            dec_node_ref(node->payload.left);
            dec_node_ref(node->payload.right);
            // Finally, detach this node and add to the free list atomically.
            RBNodeFreeList old_head{};
            RBNodeFreeList next_head{};
            static_assert(alignof(RBNodeFreeList) == 16);
            os_atomic_u128_eval(&node->blk->base_blk->free_list, &old_head);
            RBNodeCounted* mut_node = const_cast<RBNodeCounted*>(node);
            do
            {
                mut_node->free_next = old_head.head;
                next_head.head = mut_node;
                next_head.tag = old_head.tag + 1;
            } while (not os_atomic_u128_eval_cond_assign(&node->blk->base_blk->free_list, next_head, &old_head));
        }
    }
    
    const RBNodeCounted* take_node_ref(const RBNodeCounted* node)
    {
        if (not nil_node(node))
        {
            FRED_UNUSED_RESULT(os_atomic_u64_inc_eval(&node->blk->ref_count));
        }
        return node;
    }

    const RBNodeCounted* make_node(RBTreeBlock* blk, Color c, const RBNodeCounted* lft, const NodeData& data, const RBNodeCounted* rgt)
    {
        RBNodeCounted* node = nil_node();
        RBNodeBlock* node_blk = nullptr;
        // Try to fetch a node from the free list atomically.
        {
            RBNodeFreeList old_head;
            RBNodeFreeList next_head{};
            os_atomic_u128_eval(&blk->free_list, &old_head);
            do
            {
                if (nil_node(old_head.head))
                {
                    node = nil_node();
                    break;
                }
                node = old_head.head;
                next_head.head = old_head.head->free_next;
                next_head.tag = old_head.tag + 1;
            } while (not os_atomic_u128_eval_cond_assign(&blk->free_list, next_head, &old_head));
            if (not nil_node(node))
            {
                node_blk = node->blk;
            }
        }

        if (nil_node(node))
        {
            node = Arena::push_array_no_zero<RBNodeCounted>(blk->alloc_arena, 1);
            node_blk = Arena::push_array<RBNodeBlock>(blk->alloc_arena, 1);
        }
        zero_bytes(node);
        zero_bytes(node_blk);
        // Set pointer trees to null.
        node->payload.left = node->payload.right = nil_node();
        node_blk->base_blk = blk;

        node->payload.left = take_node_ref(lft);
        node->payload.data = data;
        node->payload.right = take_node_ref(rgt);
        node->payload.color = c;
        node->blk = node_blk;
        return node;
    }

    RedBlackTree::RedBlackTree(RedBlackTree&& other):
        root_node{ other.root_node }
    {
        other.root_node = nil_node();
    }

    RedBlackTree& RedBlackTree::operator=(RedBlackTree&& other)
    {
        // Swap these, so the dtor for 'other' will handle decrementing the ref.
        const RBNodeCounted* old_root = root_node;
        root_node = other.root_node;
        other.root_node = old_root;
        return *this;
    }

    RedBlackTree::~RedBlackTree()
    {
        dec_node_ref(root_node);
    }

    const RBNodeCounted* RedBlackTree::root_ptr() const
    {
        return root_node;
    }

    bool RedBlackTree::is_empty() const
    {
        return nil_node(root_node);
    }

    const NodeData& RedBlackTree::root() const
    {
        assert(not is_empty());
        return root_node->payload.data;
    }

    RedBlackTree RedBlackTree::left() const
    {
        assert(not is_empty());
        return RedBlackTree(root_node->payload.left);
    }

    RedBlackTree RedBlackTree::right() const
    {
        assert(not is_empty());
        return RedBlackTree(root_node->payload.right);
    }

    Color RedBlackTree::root_color() const
    {
        assert(!is_empty());
        return root_node->payload.color;
    }

    RedBlackTree RedBlackTree::insert(RBTreeBlock* blk, const NodeData& x, Offset at) const
    {
        RedBlackTree t = ins(blk, x, at, Offset{ 0 });
        return RedBlackTree(blk, Color::Black, t.left(), t.root(), t.right());
    }

    RedBlackTree::RedBlackTree(RBTreeBlock* blk,
                Color c,
                const RedBlackTree& lft,
                const NodeData& val,
                const RedBlackTree& rgt)
        : root_node(take_node_ref(make_node(blk, c, lft.root_node, attribute(val, lft), rgt.root_node)))
    {
    }

    RedBlackTree::RedBlackTree(const RBNodeCounted* node):
        root_node(take_node_ref(node))
    {
    }

    RedBlackTree RedBlackTree::ins(RBTreeBlock* blk, const NodeData& x, Offset at, Offset total_offset) const
    {
        if (is_empty())
            return RedBlackTree(blk, Color::Red, RedBlackTree(), x, RedBlackTree());
        const NodeData& y = root();
        if (at < total_offset + y.left_subtree_length + y.piece.length)
            return balance(blk, root_color(), left().ins(blk, x, at, total_offset), y, right());
        return balance(blk, root_color(), left(), y, right().ins(blk, x, at, total_offset + y.left_subtree_length + y.piece.length));
    }

    RedBlackTree RedBlackTree::balance(RBTreeBlock* blk, Color c, const RedBlackTree& lft, const NodeData& x, const RedBlackTree& rgt)
    {
        if (c == Color::Black and lft.doubled_left())
            return RedBlackTree(blk, Color::Red,
                                lft.left().paint(blk, Color::Black),
                                lft.root(),
                                RedBlackTree(blk, Color::Black,
                                                lft.right(),
                                                x,
                                                rgt));
        else if (c == Color::Black and lft.doubled_right())
            return RedBlackTree(blk, Color::Red,
                                RedBlackTree(blk, Color::Black,
                                                lft.left(),
                                                lft.root(),
                                                lft.right().left()),
                                lft.right().root(),
                                RedBlackTree(blk, Color::Black,
                                                lft.right().right(),
                                                x,
                                                rgt));
        else if (c == Color::Black and rgt.doubled_left())
            return RedBlackTree(blk, Color::Red,
                                RedBlackTree(blk, Color::Black,
                                                lft,
                                                x,
                                                rgt.left().left()),
                                rgt.left().root(),
                                RedBlackTree(blk, Color::Black,
                                                rgt.left().right(),
                                                rgt.root(),
                                                rgt.right()));
        else if (c == Color::Black and rgt.doubled_right())
            return RedBlackTree(blk, Color::Red,
                                RedBlackTree(blk, Color::Black,
                                                lft,
                                                x,
                                                rgt.left()),
                                rgt.root(),
                                rgt.right().paint(blk, Color::Black));
        return RedBlackTree(blk, c, lft, x, rgt);
    }

    bool RedBlackTree::doubled_left() const
    {
        return not is_empty()
                and root_color() == Color::Red
                and not left().is_empty()
                and left().root_color() == Color::Red;
    }

    bool RedBlackTree::doubled_right() const
    {
        return not is_empty()
                and root_color() == Color::Red
                and not right().is_empty()
                and right().root_color() == Color::Red;
    }

    RedBlackTree RedBlackTree::paint(RBTreeBlock* blk, Color c) const
    {
        assert(not is_empty());
        return RedBlackTree(blk, c, left(), root(), right());
    }

    PieceTree::Length tree_length(const RedBlackTree& root)
    {
        if (root.is_empty())
            return { };
        return root.root().left_subtree_length + root.root().piece.length + tree_length(root.right());
    }

    PieceTree::LFCount tree_lf_count(const RedBlackTree& root)
    {
        if (root.is_empty())
            return { };
        return root.root().left_subtree_lf_count + root.root().piece.newline_count + tree_lf_count(root.right());
    }

    NodeData attribute(const NodeData& data, const RedBlackTree& left)
    {
        auto new_data = data;
        new_data.left_subtree_length = tree_length(left);
        new_data.left_subtree_lf_count = tree_lf_count(left);
        return new_data;
    }

    struct WalkResult
    {
        RedBlackTree tree;
        Offset accumulated_offset;
    };

    WalkResult pred(const RedBlackTree& root, Offset start_offset)
    {
        RedBlackTree t = root.left();
        while (!t.right().is_empty())
        {
            start_offset = start_offset + t.root().left_subtree_length + t.root().piece.length;
            t = t.right();
        }
        // Add the final offset from the last right node.
        start_offset = start_offset + t.root().left_subtree_length;
        return { .tree = t.dup(), .accumulated_offset = start_offset };
    }

#ifdef EXPERIMENTAL_REMOVE
    struct RedBlackTree::ColorTree
    {
        const Color color;
        const RedBlackTree tree;

        static ColorTree double_black()
        {
            return ColorTree();
        }

        explicit ColorTree(RedBlackTree const &tree)
            : color(tree.is_empty() ? Color::Black : tree.root_color()), tree(tree)
        {
        }

        explicit ColorTree(Color c, const RedBlackTree& lft, const NodeData& x, const RedBlackTree& rgt)
            : color(c), tree(c, lft, x, rgt)
        {
        }

    private:
        ColorTree(): color(Color::DoubleBlack)
        {
        }
    };
    RedBlackTree RedBlackTree::remove(Offset at) const
    {
        auto t = rem(at, Offset{ 0 }).tree;
        if (t.is_empty())
            return RedBlackTree();
        return RedBlackTree(Color::Black, t.left(), t.root(), t.right());
    }

    RedBlackTree::ColorTree RedBlackTree::remove_double_black(Color c, ColorTree const &lft, const NodeData& x, ColorTree const &rgt)
    {
        if (lft.color == Color::DoubleBlack)
        {
            auto left = lft.tree.is_empty() ? RedBlackTree() : lft.tree.paint(Color::Black);

            if (rgt.color == Color::Black)
            {
                assert(c != Color::DoubleBlack);
                return ColorTree(extend(c), left, x, rgt.tree.paint(Color::Red));
            }
            else
                return ColorTree(Color::Black, RedBlackTree(Color::Black, left, x, rgt.tree.left().paint(Color::Red)), rgt.tree.root(), rgt.tree.right());
        }
        else if (rgt.color == Color::DoubleBlack)
        {
            auto right = rgt.tree.is_empty() ? RedBlackTree() : rgt.tree.paint(Color::Black);

            if (lft.color == Color::Black)
            {
                assert(c != Color::DoubleBlack);
                return ColorTree(extend(c), lft.tree.paint(Color::Red), x, right);
            }
            else
                return ColorTree(Color::Black, lft.tree.left(), lft.tree.root(), RedBlackTree(Color::Black, lft.tree.right().paint(Color::Red), x, right));
        }
        else
            return ColorTree(c, lft.tree, x, rgt.tree);
    }

    RedBlackTree::ColorTree RedBlackTree::rem(Offset at, Offset total) const
    {
        if (is_empty())
            return ColorTree(RedBlackTree());
        const NodeData& y = root();
        if (at < total + y.left_subtree_length)
            return remove_double_black(root_color(), left().rem(at, total), y, ColorTree(right()));
        if (at == total + y.left_subtree_length)
            return remove_node();
        return remove_double_black(root_color(), ColorTree(left()), y, right().rem(at, total + y.left_subtree_length + y.piece.length));
    }

    RedBlackTree::ColorTree RedBlackTree::remove_node() const
    {
        if (not left().is_empty()
            and not right().is_empty())
        {
            auto [p, off] = pred(*this, Offset(0));
            const NodeData& x = p.root();

            Color c = root_color();

            return remove_double_black(c, left().rem(off, Offset(0)), x, ColorTree(right()));
        }
        else if (not left().is_empty())
        {
            return ColorTree(left().paint(Color::Black));
        }
        else if (not right().is_empty())
        {
            return ColorTree(right().paint(Color::Black));
        }
        else if (root_color() == Color::Black)
        {
            return ColorTree::double_black();
        }
        return ColorTree(RedBlackTree());
    }
#else
    RedBlackTree RedBlackTree::remove(RBTreeBlock* blk, Offset at) const
    {
        auto t = rem(blk, *this, at, Offset{ 0 });
        if (t.is_empty())
            return RedBlackTree();
        return RedBlackTree(blk, Color::Black, t.left(), t.root(), t.right());
    }

    RedBlackTree RedBlackTree::dup() const
    {
        return RedBlackTree(root_node);
    }

    RedBlackTree RedBlackTree::fuse(RBTreeBlock* blk, const RedBlackTree& left, const RedBlackTree& right)
    {
        // match: (left, right)
        // case: (None, r)
        if (left.is_empty())
            return right.dup();
        if (right.is_empty())
            return left.dup();
        // match: (left.color, right.color)
        // case: (B, R)
        if (left.root_color() == Color::Black and right.root_color() == Color::Red)
        {
            return RedBlackTree(blk, Color::Red,
                                fuse(blk, left, right.left()),
                                right.root(),
                                right.right());
        }
        // case: (R, B)
        if (left.root_color() == Color::Red and right.root_color() == Color::Black)
        {
            return RedBlackTree(blk, Color::Red,
                                left.left(),
                                left.root(),
                                fuse(blk, left.right(), right));
        }
        // case: (R, R)
        if (left.root_color() == Color::Red and right.root_color() == Color::Red)
        {
            auto fused = fuse(blk, left.right(), right.left());
            if (not fused.is_empty() and fused.root_color() == Color::Red)
            {
                auto new_left = RedBlackTree(blk, Color::Red,
                                                left.left(),
                                                left.root(),
                                                fused.left());
                auto new_right = RedBlackTree(blk, Color::Red,
                                                fused.right(),
                                                right.root(),
                                                right.right());
                return RedBlackTree(blk, Color::Red,
                                    new_left,
                                    fused.root(),
                                    new_right);
            }
            auto new_right = RedBlackTree(blk, Color::Red,
                                            fused,
                                            right.root(),
                                            right.right());
            return RedBlackTree(blk, Color::Red,
                                left.left(),
                                left.root(),
                                new_right);
        }
        // case: (B, B)
        assert(left.root_color() == right.root_color() and left.root_color() == Color::Black);
        auto fused = fuse(blk, left.right(), right.left());
        if (not fused.is_empty() and fused.root_color() == Color::Red)
        {
            auto new_left = RedBlackTree(blk, Color::Black,
                                            left.left(),
                                            left.root(),
                                            fused.left());
            auto new_right = RedBlackTree(blk, Color::Black,
                                            fused.right(),
                                            right.root(),
                                            right.right());
            return RedBlackTree(blk, Color::Red,
                                new_left,
                                fused.root(),
                                new_right);
        }
        auto new_right = RedBlackTree(blk, Color::Black,
                                        fused,
                                        right.root(),
                                        right.right());
        auto new_node = RedBlackTree(blk, Color::Red,
                                        left.left(),
                                        left.root(),
                                        new_right);
        return balance_left(blk, new_node);
    }

    RedBlackTree RedBlackTree::balance(RBTreeBlock* blk, const RedBlackTree& node)
    {
        // Two red children.
        if (not node.left().is_empty()
            and node.left().root_color() == Color::Red
            and not node.right().is_empty()
            and node.right().root_color() == Color::Red)
        {
            auto l = node.left().paint(blk, Color::Black);
            auto r = node.right().paint(blk, Color::Black);
            return RedBlackTree(blk,
                                Color::Red,
                                l,
                                node.root(),
                                r);
        }

        assert(node.root_color() == Color::Black);
        return balance(blk, node.root_color(), node.left(), node.root(), node.right());
    }

    RedBlackTree RedBlackTree::balance_left(RBTreeBlock* blk, const RedBlackTree& left)
    {
        // match: (color_l, color_r, color_r_l)
        // case: (Some(R), ..)
        if (not left.left().is_empty() and left.left().root_color() == Color::Red)
        {
            return RedBlackTree(blk, Color::Red,
                                left.left().paint(blk, Color::Black),
                                left.root(),
                                left.right());
        }
        // case: (_, Some(B), _)
        if (not left.right().is_empty() and left.right().root_color() == Color::Black)
        {
            auto new_left = RedBlackTree(blk, Color::Black,
                                            left.left(),
                                            left.root(),
                                            left.right().paint(blk, Color::Red));
            return balance(blk, new_left);
        }
        // case: (_, Some(R), Some(B))
        if (not left.right().is_empty() and left.right().root_color() == Color::Red
            and not left.right().left().is_empty() and left.right().left().root_color() == Color::Black)
        {
            auto unbalanced_new_right = RedBlackTree(blk, Color::Black,
                                                        left.right().left().right(),
                                                        left.right().root(),
                                                        left.right().right().paint(blk, Color::Red));
            auto new_right = balance(blk, unbalanced_new_right);
            auto new_left = RedBlackTree(blk, Color::Black,
                                            left.left(),
                                            left.root(),
                                            left.right().left().left());
            return RedBlackTree(blk, Color::Red,
                                new_left,
                                left.right().left().root(),
                                new_right);
        }
        assert(!"impossible");
        return left.dup();
    }

    RedBlackTree RedBlackTree::balance_right(RBTreeBlock* blk, const RedBlackTree& right)
    {
        // match: (color_l, color_l_r, color_r)
        // case: (.., Some(R))
        if (not right.right().is_empty() and right.right().root_color() == Color::Red)
        {
            return RedBlackTree(blk, Color::Red,
                                right.left(),
                                right.root(),
                                right.right().paint(blk, Color::Black));
        }
        // case: (Some(B), ..)
        if (not right.left().is_empty() and right.left().root_color() == Color::Black)
        {
            auto new_right = RedBlackTree(blk, Color::Black,
                                            right.left().paint(blk, Color::Red),
                                            right.root(),
                                            right.right());
            return balance(blk, new_right);
        }
        // case: (Some(R), Some(B), _)
        if (not right.left().is_empty() and right.left().root_color() == Color::Red
            and not right.left().right().is_empty() and right.left().right().root_color() == Color::Black)
        {
            auto unbalanced_new_left = RedBlackTree(blk, Color::Black,
                                                    // Note: Because 'left' is red, it must have a left child.
                                                    right.left().left().paint(blk, Color::Red),
                                                    right.left().root(),
                                                    right.left().right().left());
            auto new_left = balance(blk, unbalanced_new_left);
            auto new_right = RedBlackTree(blk, Color::Black,
                                            right.left().right().right(),
                                            right.root(),
                                            right.right());
            return RedBlackTree(blk, Color::Red,
                                new_left,
                                right.left().right().root(),
                                new_right);
        }
        assert(!"impossible");
        return right.dup();
    }

    RedBlackTree RedBlackTree::remove_left(RBTreeBlock* blk, const RedBlackTree& root, Offset at, Offset total)
    {
        auto new_left = rem(blk, root.left(), at, total);
        auto new_node = RedBlackTree(blk, Color::Red,
                                        new_left,
                                        root.root(),
                                        root.right());
        // In this case, the root was a red node and must've had at least two children.
        if (not root.left().is_empty()
            and root.left().root_color() == Color::Black)
            return balance_left(blk, new_node);
        return new_node;
    }

    RedBlackTree RedBlackTree::remove_right(RBTreeBlock* blk, const RedBlackTree& root, Offset at, Offset total)
    {
        const NodeData& y = root.root();
        auto new_right = rem(blk, root.right(), at, total + y.left_subtree_length + y.piece.length);
        auto new_node = RedBlackTree(blk, Color::Red,
                                        root.left(),
                                        root.root(),
                                        new_right);
        // In this case, the root was a red node and must've had at least two children.
        if (not root.right().is_empty()
            and root.right().root_color() == Color::Black)
            return balance_right(blk, new_node);
        return new_node;
    }

    RedBlackTree RedBlackTree::rem(RBTreeBlock* blk, const RedBlackTree& root, Offset at, Offset total)
    {
        if (root.is_empty())
            return RedBlackTree();
        const NodeData& y = root.root();
        if (at < total + y.left_subtree_length)
            return remove_left(blk, root, at, total);
        if (at == total + y.left_subtree_length)
            return fuse(blk, root.left(), root.right());
        return remove_right(blk, root, at, total);
    }
#endif // EXPERIMENTAL_REMOVE

#ifdef TEXTBUF_DEBUG
    // Borrowed from https://github.com/dotnwat/persistent-rbtree/blob/master/tree.h:checkConsistency.
    int check_black_node_invariant(const RedBlackTree& node)
    {
        if (node.is_empty())
            return 1;
        if (node.root_color() == Color::Red and
            ((not node.left().is_empty() and node.left().root_color() == Color::Red)
            or (not node.right().is_empty() and node.right().root_color() == Color::Red)))
        {
            return 1;
        }
        auto l = check_black_node_invariant(node.left());
        auto r = check_black_node_invariant(node.right());

        if (l != 0 and r != 0 and l != r)
            return 0;

        if (l != 0 and r != 0)
            return node.root_color() == Color::Red ? l : l + 1;
        return 0;
    }

    void satisfies_rb_invariants(const RedBlackTree& root)
    {
        // 1. Every node is either red or black.
        // 2. All NIL nodes (figure 1) are considered black.
        // 3. A red node does not have a red child.
        // 4. Every path from a given node to any of its descendant NIL nodes goes through the same number of black nodes.

        // The internal nodes in this RB tree can be totally black so we will not count them directly, we'll just track
        // odd nodes as either red or black.
        // Measure the number of black nodes we need to validate.
        if (root.is_empty()
            or (root.left().is_empty() and root.right().is_empty()))
            return;
        assert(check_black_node_invariant(root) != 0);
    }
#endif // TEXTBUF_DEBUG
} // namespace PieceTree

namespace PieceTree
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

        void compute_buffer_meta(BufferMeta* meta, const RedBlackTree& root)
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

    void Tree::build_tree()
    {
        // First, take a reference to this immutable buffer set.
        take_buffer_ref(&buffers);
        // Note: The buffers were populated with valid array starts from the builder.
        // In order to maintain the invariant of other buffers, the mod_buffer needs a single line-start of 0.
        append_mut_buf_start(&buffers, {});
        last_insert = { };

        const auto buf_count = buffers.orig_buffers.count;
        CharOffset offset = { };
        for (size_t i = 0; i < buf_count; ++i)
        {
            const CharBuffer* buf = &buffers.orig_buffers.buffers[i];
            // Enforced by 'populate_line_starts'.
            assert(buf->line_starts.count != 0);
            // If this immutable buffer is empty, we can avoid creating a piece for it altogether.
            if (buf->buffer.size == 0)
                continue;
            auto last_line = Line{ buf->line_starts.count - 1 };
            // Create a new node that spans this buffer and retains an index to it.
            // Insert the node into the balanced tree.
            Piece piece {
                .index = BufferIndex{ i },
                .first = { .line = Line{ 0 }, .column = Column{ 0 } },
                .last = { .line = last_line, .column = Column{ buf->buffer.size - rep(buf->line_starts.starts[rep(last_line)]) } },
                .length = Length{ buf->buffer.size },
                // Note: the number of newlines
                .newline_count = LFCount{ rep(last_line) }
            };
            root = root.insert(buffers.rb_tree_blk, { piece }, offset);
            offset = offset + piece.length;
        }

        compute_buffer_meta();
    }

    void Tree::internal_insert(CharOffset offset, String8 txt)
    {
        assert(txt.size != 0);
        end_last_insert = extend(offset, txt.size);
        ScopeGuard guard{ [&] {
            compute_buffer_meta();
#ifdef TEXTBUF_DEBUG
            satisfies_rb_invariants(root);
#endif // TEXTBUF_DEBUG
        } };
        if (root.is_empty())
        {
            auto piece = build_piece(txt);
            root = root.insert(buffers.rb_tree_blk, { piece }, CharOffset{ 0 });
            return;
        }

        auto result = node_at(&buffers, root.dup(), offset);
        // If the offset is beyond the buffer, just select the last node.
        if (result.node == nullptr)
        {
            auto off = CharOffset{ 0 };
            if (meta.total_content_length != Length{})
            {
                off = off + retract(meta.total_content_length);
            }
            result = node_at(&buffers, root.dup(), off);
        }

        // There are 3 cases:
        // 1. We are inserting at the beginning of an existing node.
        // 2. We are inserting at the end of an existing node.
        // 3. We are inserting in the middle of the node.
        auto [node, remainder, node_start_offset, line] = result;
        assert(node != nullptr);
        auto insert_pos = buffer_position(&buffers, node->piece, remainder);
        // Case #1.
        if (node_start_offset == offset)
        {
            // There's a bonus case here.  If our last insertion point was the same as this piece's
            // last and it inserted into the mod buffer, then we can simply 'extend' this piece by
            // the following process:
            // 1. Fetch the previous node (if we can) and compare.
            // 2. Build the new piece.
            // 3. Remove the old piece.
            // 4. Extend the old piece's length to the length of the newly created piece.
            // 5. Re-insert the new piece.
            if (offset != CharOffset{})
            {
                auto prev_node_result = node_at(&buffers, root.dup(), retract(offset));
                if (prev_node_result.node->piece.index == BufferIndex::ModBuf
                    and prev_node_result.node->piece.last == last_insert)
                {
                    auto new_piece = build_piece(txt);
                    combine_pieces(prev_node_result, new_piece);
                    return;
                }
            }
            auto piece = build_piece(txt);
            root = root.insert(buffers.rb_tree_blk, { piece }, offset);
            return;
        }

        const bool inside_node = offset < node_start_offset + node->piece.length;

        // Case #2.
        if (not inside_node)
        {
            // There's a bonus case here.  If our last insertion point was the same as this piece's
            // last and it inserted into the mod buffer, then we can simply 'extend' this piece by
            // the following process:
            // 1. Build the new piece.
            // 2. Remove the old piece.
            // 3. Extend the old piece's length to the length of the newly created piece.
            // 4. Re-insert the new piece.
            if (node->piece.index == BufferIndex::ModBuf and node->piece.last == last_insert)
            {
                auto new_piece = build_piece(txt);
                combine_pieces(result, new_piece);
                return;
            }
            // Insert the new piece at the end.
            auto piece = build_piece(txt);
            root = root.insert(buffers.rb_tree_blk, { piece }, offset);
            return;
        }

        // Case #3.
        // The basic approach here is to split the existing node into two pieces
        // and insert the new piece in between them.
        auto new_len_right = distance(buffers.buffer_offset(node->piece.index, insert_pos),
                                        buffers.buffer_offset(node->piece.index, node->piece.last));
        auto new_piece_right = node->piece;
        new_piece_right.first = insert_pos;
        new_piece_right.length = new_len_right;
        new_piece_right.newline_count = line_feed_count(&buffers, node->piece.index, insert_pos, node->piece.last);

        // Remove the original node tail.
        auto new_piece_left = trim_piece_right(&buffers, node->piece, insert_pos);

        auto new_piece = build_piece(txt);

        // Remove the original node.
        root = root.remove(buffers.rb_tree_blk, node_start_offset);

        // Insert the left.
        root = root.insert(buffers.rb_tree_blk, { new_piece_left }, node_start_offset);

        // Insert the new mid.
        node_start_offset = node_start_offset + new_piece_left.length;
        root = root.insert(buffers.rb_tree_blk, { new_piece }, node_start_offset);

        // Insert remainder.
        node_start_offset = node_start_offset + new_piece.length;
        root = root.insert(buffers.rb_tree_blk, { new_piece_right }, node_start_offset);
    }

    void Tree::internal_remove(CharOffset offset, Length count)
    {
        assert(rep(count) != 0 and not root.is_empty());
        ScopeGuard guard{ [&] {
            compute_buffer_meta();
#ifdef TEXTBUF_DEBUG
            satisfies_rb_invariants(root);
#endif // TEXTBUF_DEBUG
        } };
        auto first = node_at(&buffers, root.dup(), offset);
        auto last = node_at(&buffers, root.dup(), offset + count);
        auto first_node = first.node;
        auto last_node = last.node;

        auto start_split_pos = buffer_position(&buffers, first_node->piece, first.remainder);

        // Simple case: the range of characters we want to delete are
        // held directly within this node.  Remove the node, resize it
        // then add it back.
        if (first_node == last_node)
        {
            auto end_split_pos = buffer_position(&buffers, first_node->piece, last.remainder);
            // We're going to shrink the node starting from the beginning.
            if (first.start_offset == offset)
            {
                // Delete the entire node.
                if (count == first_node->piece.length)
                {
                    root = root.remove(buffers.rb_tree_blk, first.start_offset);
                    return;
                }
                // Shrink the node.
                auto new_piece = trim_piece_left(&buffers, first_node->piece, end_split_pos);
                // Remove the old one and update.
                root = root.remove(buffers.rb_tree_blk, first.start_offset)
                            .insert(buffers.rb_tree_blk, { new_piece }, first.start_offset);
                return;
            }

            // Trim the tail of this piece.
            if (first.start_offset + first_node->piece.length == offset + count)
            {
                auto new_piece = trim_piece_right(&buffers, first_node->piece, start_split_pos);
                // Remove the old one and update.
                root = root.remove(buffers.rb_tree_blk, first.start_offset)
                            .insert(buffers.rb_tree_blk, { new_piece }, first.start_offset);
                return;
            }

            // The removed buffer is somewhere in the middle.  Trim it in both directions.
            auto [left, right] = shrink_piece(&buffers, first_node->piece, start_split_pos, end_split_pos);
            root = root.remove(buffers.rb_tree_blk, first.start_offset)
                        // Note: We insert right first so that the 'left' will be inserted
                        // to the right node's left.
                        .insert(buffers.rb_tree_blk, { right }, first.start_offset)
                        .insert(buffers.rb_tree_blk, { left }, first.start_offset);
            return;
        }

        // Traverse nodes and delete all nodes within the offset range. First we will build the
        // partial pieces for the nodes that will eventually make up this range.
        // There are four cases here:
        // 1. The entire first node is deleted as well as all of the last node.
        // 2. Part of the first node is deleted and all of the last node.
        // 3. Part of the first node is deleted and part of the last node.
        // 4. The entire first node is deleted and part of the last node.

        auto new_first = trim_piece_right(&buffers, first_node->piece, start_split_pos);
        if (last_node == nullptr)
        {
            remove_node_range(first, count);
        }
        else
        {
            auto end_split_pos = buffer_position(&buffers, last_node->piece, last.remainder);
            auto new_last = trim_piece_left(&buffers, last_node->piece, end_split_pos);
            remove_node_range(first, count);
            // There's an edge case here where we delete all the nodes up to 'last' but
            // last itself remains untouched.  The test of 'remainder' in 'last' can identify
            // this scenario to avoid inserting a duplicate of 'last'.
            if (last.remainder != Length{})
            {
                if (new_last.length != Length{})
                {
                    root = root.insert(buffers.rb_tree_blk, { new_last }, first.start_offset);
                }
            }
        }

        if (new_first.length != Length{})
        {
            root = root.insert(buffers.rb_tree_blk, { new_first }, first.start_offset);
        }
    }

    // Fetches the length of the piece starting from the first line to 'index' or to the end of
    // the piece.
    Length Tree::accumulate_value(const BufferCollection* buffers, const Piece& piece, Line index)
    {
        const CharBuffer* buffer = buffers->buffer_at(piece.index);
        const LineStarts* line_starts = &buffer->line_starts;
        // Extend it so we can capture the entire line content including newline.
        auto expected_start = extend(piece.first.line, rep(index) + 1);
        auto first = rep(line_starts->starts[rep(piece.first.line)]) + rep(piece.first.column);
        if (expected_start > piece.last.line)
        {
            auto last = rep(line_starts->starts[rep(piece.last.line)]) + rep(piece.last.column);
            return Length{ last - first };
        }
        auto last = rep(line_starts->starts[rep(expected_start)]);
        return Length{ last - first };
    }

    // Fetches the length of the piece starting from the first line to 'index' or to the end of
    // the piece.
    Length Tree::accumulate_value_no_lf(const BufferCollection* buffers, const Piece& piece, Line index)
    {
        const CharBuffer* buffer = buffers->buffer_at(piece.index);
        const LineStarts* line_starts = &buffer->line_starts;
        // Extend it so we can capture the entire line content including newline.
        auto expected_start = extend(piece.first.line, rep(index) + 1);
        auto first = rep(line_starts->starts[rep(piece.first.line)]) + rep(piece.first.column);
        if (expected_start > piece.last.line)
        {
            auto last = rep(line_starts->starts[rep(piece.last.line)]) + rep(piece.last.column);
            if (last == first)
                return Length{ };
            if (buffer->buffer.str[last - 1] == '\n')
                return Length{ last - 1 - first };
            return Length{ last - first };
        }
        auto last = rep(line_starts->starts[rep(expected_start)]);
        if (last == first)
            return Length{ };
        if (buffer->buffer.str[last - 1] == '\n')
            return Length{ last - 1 - first };
        return Length{ last - first };
    }

    void Tree::populate_from_node(Arena::Arena* arena, String8List* lst, const BufferCollection* buffers, const PieceTree::RedBlackTree& node)
    {
        String8 buffer = buffers->buffer_at(node.root().piece.index)->buffer;
        // We know we want the first line (index 0).
        auto accumulated_value = accumulate_value(buffers, node.root().piece, node.root().piece.first.line);
        auto start_offset = buffers->buffer_offset(node.root().piece.index, node.root().piece.first);
        auto first = buffer.str + rep(start_offset);
        auto last = first + rep(accumulated_value);
        String8 str_to_append = str8(first, last - first);
        str8_serial_push_str8(arena, lst, str_to_append);
    }

    void Tree::populate_from_node(Arena::Arena* arena, String8List* lst, const BufferCollection* buffers, const PieceTree::RedBlackTree& node, Line line_index)
    {
        auto accumulated_value = accumulate_value(buffers, node.root().piece, line_index);
        Length prev_accumulated_value = { };
        if (line_index != Line::IndexBeginning)
        {
            prev_accumulated_value = accumulate_value(buffers, node.root().piece, retract(line_index));
        }
        String8 buffer = buffers->buffer_at(node.root().piece.index)->buffer;
        auto start_offset = buffers->buffer_offset(node.root().piece.index, node.root().piece.first);

        auto first = buffer.str + rep(start_offset) + rep(prev_accumulated_value);
        auto last = buffer.str + rep(start_offset) + rep(accumulated_value);
        String8 str_to_append = str8(first, last - first);
        str8_serial_push_str8(arena, lst, str_to_append);
    }

    template <Tree::Accumulator accumulate>
    void Tree::line_start(CharOffset* offset, const BufferCollection* buffers, const PieceTree::RedBlackTree& node, Line line)
    {
        if (node.is_empty())
            return;
        assert(line != Line::IndexBeginning);
        auto line_index = rep(retract(line));
        if (rep(node.root().left_subtree_lf_count) >= line_index)
        {
            line_start<accumulate>(offset, buffers, node.left(), line);
        }
        // The desired line is directly within the node.
        else if (rep(node.root().left_subtree_lf_count + node.root().piece.newline_count) >= line_index)
        {
            line_index -= rep(node.root().left_subtree_lf_count);
            Length len = node.root().left_subtree_length;
            if (line_index != 0)
            {
                len = len + (*accumulate)(buffers, node.root().piece, Line{ line_index - 1 });
            }
            *offset = *offset + len;
        }
        // assemble the LHS and RHS.
        else
        {
            // This case implies that 'left_subtree_lf_count' is strictly < line_index.
            // The content is somewhere in the middle.
            line_index -= rep(node.root().left_subtree_lf_count + node.root().piece.newline_count);
            *offset = *offset + node.root().left_subtree_length + node.root().piece.length;
            line_start<accumulate>(offset, buffers, node.right(), Line{ line_index + 1 });
        }
    }

    void Tree::line_end_crlf(CharOffset* offset, const BufferCollection* buffers, const RedBlackTree& root, const RedBlackTree& node, Line line)
    {
        if (node.is_empty())
            return;
        assert(line != Line::IndexBeginning);
        auto line_index = rep(retract(line));
        if (rep(node.root().left_subtree_lf_count) >= line_index)
        {
            line_end_crlf(offset, buffers, root, node.left(), line);
        }
        // The desired line is directly within the node.
        else if (rep(node.root().left_subtree_lf_count + node.root().piece.newline_count) >= line_index)
        {
            line_index -= rep(node.root().left_subtree_lf_count);
            Length len = node.root().left_subtree_length;
            if (line_index != 0)
            {
                len = len + accumulate_value_no_lf(buffers, node.root().piece, Line{ line_index - 1 });
            }

            // If the length is anything but 0, we need to check if the last character was a carriage return.
            if (len != Length{})
            {
                auto last_char_offset = *offset + retract(len);
                if (char_at(buffers, root, last_char_offset) == '\r' and char_at(buffers, root, extend(last_char_offset)) == '\n')
                {
                    len = retract(len);
                }
            }
            *offset = *offset + len;
        }
        // assemble the LHS and RHS.
        else
        {
            // This case implies that 'left_subtree_lf_count + piece NL count' is strictly < line_index.
            // The content is somewhere in the middle.
            auto& piece = node.root().piece;
            line_index -= rep(node.root().left_subtree_lf_count + piece.newline_count);
            *offset = *offset + node.root().left_subtree_length + piece.length;
            line_end_crlf(offset, buffers, root, node.right(), Line{ line_index + 1 });
        }
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
        line_end_crlf(&range.last, &buffers, root, root, extend(line));
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

    BufferCollection Tree::buffer_collection_no_ref() const
    {
        return buffers;
    }

    Line Tree::line_at(CharOffset offset) const
    {
        if (is_empty())
            return Line::Beginning;
        auto result = node_at(&buffers, root.dup(), offset);
        return result.line;
    }

    char Tree::at(CharOffset offset) const
    {
        return char_at(&buffers, root, offset);
    }

    char Tree::char_at(const BufferCollection* buffers, const RedBlackTree& node, CharOffset offset)
    {
        auto result = node_at(buffers, node.dup(), offset);
        if (result.node == nullptr)
            return '\0';
        auto* buffer = buffers->buffer_at(result.node->piece.index);
        auto buf_offset = buffers->buffer_offset(result.node->piece.index, result.node->piece.first);
        const char* p = buffer->buffer.str + rep(buf_offset) + rep(result.remainder);
        return *p;
    }

    String8 Tree::assemble_line(Arena::Arena* arena, const BufferCollection* buffers, const BufferMeta& meta, const RedBlackTree& node, Line line)
    {
        String8 result = str8_empty;
        if (node.is_empty())
            return result;
        // Trying this new logic for now.
#if 1
        auto scratch = Arena::scratch_begin({ &arena, 1 });
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
        result = str8_serial_end(arena, serial_lst);
#else
        assert(line != Line::IndexBeginning);
        auto line_index = rep(retract(line));
        if (rep(node.root().left_subtree_lf_count) >= line_index)
        {
            assemble_line(buf, node.left(), line);
            const bool same_index = rep(node.root().left_subtree_lf_count) == line_index;
            if (same_index)
            {
                populate_from_node(buf, node);
                // Visit the RHS if this piece did not introduce a newline.
                if (rep(node.root().piece.newline_count) == 0)
                {
                    assemble_line(buf, node.right(), retract(line, rep(node.root().left_subtree_lf_count)));
                }
            }
        }
        // The desired line is directly within the node.
        else if (rep(node.root().left_subtree_lf_count + node.root().piece.newline_count) > line_index)
        {
            line_index -= rep(node.root().left_subtree_lf_count);
            populate_from_node(buf, node, Line{ line_index });
            return;
        }
        // assemble the LHS and RHS.
        else
        {
            // This case implies that 'left_subtree_lf_count' is strictly < line_index.
            // The content is somewhere in the middle.  We only need to populate from this node if the line index is exactly
            assemble_line(buf, node.left(), line);
            line_index -= rep(node.root().left_subtree_lf_count);
            populate_from_node(buf, node, Line{ line_index });
            if (rep(node.root().piece.newline_count) <= line_index)
            {
                assemble_line(buf, node.right(), retract(line, rep(node.root().left_subtree_lf_count + node.root().piece.newline_count)));
            }
        }
#endif
        Arena::scratch_end(scratch);
        return result;
    }

    String8 Tree::get_line_content(Arena::Arena* arena, Line line) const
    {
        String8 result = str8_empty;
        if (line == Line::IndexBeginning)
            return result;
        result = assemble_line(arena, &buffers, meta, root, line);
        return result;
    }

    String8 OwningSnapshot::get_line_content(Arena::Arena* arena, Line line) const
    {
        String8 result = str8_empty;
        if (line == Line::IndexBeginning)
            return result;
        result = Tree::assemble_line(arena, &buffers, meta, root, line);
        return result;
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

    IncompleteCRLF OwningSnapshot::get_line_content_crlf(Arena::Arena* arena, String8* buf, Line line) const
    {
        // Reset the buffer.
        *buf = str8_empty;
        if (line == Line::IndexBeginning)
            return IncompleteCRLF::No;
        if (root.is_empty())
            return IncompleteCRLF::No;
        // Trying this new logic for now.
        CharOffset line_offset{ };
        Tree::line_start<&Tree::accumulate_value>(&line_offset, &buffers, root, line);
        return trim_crlf(arena, buf, this, line_offset);
    }

    IncompleteCRLF ReferenceSnapshot::get_line_content_crlf(Arena::Arena* arena, String8* buf, Line line) const
    {
        // Reset the buffer.
        *buf = str8_empty;
        if (line == Line::IndexBeginning)
            return IncompleteCRLF::No;
        if (root.is_empty())
            return IncompleteCRLF::No;
        // Trying this new logic for now.
        CharOffset line_offset{ };
        Tree::line_start<&Tree::accumulate_value>(&line_offset, &buffers, root, line);
        return trim_crlf(arena, buf, this, line_offset);
    }

    char OwningSnapshot::at(CharOffset offset) const
    {
        return Tree::char_at(&buffers, root, offset);
    }

    char ReferenceSnapshot::at(CharOffset offset) const
    {
        return Tree::char_at(&buffers, root, offset);
    }

    Line OwningSnapshot::line_at(CharOffset offset) const
    {
        if (is_empty())
            return Line::Beginning;
        auto result = Tree::node_at(&buffers, root.dup(), offset);
        return result.line;
    }

    Line ReferenceSnapshot::line_at(CharOffset offset) const
    {
        if (is_empty())
            return Line::Beginning;
        auto result = Tree::node_at(&buffers, root.dup(), offset);
        return result.line;
    }

    LineRange OwningSnapshot::get_line_range(Line line) const
    {
        LineRange range{ };
        Tree::line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        Tree::line_start<&Tree::accumulate_value_no_lf>(&range.last, &buffers, root, extend(line));
        return range;
    }

    LineRange ReferenceSnapshot::get_line_range(Line line) const
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
        Tree::line_end_crlf(&range.last, &buffers, root, root, extend(line));
        return range;
    }

    LineRange ReferenceSnapshot::get_line_range_crlf(Line line) const
    {
        LineRange range{ };
        Tree::line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        Tree::line_end_crlf(&range.last, &buffers, root, root, extend(line));
        return range;
    }

    LineRange OwningSnapshot::get_line_range_with_newline(Line line) const
    {
        LineRange range{ };
        Tree::line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        Tree::line_start<&Tree::accumulate_value>(&range.last, &buffers, root, extend(line));
        return range;
    }

    LineRange ReferenceSnapshot::get_line_range_with_newline(Line line) const
    {
        LineRange range{ };
        Tree::line_start<&Tree::accumulate_value>(&range.first, &buffers, root, line);
        Tree::line_start<&Tree::accumulate_value>(&range.last, &buffers, root, extend(line));
        return range;
    }

    LFCount Tree::line_feed_count(const BufferCollection* buffers, BufferIndex index, const BufferCursor& start, const BufferCursor& end)
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

    NodePosition Tree::node_at(const BufferCollection* buffers, RedBlackTree node, CharOffset off)
    {
        size_t node_start_offset = 0;
        size_t newline_count = 0;
        while (not node.is_empty())
        {
            if (rep(node.root().left_subtree_length) > rep(off))
            {
                node = node.left();
            }
            else if (rep(node.root().left_subtree_length + node.root().piece.length) > rep(off))
            {
                node_start_offset += rep(node.root().left_subtree_length);
                newline_count += rep(node.root().left_subtree_lf_count);
                // Now we find the line within this piece.
                auto remainder = Length{ rep(retract(off, rep(node.root().left_subtree_length))) };
                auto pos = buffer_position(buffers, node.root().piece, remainder);
                // Note: since buffer_position will return us a newline relative to the buffer itself, we need
                // to retract it by the starting line of the piece to get the real difference.
                newline_count += rep(retract(pos.line, rep(node.root().piece.first.line)));
                return { .node = &node.root(),
                            .remainder = remainder,
                            .start_offset = CharOffset{ node_start_offset },
                            .line = Line{ newline_count + 1 } };
            }
            else
            {
                // If there are no more nodes to traverse to, return this final node.
                if (node.right().is_empty())
                {
                    auto offset_amount = rep(node.root().left_subtree_length);
                    node_start_offset += offset_amount;
                    newline_count += rep(node.root().left_subtree_lf_count + node.root().piece.newline_count);
                    // Now we find the line within this piece.
                    auto remainder = node.root().piece.length;
                    return { .node = &node.root(),
                                .remainder = remainder,
                                .start_offset = CharOffset{ node_start_offset },
                                .line = Line{ newline_count + 1 } };
                }
                auto offset_amount = rep(node.root().left_subtree_length + node.root().piece.length);
                off = retract(off, offset_amount);
                node_start_offset += offset_amount;
                newline_count += rep(node.root().left_subtree_lf_count + node.root().piece.newline_count);
                node = node.right();
            }
        }
        return { };
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

    Piece Tree::trim_piece_right(const BufferCollection* buffers, const Piece& piece, const BufferCursor& pos)
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

    Piece Tree::trim_piece_left(const BufferCollection* buffers, const Piece& piece, const BufferCursor& pos)
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

    Tree::ShrinkResult Tree::shrink_piece(const BufferCollection* buffers, const Piece& piece, const BufferCursor& first, const BufferCursor& last)
    {
        auto left = trim_piece_right(buffers, piece, first);
        auto right = trim_piece_left(buffers, piece, last);

        return { .left = left, .right = right };
    }

    void Tree::combine_pieces(NodePosition existing, Piece new_piece)
    {
        // This transformation is only valid under the following conditions.
        assert(existing.node->piece.index == BufferIndex::ModBuf);
        // This assumes that the piece was just built.
        assert(existing.node->piece.last == new_piece.first);
        auto old_piece = existing.node->piece;
        new_piece.first = old_piece.first;
        new_piece.newline_count = new_piece.newline_count + old_piece.newline_count;
        new_piece.length = new_piece.length + old_piece.length;
        root = root.remove(buffers.rb_tree_blk, existing.start_offset)
                    .insert(buffers.rb_tree_blk, { new_piece }, existing.start_offset);
    }

    void Tree::remove_node_range(NodePosition first, Length length)
    {
        // Remove pieces until we reach the desired length.
        Length deleted_len{};
        // Because we could be deleting content in the range starting at 'first' where the piece
        // length could be much larger than 'length', we need to adjust 'length' to contain the
        // delta in length within the piece to the end where 'length' starts:
        // "abcd"  "efg"
        //     ^     ^
        //     |_____|
        //      length to delete = 3
        // P1 length: 4
        // P2 length: 3 (though this length does not matter)
        // We're going to remove all of 'P1' and 'P2' in this range and the caller will re-insert
        // these pieces with the correct lengths.  If we fail to adjust 'length' we will delete P1
        // and believe that the entire range was deleted.
        assert(first.node != nullptr);
        auto total_length = first.node->piece.length;
        // (total - remainder) is the section of 'length' where 'first' intersects.
        length = length - (total_length - first.remainder) + total_length;
        auto delete_at_offset = first.start_offset;
        while (deleted_len < length and first.node != nullptr)
        {
            deleted_len = deleted_len + first.node->piece.length;
            root = root.remove(buffers.rb_tree_blk, delete_at_offset);
            first = node_at(&buffers, root.dup(), delete_at_offset);
        }
    }

    void Tree::insert(CharOffset offset, String8 txt, SuppressHistory suppress_history)
    {
        if (txt.size == 0)
            return;
        // This allows us to undo blocks of code.
        if (is_no(suppress_history)
            and (end_last_insert != offset or root.is_empty()))
        {
            append_undo(root, offset);
        }
        internal_insert(offset, txt);
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

    void Tree::compute_buffer_meta()
    {
        ::PieceTree::compute_buffer_meta(&meta, root);
    }

    namespace
    {
        void push_ur_node(Arena::Arena* arena, UndoRedoEntry** free_list, UndoRedoList* lst, const RedBlackTree& root, CharOffset op_offset)
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
                entry = new (blob) UndoRedoEntry{ .root = RedBlackTree{} };
            }
            zero_bytes(entry);
            new(&entry->root) RedBlackTree{ root.dup() };
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

    void Tree::append_undo(const RedBlackTree& old_root, CharOffset op_offset)
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

    // Direct history manipulation.
    void Tree::commit_head(CharOffset offset)
    {
        append_undo(root, offset);
    }

    RedBlackTree Tree::head() const
    {
        return root.dup();
    }

    void Tree::snap_to(const RedBlackTree& new_root)
    {
        root = new_root.dup();
        compute_buffer_meta();
    }

#ifdef TEXTBUF_DEBUG
    void print_piece(const Piece& piece, const Tree* tree, int level)
    {
        const char* levels = "|||||||||||||||||||||||||||||||";
        printf("%.*sidx{%zd}, first{l{%zd}, c{%zd}}, last{l{%zd}, c{%zd}}, len{%zd}, lf{%zd}\n",
                level, levels,
                rep(piece.index), rep(piece.first.line), rep(piece.first.column),
                                  rep(piece.last.line), rep(piece.last.column),
                    rep(piece.length), rep(piece.newline_count));
        auto* buffer = tree->buffers.buffer_at(piece.index);
        auto offset = tree->buffers.buffer_offset(piece.index, piece.first);
        printf("%.*sPiece content: %.*s\n", level, levels, static_cast<int>(piece.length), buffer->buffer.str + rep(offset));
    }
#endif // TEXTBUF_DEBUG

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
        RBTreeBlock* rb_tree_blk = Arena::push_array<RBTreeBlock>(builder->immutable_buf_arena, 1);
        rb_tree_blk->free_list.head = nil_node();
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
        root{ tree->root.dup() },
        meta{ tree->meta },
        buffers{ take_buffer_ref(&tree->buffers) }
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

    OwningSnapshot::OwningSnapshot(Arena::Arena* mut_buf_arena, const Tree* tree, const RedBlackTree& dt):
        OwningSnapshot{ mut_buf_arena, tree }
    {
        root = dt.dup();
        // Compute the buffer meta for 'dt'.
        compute_buffer_meta(&meta, dt);
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
        root{ tree->root.dup() },
        meta{ tree->meta },
        buffers{ take_buffer_ref(&tree->buffers) } { }

    ReferenceSnapshot::ReferenceSnapshot(const Tree* tree, const RedBlackTree& dt):
        root{ dt.dup() },
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
        root = RedBlackTree{};
        dec_buffer_ref(&buffers);
    }

    namespace
    {
        bool walker_stack_empty(const StackList& lst)
        {
            return lst.stack == nullptr;
        }

        StackEntry* walker_stack_top(const StackList& lst)
        {
            return lst.stack;
        }

        void walker_stack_push(Arena::Arena* arena, StackList* lst, const RBNodeCounted* node, Direction dir)
        {
            StackEntry* e = lst->free_list;
            if (e != nullptr)
            {
                SLLStackPop(lst->free_list);
            }
            else
            {
                e = Arena::push_array_no_zero<StackEntry>(arena, 1);
            }
            e->next = nullptr;
            e->node = node;
            e->dir = dir;
            SLLStackPush(lst->stack, e);
        }

        void walker_stack_pop(StackList* lst)
        {
            assert(not walker_stack_empty(*lst));
            StackEntry* e = lst->stack;
            SLLStackPop(lst->stack);
            e->next = nullptr;
            SLLStackPush(lst->free_list, e);
        }

        void walker_stack_clear(StackList* lst)
        {
            while (not walker_stack_empty(*lst))
            {
                walker_stack_pop(lst);
            }
        }
    } // namespace [anon]

    TreeWalker::TreeWalker(Arena::Arena* arena, const Tree* tree, CharOffset offset):
        buffers{ &tree->buffers },
        root{ tree->root.dup() },
        meta{ tree->meta },
        arena{ arena },
        total_offset{ offset }
    {
        // Populate initial stack state.
        walker_stack_push(arena, &stack, root.root_ptr(), Direction::Left);
        fast_forward_to(offset);
    }

    TreeWalker::TreeWalker(Arena::Arena* arena, const OwningSnapshot* snap, CharOffset offset):
        buffers{ &snap->buffers },
        root{ snap->root.dup() },
        meta{ snap->meta },
        arena{ arena },
        total_offset{ offset }
    {
        // Populate initial stack state.
        walker_stack_push(arena, &stack, root.root_ptr(), Direction::Left);
        fast_forward_to(offset);
    }

    TreeWalker::TreeWalker(Arena::Arena* arena, const ReferenceSnapshot* snap, CharOffset offset):
        buffers{ &snap->buffers },
        root{ snap->root.dup() },
        meta{ snap->meta },
        arena{ arena },
        total_offset{ offset }
    {
        // Populate initial stack state.
        walker_stack_push(arena, &stack, root.root_ptr(), Direction::Left);
        fast_forward_to(offset);
    }

    TreeWalker::TreeWalker(Arena::Arena* arena, const BufferCollection* buffers, const BufferMeta& meta, const RedBlackTree& root, CharOffset offset):
        buffers{ buffers },
        root{ root.dup() },
        meta{ meta },
        arena{ arena },
        total_offset{ offset }
    {
        // Populate initial stack state.
        walker_stack_push(arena, &stack, root.root_ptr(), Direction::Left);
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
        return *first_ptr++;
    }

    char TreeWalker::current()
    {
        if (first_ptr == last_ptr)
        {
            populate_ptrs();
            // If this is exhausted, we're done.
            if (exhausted())
                return '\0';
        }
        return *first_ptr;
    }

    void TreeWalker::seek(CharOffset offset)
    {
        walker_stack_clear(&stack);
        walker_stack_push(arena, &stack, root.root_ptr(), Direction::Left);
        total_offset = offset;
        fast_forward_to(offset);
    }

    bool TreeWalker::exhausted() const
    {
        if (walker_stack_empty(stack))
            return true;
        // If we have not exhausted the pointers, we're still active.
        if (first_ptr != last_ptr)
            return false;
        // If there's more than one entry on the stack, we're still active.
        if (stack.stack->next != nullptr)
            return false;
        // Now, if there's exactly one entry and that entry itself is exhausted (no right subtree)
        // we're done.
        StackEntry* entry = walker_stack_top(stack);
        // We descended into a null child, we're done.
        if (nil_node(entry->node))
            return true;
        if (entry->dir == Direction::Right and nil_node(entry->node->payload.right))
            return true;
        return false;
    }

    Length TreeWalker::remaining() const
    {
        return meta.total_content_length - distance(CharOffset{}, total_offset);
    }

    void TreeWalker::populate_ptrs()
    {
        if (exhausted())
            return;
        if (nil_node(walker_stack_top(stack)->node))
        {
            walker_stack_pop(&stack);
            populate_ptrs();
            return;
        }

        auto& [nxt, node, dir] = *walker_stack_top(stack);
        if (dir == Direction::Left)
        {
            if (not nil_node(node->payload.left))
            {
                const RBNodeCounted* left = node->payload.left;
                // Change the dir for when we pop back.
                walker_stack_top(stack)->dir = Direction::Center;
                walker_stack_push(arena, &stack, left, Direction::Left);
                populate_ptrs();
                return;
            }
            // Otherwise, let's visit the center, we can actually fallthrough.
            walker_stack_top(stack)->dir = Direction::Center;
            dir = Direction::Center;
        }

        if (dir == Direction::Center)
        {
            auto& piece = node->payload.data.piece;
            auto* buffer = buffers->buffer_at(piece.index);
            auto first_offset = buffers->buffer_offset(piece.index, piece.first);
            auto last_offset = buffers->buffer_offset(piece.index, piece.last);
            first_ptr = buffer->buffer.str + rep(first_offset);
            last_ptr = buffer->buffer.str + rep(last_offset);
            // Change this direction.
            walker_stack_top(stack)->dir = Direction::Right;
            return;
        }

        assert(dir == Direction::Right);
        const RBNodeCounted* right = node->payload.right;
        walker_stack_pop(&stack);
        walker_stack_push(arena, &stack, right, Direction::Left);
        populate_ptrs();
    }

    void TreeWalker::fast_forward_to(CharOffset offset)
    {
        const RBNodeCounted* node = root.root_ptr();
        while (not nil_node(node))
        {
            if (rep(node->payload.data.left_subtree_length) > rep(offset))
            {
                // For when we revisit this node.
                walker_stack_top(stack)->dir = Direction::Center;
                node = node->payload.left;
                walker_stack_push(arena, &stack, node, Direction::Left);
            }
            // It is inside this node.
            else if (rep(node->payload.data.left_subtree_length + node->payload.data.piece.length) > rep(offset))
            {
                walker_stack_top(stack)->dir = Direction::Right;
                // Make the offset relative to this piece.
                offset = retract(offset, rep(node->payload.data.left_subtree_length));
                auto& piece = node->payload.data.piece;
                auto* buffer = buffers->buffer_at(piece.index);
                auto first_offset = buffers->buffer_offset(piece.index, piece.first);
                auto last_offset = buffers->buffer_offset(piece.index, piece.last);
                first_ptr = buffer->buffer.str + rep(first_offset) + rep(offset);
                last_ptr = buffer->buffer.str + rep(last_offset);
                return;
            }
            else
            {
                assert(not walker_stack_empty(stack));
                // This parent is no longer relevant.
                walker_stack_pop(&stack);
                auto offset_amount = rep(node->payload.data.left_subtree_length + node->payload.data.piece.length);
                offset = retract(offset, offset_amount);
                node = node->payload.right;
                walker_stack_push(arena, &stack, node, Direction::Left);
            }
        }
    }

    ReverseTreeWalker::ReverseTreeWalker(Arena::Arena* arena, const Tree* tree, CharOffset offset):
        buffers{ &tree->buffers },
        root{ tree->root.dup() },
        meta{ tree->meta },
        arena{ arena },
        total_offset{ offset }
    {
        // Initial start.
        walker_stack_push(arena, &stack, root.root_ptr(), Direction::Right);
        fast_forward_to(offset);
    }

    ReverseTreeWalker::ReverseTreeWalker(Arena::Arena* arena, const OwningSnapshot* snap, CharOffset offset):
        buffers{ &snap->buffers },
        root{ snap->root.dup() },
        meta{ snap->meta },
        arena{ arena },
        total_offset{ offset }
    {
        // Initial start.
        walker_stack_push(arena, &stack, root.root_ptr(), Direction::Right);
        fast_forward_to(offset);
    }

    ReverseTreeWalker::ReverseTreeWalker(Arena::Arena* arena, const ReferenceSnapshot* snap, CharOffset offset):
        buffers{ &snap->buffers },
        root{ snap->root.dup() },
        meta{ snap->meta },
        arena{ arena },
        total_offset{ offset }
    {
        // Initial start.
        walker_stack_push(arena, &stack, root.root_ptr(), Direction::Right);
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

    char ReverseTreeWalker::current()
    {
        if (first_ptr == last_ptr)
        {
            populate_ptrs();
            // If this is exhausted, we're done.
            if (exhausted())
                return '\0';
        }
        return *(first_ptr - 1);
    }

    void ReverseTreeWalker::seek(CharOffset offset)
    {
        walker_stack_clear(&stack);
        walker_stack_push(arena, &stack, root.root_ptr(), Direction::Right);
        total_offset = offset;
        fast_forward_to(offset);
    }

    bool ReverseTreeWalker::exhausted() const
    {
        if (walker_stack_empty(stack))
            return true;
        // If we have not exhausted the pointers, we're still active.
        if (first_ptr != last_ptr)
            return false;
        // If there's more than one entry on the stack, we're still active.
        if (stack.stack->next != nullptr)
            return false;
        // Now, if there's exactly one entry and that entry itself is exhausted (no right subtree)
        // we're done.
        StackEntry* entry = walker_stack_top(stack);
        // We descended into a null child, we're done.
        if (nil_node(entry->node))
            return true;
        // Do we need this check for reverse iterators?
        if (entry->dir == Direction::Left and nil_node(entry->node->payload.left))
            return true;
        return false;
    }

    Length ReverseTreeWalker::remaining() const
    {
        return distance(CharOffset{}, extend(total_offset));
    }

    void ReverseTreeWalker::populate_ptrs()
    {
        if (exhausted())
            return;
        if (nil_node(walker_stack_top(stack)->node))
        {
            walker_stack_pop(&stack);
            populate_ptrs();
            return;
        }

        auto& [nxt, node, dir] = *walker_stack_top(stack);
        if (dir == Direction::Right)
        {
            if (not nil_node(node->payload.right))
            {
                const RBNodeCounted* right = node->payload.right;
                // Change the dir for when we pop back.
                walker_stack_top(stack)->dir = Direction::Center;
                walker_stack_push(arena, &stack, right, Direction::Right);
                populate_ptrs();
                return;
            }
            // Otherwise, let's visit the center, we can actually fallthrough.
            walker_stack_top(stack)->dir = Direction::Center;
            dir = Direction::Center;
        }

        if (dir == Direction::Center)
        {
            auto& piece = node->payload.data.piece;
            auto* buffer = buffers->buffer_at(piece.index);
            auto first_offset = buffers->buffer_offset(piece.index, piece.first);
            auto last_offset = buffers->buffer_offset(piece.index, piece.last);
            last_ptr = buffer->buffer.str + rep(first_offset);
            first_ptr = buffer->buffer.str + rep(last_offset);
            // Change this direction.
            walker_stack_top(stack)->dir = Direction::Left;
            return;
        }

        assert(dir == Direction::Left);
        const RBNodeCounted* left = node->payload.left;
        walker_stack_pop(&stack);
        walker_stack_push(arena, &stack, left, Direction::Right);
        populate_ptrs();
    }

    void ReverseTreeWalker::fast_forward_to(CharOffset offset)
    {
        const RBNodeCounted* node = root.root_ptr();
        while (not nil_node(node))
        {
            if (rep(node->payload.data.left_subtree_length) > rep(offset))
            {
                assert(not walker_stack_empty(stack));
                // This parent is no longer relevant.
                walker_stack_pop(&stack);
                node = node->payload.left;
                walker_stack_push(arena, &stack, node, Direction::Right);
            }
            // It is inside this node.
            else if (rep(node->payload.data.left_subtree_length + node->payload.data.piece.length) > rep(offset))
            {
                walker_stack_top(stack)->dir = Direction::Left;
                // Make the offset relative to this piece.
                offset = retract(offset, rep(node->payload.data.left_subtree_length));
                auto& piece = node->payload.data.piece;
                auto* buffer = buffers->buffer_at(piece.index);
                auto first_offset = buffers->buffer_offset(piece.index, piece.first);
                last_ptr = buffer->buffer.str + rep(first_offset);
                // We extend offset because it is the point where we want to start and because this walker works by dereferencing
                // 'first_ptr - 1', offset + 1 is our 'begin'.
                first_ptr = buffer->buffer.str + rep(first_offset) + rep(extend(offset));
                return;
            }
            else
            {
                // For when we revisit this node.
                walker_stack_top(stack)->dir = Direction::Center;
                auto offset_amount = rep(node->payload.data.left_subtree_length + node->payload.data.piece.length);
                offset = retract(offset, offset_amount);
                node = node->payload.right;
                walker_stack_push(arena, &stack, node, Direction::Right);
            }
        }
    }

    SelectionNode* push_selection(Arena::Arena* arena, SelectionList* lst, Selection sel)
    {
        SelectionNode* node = Arena::push_array_no_zero<SelectionNode>(arena, 1);
        node->selection = sel;
        SLLQueuePush(lst->first, lst->last, node);
        ++lst->count;
        return node;
    }

    void pop_selection(SelectionList* lst)
    {
        assert(lst->count != 0);
        SLLQueuePop(lst->first, lst->last);
        --lst->count;
    }
} // namespace PieceTree

// Debugging stuff
#ifdef TEXTBUF_DEBUG
void print_tree(const PieceTree::RedBlackTree& root, const PieceTree::Tree* tree, int level = 0, size_t node_offset = 0)
{
    if (root.is_empty())
        return;
    const char* levels = "|||||||||||||||||||||||||||||||";
    auto this_offset = node_offset + rep(root.root().left_subtree_length);
    printf("%.*sme: %p, left: %p, right: %p, color: %s\n", level, levels, root.root_ptr(), root.left().root_ptr(), root.right().root_ptr(), to_string(root.root_color()));
    print_piece(root.root().piece, tree, level);
    printf("%.*sleft_len{%zd}, left_lf{%zd}, node_offset{%zd}\n", level, levels, rep(root.root().left_subtree_length), rep(root.root().left_subtree_lf_count), this_offset);
    printf("\n");
    print_tree(root.left(), tree, level + 1, node_offset);
    printf("\n");
    print_tree(root.right(), tree, level + 1, this_offset + rep(root.root().piece.length));
}

namespace PieceTree
{
    void print_tree(const PieceTree::Tree& tree)
    {
        ::print_tree(tree.root, &tree);
    }
}

void print_buffer(const PieceTree::Tree* tree)
{
    printf("--- Entire Buffer ---\n");
    auto scratch = Arena::scratch_begin(Arena::no_conflicts);

    PieceTree::Length len = tree->length();
    for EachIndex(i, rep(len))
    {
        printf("|%2zu", i);
    }
    printf("\n");
    PieceTree::TreeWalker walker{ scratch.arena, tree };
    while (not walker.exhausted())
    {
        char c = walker.next();
        if (c == '\n')
            printf("|\\n");
        else
            printf("| %c", c);
    }
    printf("\n");
    Arena::scratch_end(scratch);
}

void flush()
{
    fflush(stdout);
}
#endif // TEXTBUF_DEBUG