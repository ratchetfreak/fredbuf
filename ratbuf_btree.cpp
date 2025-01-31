#include "ratbuf_btree.h"


#include <cassert>

#include <memory>
#include <string_view>
#include <string>
#include <vector>
#include <array>

#include "types.h"
#include "enum-utils.h"
#include "scope-guard.h"

namespace PieceTree
{
    template<size_t MaxChildren>
    B_Tree<MaxChildren>::B_Tree(NodePtr root):root_node(root){
        
    }
    template<size_t MaxChildren>
    B_Tree<MaxChildren> B_Tree<MaxChildren>::insert(const NodeData& x, Offset at) const{
        const Node* root = root_node.get();
        auto result = insertInto(root, x, Editor::distance(Offset(0), at));
        size_t newNumChilder = result.offsets.size();
        if(result.offsets.size() > MaxChildren)
        {
            size_t numLChild = result.offsets.size()/2;
            size_t numRChild = newNumChilder-numLChild;
            Length mid = result.offsets[numLChild];
            
            
            std::array<NodePtr, MaxChildren> left_children;
            std::array<Length, MaxChildren> left_offsets;
            // std::vector<NodePtr> children = 
           
            for(int i = 0; i < numLChild; i++)
            {
                left_children[i] = std::move(std::get<std::vector<NodePtr> >(result.children)[i]);
                left_offsets[i] = result.offsets[i];
            }
            NodePtr left = std::make_shared<Node>(left_children, left_offsets, numLChild);
            
            
            std::array<NodePtr, MaxChildren> right_children;
            std::array<Length, MaxChildren> right_offsets;
            for(int i = 0; i <  numRChild; i++)
            {
                left_children[i] = std::get<std::vector<NodePtr> >(result.children)[i+numLChild];
                left_offsets[i] = result.offsets[i+numLChild]-mid;
            }
            left_children[result.offsets.size()] = std::get<std::vector<NodePtr> >(result.children)[result.offsets.size()];
            
            NodePtr right = std::make_shared<Node>(right_children, right_offsets, numRChild);
            
            //Length mid = result.offsets[result.offsets.size()/2];
            
            std::array<NodePtr, MaxChildren> new_root_children;
            std::array<Length, MaxChildren> new_root_offsets;
            new_root_children[0] = left;
            new_root_children[1] = left;
            new_root_offsets[0] = mid;
            NodePtr new_root = std::make_shared<Node>(new_root_children, new_root_offsets, 2);
            
            return B_Tree<MaxChildren>(new_root);
            
        }
        else 
        {
            std::array<NodePtr, MaxChildren> new_root_children;
            std::array<Length, MaxChildren> new_root_offsets;
            for(int i = 0; i < newNumChilder; i++)
            {
                new_root_children[i] = std::move(std::get<std::vector<NodePtr> >(result.children)[i]);
                if(i+1 < newNumChilder)
                    new_root_offsets[i] = result.offsets[i];
            }
            return B_Tree<MaxChildren>(std::make_shared<Node>(new_root_children, new_root_offsets, std::get<0>(result.children).size()));
        }
        
    }
    
    template<size_t MaxChildren>
    B_Tree<MaxChildren>::TreeManipResult B_Tree<MaxChildren>::insertInto_leaf(const Node* node, const NodeData& x, Length at) const
    {
        auto offsets_end = node->offsets.begin()+node->childCount;
        auto insertPoint = std::lower_bound(node->offsets.begin(), offsets_end, at);
        
        std::vector<NodeData> resultch;
        std::vector<Length> resultoff;
        auto child_it = std::get<std::array<NodeData, MaxChildren>>(node->children).begin();
        auto off_it = node->offsets.begin();
        Length acc { 0};
        for(; off_it!=insertPoint; ++off_it, ++child_it)
        {
            resultch.push_back(*child_it);
            acc = acc + (*child_it).piece.length;
            resultoff.push_back(acc);
        }
        Length split_offset = at - acc;
        NodeData second{};
        if(split_offset > Length{0}){
            
            //split insert point
            NodeData first{};
            resultch.push_back(first);
            acc = acc + split_offset;
            resultoff.push_back(acc);
        }
        resultch.push_back(x);
        acc = acc + x.piece.length;
        resultoff.push_back(acc);
        if(split_offset > Length{0}){
            //split insert point
            resultch.push_back(second);
            acc = acc + (*child_it).piece.length-split_offset;
            resultoff.push_back(acc);
            ++off_it;++child_it;
        }
        
        for(; off_it!=offsets_end; ++off_it, ++child_it)
        {
            resultch.push_back(*child_it);
            acc = acc + (*child_it).piece.length;
            resultoff.push_back(acc);
        }
        B_Tree<MaxChildren>::TreeManipResult res;
        res.children = std::move(resultch);
        res.offsets = std::move(resultoff);
        res.preserved_invariant = true;
        return res;
    }
    
    template<size_t MaxChildren>
    B_Tree<MaxChildren>::TreeManipResult B_Tree<MaxChildren>::insertInto(const Node* node, const NodeData& x, Length at) const
    {
        
        if(node->isLeaf())
        {
            return insertInto_leaf(node, x, at);
        }
        else
        {
            auto offsets_end = node->offsets.begin()+node->childCount;
            auto insertPoint = std::lower_bound(node->offsets.begin(), offsets_end, at);
        
            auto off_it = node->offsets.begin();
            auto insert_child = std::get<std::array<NodePtr, MaxChildren>>(node->children)[insertPoint-off_it];
            auto insert_result = insertInto(insert_child.get(), x, at-*off_it);
            size_t newNumChilder = insert_result.offsets.size();
           
             
            {
                
                std::vector<NodePtr> resultch;
                std::vector<Length> resultoff;
                auto child_it = std::get<std::array<NodePtr, MaxChildren>>(node->children).begin();
                 off_it = node->offsets.begin();
                Length acc { 0};
                for(; off_it!=insertPoint; ++off_it, ++child_it)
                {
                    resultch.push_back(*child_it);
                    acc = acc + (*child_it)->subTreeLength();
                    resultoff.push_back(acc);
                }
                
                if(insert_result.offsets.size() > MaxChildren)
                {
                    size_t numLChild = insert_result.offsets.size()/2;
                    //size_t numRChild = insert_result.offsets.size() - numLChild;
                    std::array<NodePtr, MaxChildren> new_left_children;
                    std::array<Length, MaxChildren>  new_left_offsets;
                    for(int i = 0; i < numLChild; i++)
                    {
                        new_left_children[i] = std::get<std::vector<NodePtr>>(insert_result.children)[i];
                        new_left_offsets[i] = insert_result.offsets[i];
                    }
                    NodePtr leftChild;
                    NodePtr rightChild;
                    if(std::holds_alternative<std::vector<NodeData>>(insert_result.children))
                    {
                        rightChild = construct_leaf( std::get<std::vector<NodeData>>(insert_result.children), insert_result.offsets, numLChild, insert_result.offsets.size());
                        leftChild = construct_leaf( std::get<std::vector<NodeData>>(insert_result.children), insert_result.offsets, 0, numLChild);
                    }
                    else
                    {
                        leftChild = construct_internal( std::get<std::vector<NodePtr>>(insert_result.children), insert_result.offsets, 0, numLChild);rightChild = construct_internal( std::get<std::vector<NodePtr>>(insert_result.children), insert_result.offsets, numLChild, insert_result.offsets.size());
                    }
                    
                    resultch.push_back(std::move(leftChild));
                    acc = acc + leftChild->subTreeLength();
                    resultoff.push_back(acc);
                    
                    resultch.push_back(std::move(rightChild));
                    acc = acc + rightChild->subTreeLength();
                    resultoff.push_back(acc);
                }
                else
                {
                    std::array<NodePtr, MaxChildren> new_root_children;
                    std::array<Length, MaxChildren> new_root_offsets;
                    for(int i = 0; i < newNumChilder; i++)
                    {
                        new_root_children[i] = std::move(std::get<std::vector<NodePtr> >(insert_result.children)[i]);
                        if(i+1 < newNumChilder)
                            new_root_offsets[i] = insert_result.offsets[i];
                    }
                    auto newChild = std::make_shared<Node>(new_root_children, new_root_offsets, std::get<0>(insert_result.children).size());
                    resultch.push_back(newChild);
                    resultoff.push_back(acc);
                    acc = acc + new_root_offsets.back();
                    ++off_it;++child_it;
                }
                
                for(; off_it!=offsets_end; ++off_it, ++child_it)
                {
                    resultch.push_back(*child_it);
                    acc = acc + (*child_it)->subTreeLength();
                    resultoff.push_back(acc);
                }
                B_Tree<MaxChildren>::TreeManipResult res;
                res.children = std::move(resultch);
                res.offsets = std::move(resultoff);
                res.preserved_invariant = true;
                return res;
            }
        }
        
        
        // return {};
    }
    
    template<size_t MaxChildren>
    B_Tree<MaxChildren>::NodePtr B_Tree<MaxChildren>::construct_leaf(std::vector<NodeData> data, std::vector<Length> offsets,size_t begin, size_t end) const
    {
        size_t numChild = end-begin;
        std::array<NodeData, MaxChildren> new_left_children;
        std::array<Length, MaxChildren>  new_left_offsets;
        for(int i = 0; i < numChild; i++)
        {
            new_left_children[i] =data[begin+i];
            new_left_offsets[i] = offsets[begin+i] - offsets[begin];
        }
        
        return std::make_shared<Node>(new_left_children, new_left_offsets,  end-begin);
    }
    
    
    template<size_t MaxChildren>
    B_Tree<MaxChildren>::NodePtr B_Tree<MaxChildren>::construct_internal(std::vector<NodePtr> data, std::vector<Length> offsets,size_t begin, size_t end) const
    {
        size_t numChild = end-begin;
        std::array<NodePtr, MaxChildren> new_left_children;
        std::array<Length, MaxChildren>  new_left_offsets;
        for(int i = 0; i < numChild; i++)
        {
            new_left_children[i] =data[begin+i];
            new_left_offsets[i] = offsets[begin+i] - offsets[begin];
        }
        
        return std::make_shared<Node>(new_left_children, new_left_offsets,  end-begin);
    }
    
    void foo(){
        B_Tree<10> tree{};
        Offset at={
            
        };
        NodeData data={
            .piece = {
                .length { 5}
            }
        };
        tree.insert(data, at);
    }
}