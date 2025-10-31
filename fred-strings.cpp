#include "fred-strings.h"

// Node construction.
String8Node* str8_list_push_node(String8List* lst, String8Node* node)
{
    SLLQueuePush(lst->first, lst->last, node);
    ++lst->node_count;
    lst->total_size += node->string.size;
    return node;
}

String8Node* str8_list_push_node_set_string(String8List* lst, String8Node* node, String8 string)
{
    SLLQueuePush(lst->first, lst->last, node);
    ++lst->node_count;
    lst->total_size += string.size;
    node->string = string;
    return node;
}

// List construction.
String8Node* str8_list_push(Arena::Arena* arena, String8List* lst, String8 string)
{
    String8Node* node = Arena::push_array_no_zero<String8Node>(arena, 1);
    return str8_list_push_node_set_string(lst, node, string);
}

// Serializing data.
void str8_serial_begin(Arena::Arena* arena, String8List* lst)
{
    String8Node* node = str8_list_push(arena, lst, str8_empty);
    // Begin the string allocation site.
    node->string.str = Arena::push_array_no_zero<char>(arena, 0);
}

String8 str8_serial_end(Arena::Arena* arena, const String8List& lst)
{
    String8 result = str8_cstr_alloc(arena, lst.total_size);
    char* out = result.str;
    for EachNode(n, lst.first)
    {
        memcpy(out, n->string.str, n->string.size);
        out += n->string.size;
    }
    return result;
}

void str8_serial_push_char(Arena::Arena* arena, String8List* lst, char c)
{
    str8_serial_push_str8(arena, lst, str8(&c, 1));
}

void str8_serial_push_str8(Arena::Arena* arena, String8List* lst, String8 str)
{
    if (str.size == 0)
        return;
    // Try to append allocations.
    auto arena_pos = Arena::pos(arena);
    char* buf = Arena::push_array_no_zero_aligned<char>(arena, str.size, Arena::Alignment{ alignof(char) });
    String8* latest = &lst->last->string;
    if (latest->str == nullptr && latest->size == 0)
    {
        latest->str = buf;
    }
    if (latest->str + latest->size == buf)
    {
        latest->size += str.size;
        lst->total_size += str.size;
    }
    // Append a new node.
    else
    {
        // Note: in order for this to remain efficient, we will actually discard the memory allocated above, allocate a new node and _then_
        // allocate a new string buffer.  This is to ensure that we can grow the buffer of the new string in the new chunk.
        Arena::pop_to(arena, arena_pos);
        str8_list_push(arena, lst, str);
        // Now we allocate a buffer for the string, and assign it.
        buf = Arena::push_array_no_zero<char>(arena, str.size);
        lst->last->string.str = buf;
    }
    memcpy(buf, str.str, str.size);
}

// Basic string construction.
String8 str8_cstr(char* str)
{
    String8 s{};
    s.str = str;
    s.size = str != nullptr ? strlen(str) : 0;
    return s;
}

String8 str8_cppview(std::string_view str)
{
    String8 s{};
    // Bad, but a workaround until I get rid of string_view.
    s.str = const_cast<char*>(str.data());
    s.size = str.size();
    return s;
}

String8 str8_mut(String8View str)
{
    String8 s{};
    s.str = const_cast<char*>(str.str);
    s.size = str.size;
    return s;
}

String8 str8_alloc(Arena::Arena* arena, uint64_t size)
{
    char* str = Arena::push_array_no_zero<char>(arena, size);
    return str8(str, size);
}

String8 str8_cstr_alloc(Arena::Arena* arena, uint64_t size)
{
    char* str = Arena::push_array_no_zero<char>(arena, size + 1);
    str[size] = 0;
    return str8(str, size);
}

String8 str8_copy(Arena::Arena* arena, String8 string)
{
    // +1 for null byte.
    String8 cpy{};
    cpy.size = string.size;
    cpy.str = Arena::push_array_no_zero<char>(arena, string.size + 1);
    memcpy(cpy.str, string.str, string.size);
    cpy.str[cpy.size] = 0;
    return cpy;
}

// String searching.
bool str8_match_exact(String8 a, String8 b)
{
    if (a.size != b.size)
        return false;
    return memcmp(a.str, b.str, a.size) == 0;
}