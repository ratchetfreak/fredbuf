#pragma once

#include <stdarg.h>

#include <string_view>

#include "arena.h"

// Basic strings.
struct String8
{
    char* str;
    uint64_t size;
};

struct String16
{
    wchar_t* str;
    uint64_t size;
};

struct String8View
{
    const char* str;
    uint64_t size;
};

struct String16View
{
    const wchar_t* str;
    uint64_t size;
};

struct String8Node
{
    String8Node* next;
    String8 string;
};

struct String8List
{
    String8Node* first;
    String8Node* last;
    uint64_t node_count;
    uint64_t total_size;
};

// C++ nonsense.
struct String8ListItr
{
    String8Node* cur;

    constexpr String8 operator*() { return cur->string; }
    constexpr void operator++() { cur = cur->next; }
    friend bool operator==(String8ListItr, String8ListItr) = default;
};

constexpr String8ListItr begin(const String8List& lst)
{
    return { .cur = lst.first };
}

constexpr String8ListItr end(const String8List&)
{
    return {};
}

constexpr std::string_view sv_str8(String8 str)
{
    return { str.str, str.size };
}

// Node construction.
String8Node* str8_list_push_node(String8List* lst, String8Node* node);
String8Node* str8_list_push_node_set_string(String8List* lst, String8Node* node, String8 string);

// List construction.
String8Node* str8_list_push(Arena::Arena* arena, String8List* lst, String8 string);

// Serializing data.
void str8_serial_begin(Arena::Arena* arena, String8List* lst);
String8 str8_serial_end(Arena::Arena* arena, const String8List& lst);
void str8_serial_push_char(Arena::Arena* arena, String8List* lst, char c);
void str8_serial_push_str8(Arena::Arena* arena, String8List* lst, String8 str);

// List joining.
String8 str8_list_join(Arena::Arena* arena, const String8List& lst);

// Basic string construction.
inline constexpr String8 str8_empty{};

String8 str8_cstr(char* str);
String8 str8_cppview(std::string_view str);
String8 str8_mut(String8View str);

constexpr String8 str8(char* str, uint64_t size)
{
#if OS_LINUX
    return { .size = size, .str = str };
#else
    return { .str = str, .size = size };
#endif
}

template <int N>
constexpr String8 str8(char (&arr)[N])
{
    return str8(arr, N);
}

template <int N>
constexpr String8View str8_literal(const char (&arr)[N])
{
    return { .str = arr, .size = N - 1 };
}

String8 str8_alloc(Arena::Arena* arena, uint64_t size);
String8 str8_cstr_alloc(Arena::Arena* arena, uint64_t size);
String8 str8_copy(Arena::Arena* arena, String8 string);
String8 str8_cat(Arena::Arena* arena, String8 a, String8 b);
String8 str8_fmt(Arena::Arena* arena, const char* fmt, ...);

constexpr String16 str16(wchar_t* str, uint64_t size)
{
    return { .str = str, .size = size };
}

template <int N>
constexpr String16View str16_literal(const wchar_t (&arr)[N])
{
    return { .str = arr, .size = N - 1 };
}

String16 str16_cstr(wchar_t* str);
String16 str16_mut(String16View str);

String16 str16_cstr_alloc(Arena::Arena* arena, uint64_t size);

// String slicing.
struct String8Slice
{
    uint64_t off = 0;
    uint64_t len = uint64_t(-1);
};

String8 str8_substr(String8 str, String8Slice slice);

// String searching.
inline constexpr uint64_t str8_index_sentinel = uint64_t(-1);
bool str8_match_exact(String8 a, String8 b);
uint64_t str8_find_last_of(String8 str, String8 pattern);