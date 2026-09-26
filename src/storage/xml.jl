# A small, strict XML reader/writer for the Storage REST API.
#
# Storage responses use a tiny subset of XML: elements, attributes, text,
# the predefined entities and character references. DOCTYPE declarations
# (and therefore custom entities, the root of XXE and "billion laughs"
# attacks) are rejected, nesting depth is bounded, and the input must be
# valid UTF-8.

"Parsed XML element: tag, attributes and children (elements and text)."
struct XElement
    tag::String
    attributes::Vector{Pair{String,String}}
    children::Vector{Union{XElement,String}}
end

struct XMLParseError <: Exception
    msg::String
end
Base.showerror(io::IO, e::XMLParseError) = print(io, "XMLParseError: ", e.msg)

const MAX_XML_DEPTH = 128

@inline _is_xml_space(c::UInt8) = c == 0x20 || c == 0x09 || c == 0x0a || c == 0x0d
@inline _is_name_end(c::UInt8) = _is_xml_space(c) || c == UInt8('>') || c == UInt8('/') || c == UInt8('=')

function _xml_unescape_entities(s::AbstractString)
    occursin('&', s) || return String(s)
    io = IOBuffer()
    i = firstindex(s)
    while i <= ncodeunits(s)
        c = s[i]
        if c == '&'
            j = findnext(';', s, i)
            (j === nothing || j - i > 12) && throw(XMLParseError("unterminated entity reference"))
            ent = SubString(s, nextind(s, i), prevind(s, j))
            if ent == "lt"
                write(io, '<')
            elseif ent == "gt"
                write(io, '>')
            elseif ent == "amp"
                write(io, '&')
            elseif ent == "quot"
                write(io, '"')
            elseif ent == "apos"
                write(io, '\'')
            elseif startswith(ent, "#x") || startswith(ent, "#X")
                v = tryparse(UInt32, ent[3:end]; base=16)
                (v === nothing || !isvalid(Char, v)) && throw(XMLParseError("invalid character reference"))
                write(io, Char(v))
            elseif startswith(ent, "#")
                v = tryparse(UInt32, ent[2:end]; base=10)
                (v === nothing || !isvalid(Char, v)) && throw(XMLParseError("invalid character reference"))
                write(io, Char(v))
            else
                throw(XMLParseError("unknown entity &$(ent);"))
            end
            i = j + 1
        else
            write(io, c)
            i = nextind(s, i)
        end
    end
    return String(take!(io))
end

# s[from:to-1] for byte offsets where `from` and `to` sit on ASCII delimiters.
_sub(s::String, from::Int, to::Int) = to <= from ? "" : String(SubString(s, from, prevind(s, to)))

function _xml_find(s::String, pat::String, from::Int, what::String)
    r = findnext(pat, s, from)
    r === nothing && throw(XMLParseError("unterminated $(what)"))
    return first(r)
end

"""
    xml_parse(data) -> XElement

Parse an XML document (string or bytes) and return its root element.
"""
function xml_parse(data::Union{AbstractString,AbstractVector{UInt8}})
    s = data isa AbstractString ? String(data) : String(copy(data))
    isvalid(s) || throw(XMLParseError("document is not valid UTF-8"))
    n = ncodeunits(s)
    i = startswith(s, "\ufeff") ? 4 : 1
    stack = XElement[]
    root = nothing
    while i <= n
        c = codeunit(s, i)
        if c == UInt8('<')
            i == n && throw(XMLParseError("unexpected end of document"))
            c2 = codeunit(s, i + 1)
            if c2 == UInt8('?')
                i = _xml_find(s, "?>", i + 2, "processing instruction") + 2
            elseif c2 == UInt8('!')
                if startswith(SubString(s, i), "<!--")
                    i = _xml_find(s, "-->", i + 4, "comment") + 3
                elseif startswith(SubString(s, i), "<![CDATA[")
                    j = _xml_find(s, "]]>", i + 9, "CDATA section")
                    isempty(stack) && throw(XMLParseError("CDATA outside the root element"))
                    push!(stack[end].children, _sub(s, i + 9, j))
                    i = j + 3
                else
                    throw(XMLParseError("DOCTYPE and other declarations are not supported"))
                end
            elseif c2 == UInt8('/')
                j = _xml_find(s, ">", i + 2, "end tag")
                name = strip(_sub(s, i + 2, j))
                (isempty(stack) || stack[end].tag != name) && throw(XMLParseError("mismatched end tag </$(name)>"))
                el = pop!(stack)
                isempty(stack) && (root = el)
                i = j + 1
            else
                root === nothing || throw(XMLParseError("content after the root element"))
                j = i + 1
                while j <= n && !_is_name_end(codeunit(s, j))
                    j += 1
                end
                name = _sub(s, i + 1, j)
                isempty(name) && throw(XMLParseError("empty element name"))
                attrs = Pair{String,String}[]
                selfclose = false
                while true
                    while j <= n && _is_xml_space(codeunit(s, j))
                        j += 1
                    end
                    j > n && throw(XMLParseError("unterminated start tag <$(name)>"))
                    cj = codeunit(s, j)
                    if cj == UInt8('>')
                        j += 1
                        break
                    elseif cj == UInt8('/')
                        (j < n && codeunit(s, j + 1) == UInt8('>')) || throw(XMLParseError("malformed start tag <$(name)>"))
                        selfclose = true
                        j += 2
                        break
                    end
                    k = j
                    while k <= n && !_is_name_end(codeunit(s, k))
                        k += 1
                    end
                    aname = _sub(s, j, k)
                    while k <= n && _is_xml_space(codeunit(s, k))
                        k += 1
                    end
                    (k <= n && codeunit(s, k) == UInt8('=')) || throw(XMLParseError("malformed attribute in <$(name)>"))
                    k += 1
                    while k <= n && _is_xml_space(codeunit(s, k))
                        k += 1
                    end
                    k > n && throw(XMLParseError("unterminated attribute"))
                    q = codeunit(s, k)
                    (q == UInt8('"') || q == UInt8('\'')) || throw(XMLParseError("unquoted attribute value"))
                    e = findnext(==(Char(q)), s, k + 1)
                    e === nothing && throw(XMLParseError("unterminated attribute value"))
                    push!(attrs, aname => _xml_unescape_entities(_sub(s, k + 1, e)))
                    j = e + 1
                end
                el = XElement(name, attrs, Union{XElement,String}[])
                if selfclose
                    if isempty(stack)
                        root = el
                    else
                        push!(stack[end].children, el)
                    end
                else
                    length(stack) >= MAX_XML_DEPTH && throw(XMLParseError("document nested too deeply"))
                    isempty(stack) || push!(stack[end].children, el)
                    push!(stack, el)
                end
                i = j
            end
        else
            j = findnext('<', s, i)
            j === nothing && (j = n + 1)
            text = SubString(s, i, prevind(s, j))
            if isempty(stack)
                all(ch -> ch in (' ', '\t', '\n', '\r'), text) || throw(XMLParseError("text outside the root element"))
            else
                push!(stack[end].children, _xml_unescape_entities(text))
            end
            i = j
        end
    end
    isempty(stack) || throw(XMLParseError("unclosed element <$(stack[end].tag)>"))
    root === nothing && throw(XMLParseError("document has no root element"))
    return root
end

"First child element named `tag`, or `nothing`."
function xfind(e::XElement, tag::AbstractString)
    for c in e.children
        c isa XElement && c.tag == tag && return c
    end
    return nothing
end
xfind(::Nothing, ::AbstractString) = nothing

"All child elements named `tag` (or all child elements when `tag === nothing`)."
xfindall(e::XElement, tag::Union{Nothing,AbstractString}=nothing) =
    XElement[c for c in e.children if c isa XElement && (tag === nothing || c.tag == tag)]
xfindall(::Nothing, tag=nothing) = XElement[]

"Concatenated text content of an element."
xtext(e::XElement) = join(c for c in e.children if c isa String)
xtext(::Nothing) = nothing

"Text of the child element `tag`, or `default` when absent."
function xchildtext(e, tag::AbstractString, default=nothing)
    c = xfind(e, tag)
    return c === nothing ? default : xtext(c)
end

"Attribute value, or `default`."
function xattr(e::XElement, name::AbstractString, default=nothing)
    for (k, v) in e.attributes
        k == name && return v
    end
    return default
end

"Escape text for use in XML content or attribute values."
function xml_escape(s::AbstractString)
    io = IOBuffer()
    for c in s
        if c == '&'
            write(io, "&amp;")
        elseif c == '<'
            write(io, "&lt;")
        elseif c == '>'
            write(io, "&gt;")
        elseif c == '"'
            write(io, "&quot;")
        elseif c == '\''
            write(io, "&apos;")
        else
            write(io, c)
        end
    end
    return String(take!(io))
end

"""
    xml_element(tag, content) -> String

Serialise `content` under `tag`: strings are escaped, `nothing` is skipped
(returns `""`), vectors/tuples are concatenated, pairs become child elements
and a `RawXML` value is inserted verbatim.
"""
struct RawXML
    s::String
end
xml_element(tag::AbstractString, ::Nothing) = ""
xml_element(tag::AbstractString, v) = string("<", tag, ">", _xml_content(v), "</", tag, ">")
_xml_content(v::RawXML) = v.s
_xml_content(v::AbstractString) = xml_escape(v)
_xml_content(v::Bool) = v ? "true" : "false"
_xml_content(v::Number) = string(v)
_xml_content(v::Pair) = xml_element(string(v.first), v.second)
_xml_content(v::Union{AbstractVector,Tuple}) = join(_xml_content(x) for x in v)
_xml_content(::Nothing) = ""

xml_document(tag::AbstractString, content) = "<?xml version=\"1.0\" encoding=\"utf-8\"?>" * xml_element(tag, content)
