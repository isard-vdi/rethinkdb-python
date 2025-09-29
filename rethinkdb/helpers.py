def decode_utf8(string, encoding="utf-8"):
    if hasattr(string, "decode"):
        return string.decode(encoding)

    return string


def chain_to_bytes(*strings):
    return b"".join(
        [
            string.encode('utf-8') if isinstance(string, str) else string
            for string in strings
        ]
    )
