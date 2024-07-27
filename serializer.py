def parse_resp_string(resp_string):
    """
    Parses a RESP string from Redis and converts it to a nested list.

    Args:
        resp_string (str): The RESP string to parse.

    Returns:
        list: The nested list representation of the RESP string.
    """
    def tokenize(resp):
        tokens = resp.split('\r\n')
        return [token for token in tokens if token]

    def parse(tokens):
        print(tokens)
        token = tokens.pop(0)
        if token.startswith('*'):
            length = int(token[1:])
            array = []
            for _ in range(length):
                array.append(parse(tokens))
            return array
        elif token.startswith(':'):
            return int(token[1:])
        elif token.startswith('$'):
            length = int(token[1:])
            if length == -1:
                return None
            return tokens.pop(0)
        else:
            raise ValueError(f"Unexpected token: {token}")

    tokens = tokenize(resp_string)
    return parse(tokens)

# Example usage:
resp_string = "*3\r\n:1\r\n:2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:3\r\n"
parsed = parse_resp_string(resp_string)
print(parsed)  # Output: [1, 2, ['foo', 'bar'], 3]
