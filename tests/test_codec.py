import unittest

from ante.codec import decode_payload, encode_payload


class CodecTests(unittest.TestCase):
    def test_json_object(self):
        body, kind = encode_payload({"task": "resize", "id": 42})
        self.assertEqual(kind, "application/json")
        self.assertEqual(decode_payload(body, kind), {"task": "resize", "id": 42})

    def test_text(self):
        body, kind = encode_payload("hello")
        self.assertEqual(kind, "text/plain")
        self.assertEqual(decode_payload(body, kind), "hello")

    def test_bytes(self):
        body, kind = encode_payload(b"\x00\x01")
        self.assertEqual(kind, "application/octet-stream")
        self.assertEqual(decode_payload(body, kind), b"\x00\x01")

    def test_missing_content_type_tries_json(self):
        self.assertEqual(decode_payload(b'{"ok":true}', None), {"ok": True})
        self.assertEqual(decode_payload(b"plain", None), "plain")

    def test_custom_parser(self):
        value = decode_payload(b"42", None, lambda raw, _kind: int(raw))
        self.assertEqual(value, 42)


if __name__ == "__main__":
    unittest.main()
