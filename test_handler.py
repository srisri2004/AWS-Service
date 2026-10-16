import json, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambda"))
import handler as h

GOOD = Path(__file__).resolve().parents[1] / "data/sample/sales_20261015.csv"
BAD = Path(__file__).resolve().parents[1] / "data/sample/bad_header.csv"


class FakeS3:
    def __init__(self, data): self.data, self.calls = data, []
    def head_object(self, Bucket, Key): return {"ContentLength": len(self.data)}
    def get_object(self, Bucket, Key, Range):
        n = int(Range.split("-")[1]) + 1
        class B:  # minimal streaming body
            def read(_): return self.data[:n]
        return {"Body": B()}
    def copy_object(self, **kw): self.calls.append(("copy", kw["Key"]))
    def delete_object(self, **kw): self.calls.append(("delete", kw["Key"]))


class FakeGlue:
    def __init__(self): self.runs = []
    def start_job_run(self, JobName, Arguments):
        self.runs.append((JobName, Arguments)); return {"JobRunId": "jr_1"}


def event(key):
    return {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": key}}}]}


class HandlerTests(unittest.TestCase):
    def setUp(self):
        h.GLUE_JOB_NAME = "job"; self.glue = FakeGlue()
        h._clients["glue"] = self.glue

    def run_with(self, path, key):
        self.s3 = FakeS3(path.read_bytes()); h._clients["s3"] = self.s3
        return h.lambda_handler(event(key))[0]

    def test_valid_file_starts_glue(self):
        r = self.run_with(GOOD, "raw/sales_20261015.csv")
        self.assertEqual(r["action"], "glue_started")
        self.assertEqual(self.glue.runs[0][1], {"--SOURCE_PATH": "s3://b/raw/sales_20261015.csv"})

    def test_bad_header_is_quarantined(self):
        r = self.run_with(BAD, "raw/bad_header.csv")
        self.assertEqual(r["action"], "quarantined")
        self.assertIn("missing columns", r["reason"])
        self.assertEqual(self.s3.calls, [("copy", "quarantine/bad_header.csv"), ("delete", "raw/bad_header.csv")])
        self.assertEqual(self.glue.runs, [])

    def test_other_prefixes_ignored(self):
        r = self.run_with(GOOD, "processed/x.csv")
        self.assertEqual(r["action"], "ignored")

    def test_url_encoded_key(self):
        r = self.run_with(GOOD, "raw/my+file%281%29.csv")
        self.assertEqual(r["key"], "raw/my file(1).csv")

    def test_empty_file_quarantined(self):
        self.s3 = FakeS3(b""); h._clients["s3"] = self.s3
        self.assertEqual(h.lambda_handler(event("raw/e.csv"))[0]["action"], "quarantined")

    def test_ragged_rows_rejected(self):
        with self.assertRaises(h.InvalidFile):
            h.validate_sample("order_id,customer_id,product,quantity,unit_price,status,order_date,updated_at\n1,2\n3,4\n5,6\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
