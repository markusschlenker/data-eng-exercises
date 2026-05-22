import boto3, requests, json, time

REGION = "eu-central-1"
STREAM_NAME = "wikimedia-stream--w8d2"
WIKI_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

kinesis = boto3.client("kinesis", region_name=REGION)
print(f"Sending Wikimedia events to Kinesis stream: {STREAM_NAME}")

headers = {"User-Agent": "AWS-Kinesis-Lab/1.0 (student@example.com)"}
count = 0
MAX_ITEMS = 50

with requests.get(WIKI_URL, headers=headers, stream=True, timeout=30) as resp:
    for line in resp.iter_lines(decode_unicode=True):
        if not line or not line.startswith("data: "):
            continue
        try:
            data = json.loads(line[6:])
        except json.JSONDecodeError:
            continue

        record = {
            "wiki": data.get("wiki"),
            "title": data.get("title"),
            "user": data.get("user"),
            "type": data.get("type"),
            "timestamp": data.get("timestamp")
        }

        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(record),
            PartitionKey=record["wiki"] or "default"
        )

        count += 1
        print(f"Sent {count}: {record['wiki']} | {record['title']}")
        if count >= MAX_ITEMS:
            break

print(f"Completed sending {count} records.")
