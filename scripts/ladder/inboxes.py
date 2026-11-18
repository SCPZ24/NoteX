"""Prepare a deterministic cohort through Feed Query and audit its Redis state."""
import hashlib
import random


READY_PERCENT = 75

# Each reply is one row, including readers whose empty inbox has no ZSET.
READ_STATE = """
local rows = {}
for i = 2, #ARGV do
    local base = ARGV[1] .. ':reader:{' .. ARGV[i] .. '}'
    local meta = base .. ':cache'
    local posts = base .. ':posts'
    local relations = base .. ':relations'
    local values = redis.call('HMGET', meta, 'ready', 'signature', 'count', 'generation')
    rows[#rows + 1] = table.concat({
        ARGV[i], values[1] or '-', values[2] or '-', values[3] or '-1',
        values[4] or '-', redis.call('PTTL', meta),
        redis.call('ZCARD', posts), redis.call('HLEN', relations),
        redis.call('PTTL', posts), redis.call('PTTL', relations),
        redis.call('EXISTS', meta, posts, relations, base .. ':deleted')
    }, ' ')
end
return rows
"""


def prepare(readers, seed, namespace, policy, following, read_feed, redis, check, save):
    """Construct 75% hot inboxes, inspect every reader, and save the gate result."""
    target = len(readers) * READY_PERCENT // 100
    warm = sorted(random.Random(seed).sample(readers, target))
    selected = set(warm)
    evidence = dict(passed=False, seed=seed, readers=len(readers), ready_percent=READY_PERCENT,
                    target=target, selected_ids=warm, ready_ids=[], cold_ids=[], mismatches=[])
    save(evidence)
    for reader in warm:
        check()
        try:
            page = read_feed(reader)
            if not isinstance(page.get('items'), list):
                raise ValueError('missing items list')
        except Exception as exc:
            evidence['failed_reader'] = reader
            save(evidence)
            raise RuntimeError(f'Feed preparation failed for reader {reader}') from exc

    edges = {reader: [] for reader in readers}
    for line in following.splitlines():
        user, relation = map(int, line.split())
        if user in edges:
            edges[user].append(relation)

    for offset in range(0, len(readers), 64):
        check()
        batch = readers[offset:offset + 64]
        rows = redis('EVAL_RO', READ_STATE, '0', namespace, *map(str, batch)).splitlines()
        if len(rows) != len(batch):
            raise RuntimeError('Redis returned an incomplete reader batch')
        for expected, line in zip(batch, rows):
            uid, flag, signature, count, generation, ttl, posts, relations, zttl, httl, exists = line.split()
            reader = int(uid)
            if reader != expected:
                raise RuntimeError('Redis reader order mismatch')
            fingerprint = '/'.join(map(str, [policy, reader, *edges[reader]]))
            valid = (flag == '1' and generation != '-' and int(ttl) > 0
                     and signature == hashlib.sha256(fingerprint.encode()).hexdigest()
                     and 0 <= int(count) == int(posts) == int(relations) <= 500
                     and (int(count) == 0 or min(int(zttl), int(httl)) > 0))
            if valid:
                evidence['ready_ids'].append(reader)
            if int(exists) == 0:
                evidence['cold_ids'].append(reader)
            if (reader in selected and not valid) or (reader not in selected and int(exists) != 0):
                evidence['mismatches'].append(line)
    evidence['passed'] = not evidence['mismatches']
    save(evidence)
    if not evidence['passed']:
        raise RuntimeError(f'inbox gate failed: {len(evidence["mismatches"])} mismatches')
    return evidence
