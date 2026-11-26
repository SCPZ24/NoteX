"""Seed matching SQL relationships and ready Redis segments for warm load runs."""
import hashlib
import json
import random
import subprocess
import time


def prepare(work, namespace, adaptive, check, ladder):
    start = time.monotonic()
    fixture = json.loads((work/'fixture.json').read_text())
    posts = list(map(int, fixture['posts']))
    readers = fixture['readers']
    rng = random.Random(fixture['seed'])
    selected = set(random.Random(fixture['seed']).sample(posts[1:], 2587)) | {posts[0]}
    bitmaps = {}
    digest = hashlib.sha256()
    for offset in range(0, len(posts), 24):
        check()
        values = []
        expected = []
        for post in posts[offset:offset+24]:
            users = sorted(rng.sample(readers, 750))
            expected.append(str(post)+'\t'+','.join(map(str, users)))
            values.extend(f'({post},{user})' for user in users)
            if post in selected:
                bits = bytearray(4096)
                for user in users:
                    bits[user//8] |= 128 >> (user%8)
                bitmaps[post] = bytes(bits)
        ladder.sql('INSERT INTO post_likes VALUES '+','.join(values), namespace)
        rows = ladder.sql(f'SET SESSION group_concat_max_len=8192; SELECT post_id,GROUP_CONCAT(user_id ORDER BY user_id) FROM post_likes WHERE post_id BETWEEN {posts[offset]} AND {posts[min(offset+23,len(posts)-1)]} GROUP BY post_id ORDER BY post_id', namespace)
        if rows.splitlines() != expected:
            raise RuntimeError('SQL relationship fixture mismatch')
        digest.update((rows+'\n').encode())
    if adaptive:
        # Seed the production key layout in this run's isolated prefix.
        def pipe(commands):
            data = bytearray()
            for cmd in commands:
                data.extend(f'*{len(cmd)}\r\n'.encode())
                for arg in cmd:
                    arg = arg if isinstance(arg, bytes) else str(arg).encode()
                    data.extend(f'${len(arg)}\r\n'.encode()+arg+b'\r\n')
            result = subprocess.run(['docker','compose','exec','-T','redis','redis-cli','-n','15','--pipe'],input=data,capture_output=True,check=True)
            if f'errors: 0, replies: {len(commands)}'.encode() not in result.stdout:
                raise RuntimeError(result.stdout[-300:])
        cooldown = namespace+':like:cool'
        score = int(ladder.redis('TIME').splitlines()[0])+192
        verify = "if redis.call('EXISTS',KEYS[1])~=1 or redis.call('GETBIT',KEYS[2],0)~=1 or redis.sha1hex(redis.call('GET',KEYS[3]))~=ARGV[1] then return redis.error_reply('bitmap mismatch') end return 1"
        pairs = list(bitmaps.items())
        for i in range(0,len(pairs),128):
            check()
            commands=[]
            for post,bits in pairs[i:i+128]:
                base=f'{namespace}:like:{post}'
                hot,ready,data=[base+s for s in (':hot',':ready',':bits')]
                commands.extend([['SET',hot,f'warm-{post}'],['SETBIT',ready,0,1],['SET',data,bits],['ZADD',cooldown,score,post],['EVAL',verify,3,hot,ready,data,hashlib.sha1(bits).hexdigest()]])
            pipe(commands)
        if int(ladder.redis('ZCARD',cooldown))!=len(selected):
            raise RuntimeError('warm coverage mismatch')
    return dict(posts=len(posts),rows=len(posts)*750,warm=len(selected) if adaptive else 0,
                sql_hash=digest.hexdigest(),seconds=time.monotonic()-start)
