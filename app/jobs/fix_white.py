with open('/tmp/admin.js', 'r', encoding='utf-8') as f:
    s = f.read()
replacements = [
    ('background:#f8f9fa', 'background:#1a2350;color:#00d4ff'),
    ('background:#fff;', 'background:#2563eb;color:#fff;'),
    ('color:#666;', 'color:#9ca3af;'),
    ('color:#999;', 'color:#9ca3af;'),
    ('border-bottom:1px solid #eee', 'border-bottom:1px solid #2a3b6e'),
    ('background:#f5f5f5', 'background:#0a0e27;color:#9ca3af'),
]
for old, new in replacements:
    n = s.count(old)
    if n:
        s = s.replace(old, new)
        print(f'  {old!r} -> {n} replacement(s)')
with open('/tmp/admin.js', 'w', encoding='utf-8') as f:
    f.write(s)
print('done')