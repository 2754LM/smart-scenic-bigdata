import re
with open('/opt/jobs/../frontend/static/js/manage.js', 'r', encoding='utf-8') as f:
    text = f.read()
print(f"Total size: {len(text)}")
for m in re.finditer(r'^async function\s+(\w+)', text, re.M):
    print(f"  function {m.group(1)} at byte {m.start()}")
# Check for any error markers
if 'undefined' in text[170*40:175*40]:
    print("error in line 170-175 area")
print("area 170-175:")
print(repr(text[170*40:180*40]))