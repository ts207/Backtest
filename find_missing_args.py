import re

with open("project/pipelines/run_all.py", "r") as f:
    content = f.read()

# Find all used args
used_args = set(re.findall(r'args\.([a-zA-Z0-9_]+)', content))

# Find all defined args
defined_args = set(re.findall(r'add_argument\(\s*"--([a-zA-Z0-9_]+)"', content))

missing = sorted(used_args - defined_args)

# Some used args might not be from CLI args, but let's check what's missing
print("Missing from parser:")
for m in missing:
    print(m)

