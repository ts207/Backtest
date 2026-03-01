import glob
import re

for filepath in glob.glob('/home/tstuv/workspace/backtest/project/pipelines/research/analyze_*.py'):
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Replace .csv with .parquet in out_dir paths
    content = re.sub(r'([A-Za-z0-9_]+)\.csv', r'\1.parquet', content)
    # Replace to_csv with to_parquet
    content = re.sub(r'\.to_csv\(([^,]+)(,\s*index=False)?', r'.to_parquet(\1, index=False', content)
    
    with open(filepath, 'w') as f:
        f.write(content)
print("Done")
