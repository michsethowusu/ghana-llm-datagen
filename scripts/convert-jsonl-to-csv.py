import json
import csv
from pathlib import Path

input_file = 'research_0_47880.jsonl'
output_file = Path(input_file).stem + '.csv'

with open(input_file, 'r', encoding='utf-8') as f, open(output_file, 'w', newline='', encoding='utf-8') as out:
    writer = csv.writer(out)
    writer.writerow(['id', 'source_title', 'category', 'source_url', 'source_date', 'chunk_id', 'turn_number', 'role', 'content'])
    
    for line in f:
        if not line.strip():
            continue
        data = json.loads(line)
        base = [data.get(k, '') for k in ['id', 'source_title', 'category', 'source_url', 'source_date', 'chunk_id']]
        
        for i, msg in enumerate(data.get('conversations', []), 1):
            writer.writerow(base + [i, msg.get('role', ''), msg.get('content', '')])

print(f'✓ Created: {output_file}')
