import os
from db_config import DbConfig
obj = DbConfig()
obj.cur.execute(f"select count(id) from {obj.links_pdp_table} where status=0")
rows = obj.cur.fetchall()
end = rows[0]['count(id)']

start = 1
# end = 139912
num_parts = 1

# pincode = 400001

# Calculate the number of items in each part
items_per_part = (end - start + 1) // num_parts

# Initialize a list to store the range parts
range_parts = []

# Generate the range parts
for i in range(num_parts):
    part_start = start + i * items_per_part
    part_end = start + (i + 1) * items_per_part - 1 if i < num_parts - 1 else end
    range_parts.append((part_start, part_end))

# Print the range parts
for i, (part_start, part_end) in enumerate(range_parts):
    cmd = f'start "Part:{i+1}" scrapy crawl data -a start={part_start} -a end={part_end}'
    os.system(cmd)
    print(cmd)