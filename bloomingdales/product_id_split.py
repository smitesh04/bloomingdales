from db_config import DbConfig
obj = DbConfig()
obj.cur.execute(f"select * from {obj.links_pdp_table} where product_id is null")
rows = obj.cur.fetchall()
for row in rows:
    link = row['link']
    # link_split = link.split('?ID=')
    link_split = link.split('?id=')
    product_id = link_split[-1]
    try:
        obj.cur.execute(f"update {obj.links_pdp_table} set product_id= '{product_id}' where link = '{link}'")
        obj.con.commit()
        print(row['id'])
    except Exception as e:
        print(e)
