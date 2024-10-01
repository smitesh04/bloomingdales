import pymysql

class DbConfig():

    def __init__(self):
        self.con = pymysql.Connect(host='localhost',
                              user='root',
                              password='actowiz',
                              database='bloomingdales')
        self.cur = self.con.cursor(pymysql.cursors.DictCursor)
        self.data_table = 'data'
        self.links_pdp_table = 'sitemap_links_pdp'
        self.links_cat_table = 'sitemap_links_cat'

    def check_table_exists(self, table_name):
        query = f"SHOW TABLES LIKE '{table_name}';"
        self.cur.execute(query)
        return self.cur.fetchone() is not None

    def create_data_table(self, data_table):
        if not self.check_table_exists(data_table):

            query = f'''
                CREATE TABLE if not exists `{data_table}`  (
                      `id` int NOT NULL AUTO_INCREMENT,
                      `category` varchar(100) DEFAULT NULL,
                      `scrapeDate` varchar(100) DEFAULT NULL,
                      `url` varchar(255) DEFAULT NULL,
                      `title` varchar(100) DEFAULT NULL,
                      `price` varchar(100) DEFAULT NULL,
                      `discountedPrice` varchar(100) DEFAULT NULL,
                      `color_list` varchar(100) DEFAULT NULL,
                      `size_list` varchar(100) DEFAULT NULL,
                      `reviewCount` int DEFAULT NULL,
                      `rating` double DEFAULT NULL,
                      `product_description` varchar(500) DEFAULT NULL,
                      `material_care_instruction` varchar(500) DEFAULT NULL,
                      `review_date` varchar(100) DEFAULT NULL,
                      `review_title` varchar(100) DEFAULT NULL,
                      `review_text` varchar(500) DEFAULT NULL,
                      `individual_review_rating` varchar(50) DEFAULT NULL,
                      `age` varchar(50) DEFAULT NULL,
                      `body_type` varchar(50) DEFAULT NULL,
                      `height` varchar(50) DEFAULT NULL,
                      `size_purchased` varchar(50) DEFAULT NULL,
                      `fits` varchar(50) DEFAULT NULL,
                      `weight` varchar(50) DEFAULT NULL,
                      `source_country` tinytext,
                      `pagesave_path` varchar(255) DEFAULT NULL,
                      PRIMARY KEY (`id`)
                    )
                '''

            self.cur.execute(query)
            self.con.commit()
            print(f'Table {data_table} has been created! ')

    def insert_data_table(self, item):

        query = f'''
                        INSERT INTO {self.data_table} (
                                        category,
                                        scrapeDate,
                                        url,
                                        title,
                                        price,
                                        discountedPrice,
                                        color_list,
                                        size_list,
                                        reviewCount,
                                        rating,
                                        product_description,
                                        material_care_instruction,
                                        review_date,
                                        review_title,
                                        review_text,
                                        individual_review_rating,
                                        age,
                                        body_type,
                                        height,
                                        size_purchased,
                                        fits,
                                        weight,
                                        source_country,
                                        hashid_review,
                                        pagesave_pdp,
                                        pagesave_review
                        )
                        VALUES (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s

                        
                        )
                        '''
        data = (
            item['category'],
            item['scrapeDate'],
            item['url'],
            item['title'],
            item['price'],
            item['discountedPrice'],
            item['color_list'],
            item['size_list'],
            item['reviewCount'],
            item['rating'],
            item['product_description'],
            item['material_care_instruction'],
            item['review_date'],
            item['review_title'],
            item['review_text'],
            item['individual_review_rating'],
            item['age'],
            item['body_type'],
            item['height'],
            item['size_purchased'],
            item['fits'],
            item['weight'],
            item['source_country'],
            item['hashid_review'],
            item['pagesave_pdp'],
            item['pagesave_review']
        )

        try:
            # print(query.format(data_table=self.data_table), data)

            self.cur.execute(query.format(data_table=self.data_table), data)
            self.con.commit()
            # print(item)
        except Exception as e:
            print(e)


    def insert_links_pdp_table(self, link):
        qr = f'''
            insert into {self.links_pdp_table}(
                        link)
            values (
                       '{link}'
            )
        '''
        print(qr)
        try:
            self.cur.execute(qr)
            self.con.commit()
        except Exception as e:print(e)
    def insert_links_cat_table(self, link):
        qr = f'''
            insert into {self.links_cat_table}(
                        link)
            values (
                       '{link}'
            )
        '''
        print(qr)
        try:
            self.cur.execute(qr)
            self.con.commit()
        except Exception as e:print(e)

    def update_links_pdp_status(self, link):
        qr = f'''
            update {self.links_pdp_table} set status = 1 where link = '{link}'
        '''
        self.cur.execute(qr)
        self.con.commit()
