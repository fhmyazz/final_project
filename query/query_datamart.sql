CREATE TABLE if not EXISTS top_product (
	product_name text NOT null,
	total int NOT NULL
);

CREATE TABLE if not EXISTS count_search (
	date_search date NOT null,
	total_search int NOT NULL
);

CREATE TABLE if not EXISTS top_search (
	product_search text NOT null,
	search_count text NOT NULL
);