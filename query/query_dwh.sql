CREATE TABLE if not EXISTS bigdata_transaction (
	id_transaction text NOT null,
	id_customer text NOT NULL references bigdata_customer (id_customer),
	date_transaction date NOT NULL,
	product_transaction text NOT null references bigdata_product ("Type"),
	amount_transaction int NOT NULL,
	PRIMARY KEY (id_transaction)
);

create table if not exists bigdata_product(
	"Product" text not null,
	"Type" text not null,
	primary key ("Type")
)

CREATE TABLE if not EXISTS bigdata_customer (
	id_customer text NOT null,
	name_customer text NOT NULL,
	birthdate_customer date NOT NULL,
	gender_customer text NOT null,
	country_customer text NOT NULL,
	PRIMARY KEY (id_customer)
);

CREATE TABLE if not EXISTS search_log (
	id_search text NOT null,
	date_search date NOT NULL,
	product_search text NOT NULL,
	PRIMARY KEY (id_search)
);

CREATE TABLE if not EXISTS top_product (
	product_name text NOT null,
	total bigint NOT NULL
);