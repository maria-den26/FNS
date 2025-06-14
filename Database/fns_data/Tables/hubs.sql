-- Таблицы-хабы:

/*
1.	fns_data.hub_ul_fns – таблица, идентифицирующая юридическое лицо. 
*/

CREATE TABLE fns_data.hub_ul_fns (
ul_hash_key 		integer PRIMARY KEY, 
inn 				varchar(10), 
ogrn 			varchar(13),
load_date 		date default current_date
);

/*
2.	fns_data.hub_ul_predecessor – таблица, идентифицирующая право предшественника.
*/

CREATE TABLE fns_data.hub_ul_predecessor (
ul_hash_key 		integer PRIMARY KEY, 
inn 				varchar(10), 
ogrn 			varchar(13),
load_date 		date default current_date
);

/*
3.	fns_data.hub_ul_successor – таблица, идентифицирующая право преемника.
*/

CREATE TABLE fns_data.hub_ul_successor (
ul_hash_key 		integer PRIMARY KEY, 
inn 				varchar(10), 
ogrn 			varchar(13),
load_date 		date default current_date
);

/*
4.	fns_data.hub_reg_organ – таблица, идентифицирующая регистрирующий орган.
*/

CREATE TABLE fns_data.hub_reg_organ (
reg_organ_hash_key	integer PRIMARY KEY,
reg_organ 		text,
load_date 		date default current_date
);
