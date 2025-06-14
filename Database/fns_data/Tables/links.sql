-- Таблицы-линки:

/*
1.	fns_data.link_ul_predecessor – таблица, отражающая связь, между юридическим лицом и его право предшественником.
*/

CREATE TABLE fns_data.link_ul_predecessor (
link_ul_pred_key		integer PRIMARY KEY,
ul_hash_key 			integer,
ul_pred_hash_key		integer,
load_date 			date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key),
FOREIGN KEY (ul_pred_hash_key) REFERENCES fns_data.hub_ul_predecessor (ul_hash_key) 
);

/*
2.	fns_data.link_ul_successor - таблица, отражающая связь, между юридическим лицом и его право преемником.
*/

CREATE TABLE fns_data.link_ul_successor (
link_ul_suc_key		integer PRIMARY KEY,
ul_hash_key 			integer,
ul_suc_hash_key		integer,
load_date 			date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key),
FOREIGN KEY (ul_suc_hash_key) REFERENCES fns_data.hub_ul_successor (ul_hash_key) 
);

/*
3.	fns_data.link_ul_reg_organ - таблица, отражающая связь, между юридическим лицом и регистрирующим органом.
*/

CREATE TABLE fns_data.link_ul_reg_organ (
link_ul_reg_organ_key	integer PRIMARY KEY,
ul_hash_key 			integer,
reg_organ_hash_key		integer,
load_date 			date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key),
FOREIGN KEY (reg_organ_hash_key) REFERENCES fns_data.hub_reg_organ (reg_organ_hash_key) 
);
