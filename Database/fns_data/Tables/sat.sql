-- Таблицы-спутники:

/*
1.	fns_data.sat_ul_name – информация о наименовании юридического лица.
*/

CREATE TABLE fns_data.sat_ul_name (
ul_hash_key		integer,
fname 			text,
fname_grn 		varchar(13),
fname_grn_dt 		date,
sname 			text,
sname_grn  		varchar(13),
sname_grn_dt  		date,
actual_flg		bool,
load_date 		date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key)
);

/*
2.	fns_data.sat_ul_loc_addr – информация о месте нахождения и адресе юридического лица.
*/

CREATE TABLE fns_data.sat_ul_loc_addr (
ul_hash_key		integer,
location_ul  		text,
location_grn 		varchar(13),
location_grn_dt 	date,
address   		text,
address_grn		varchar(13),
address_grn_dt	date,
actual_flg		bool,
load_date 		date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key)
);

/*
3.	fns_data.sat_ul_registration – сведения о регистрации.
*/

CREATE TABLE fns_data.sat_ul_registration (
ul_hash_key			integer,
formation 			text,
ogrn_dt 			date,
registration_dt		date,
registration_grn		varchar(13), 
registration_grn_dt 		date,
actual_flg			bool,
load_date 			date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key)
);

/*
4.	fns_data.sat_ul_info – информация о состоянии, деятельности и записях в записях в ЕГРЮЛ.
*/

CREATE TABLE fns_data.sat_ul_info (
ul_hash_key			integer,
status 				text,
main_activity   		text,
main_act_grn	 		varchar(13),
main_act_grn_dt 		date,
additional_activity_cnt 	integer,
first_egrul_grn 		varchar(13),
first_egrul_grn_dt 		date,
last_egrul_grn 		varchar(13),
last_egrul_grn_dt 		date,
actual_flg			bool,
load_date 			date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key)
);

/*
5.	fns_data.sat_ul_end – сведения о прекращении юридического лица.
*/

CREATE TABLE fns_data.sat_ul_end (
ul_hash_key			integer,
ending 				text,
end_dt 				date,
end_organ				text,
end_grn				varchar(13),
end_grn_dt			date,
load_date 			date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key)
);

/*
6.	fns_data.sat_ul_reorg – сведения об реорганизации.
*/

CREATE TABLE fns_data.sat_ul_reorg (
ul_hash_key			integer,
reorg_form 			text,
reorg_grn 			varchar(13),
reorg_grn_dt 			date,
load_date 			date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key)
);

/*
7.	fns_data.sat_ul_capital – сведения об уставном капитале / складочном капитале / уставном фонде / паевом фонде.
*/
CREATE TABLE fns_data.sat_ul_capital (
ul_hash_key			integer,
capital 				text,
capital_size 			numeric,
capital_grn	 		varchar(13),
capital_grn_dt 		date,
actual_flg			bool,
load_date 			date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key)
);

/*
8.	fns_data.sat_ul_nalog – сведения об учете в налоговом органе.
*/

CREATE TABLE fns_data.sat_ul_nalog (
ul_hash_key			integer,
nalog_ul_kpp 			varchar(9),
nalog_accounting_date 	date,
nalog_organ 			text,
nalog_grn				varchar(13),
nalog_grn_dt			date,
actual_flg			bool,
load_date 			date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key)
);

/*
9.	fns_data.sat_ul_face – сведения о лице, имеющем право без доверенности действовать от имени юридического лица.
*/

CREATE TABLE fns_data.sat_ul_face (
ul_hash_key			integer,
ul_face 				text,
ul_face_inn 			varchar(12),
ul_face_grn			varchar(13),
ul_face_grn_dt 		date,
ul_face_post 			text,
ul_face_post_grn 		varchar(13),
ul_face_post_grn_dt 	date,
actual_flg			bool,
load_date 			date default current_date,
FOREIGN KEY (ul_hash_key) REFERENCES fns_data.hub_ul_fns (ul_hash_key)
);

/*
10.	 fns_data.sat_reg_organ - сведения о регистрирующем органе.
*/

CREATE TABLE fns_data.sat_reg_organ (
reg_organ_hash_key		integer,
reg_organ_addr 		text,
actual_flg			bool,
load_date 			date default current_date,
FOREIGN KEY (reg_organ_hash_key) REFERENCES fns_data.hub_reg_organ (reg_organ_hash_key)
);

/*
11.	fns_data.sat_ul_predecessor – информация о внесении записи в ЕГРЮЛ о право предшественнике юридического лица.
*/

CREATE TABLE fns_data.sat_ul_predecessor (
link_ul_pred_key		integer,
predecessor_grn 		varchar(13),
predecessor_grn_dt 		date,
actual_flg			bool,
load_date 			date default current_date,
FOREIGN KEY (link_ul_pred_key) REFERENCES fns_data.link_ul_predecessor (link_ul_pred_key)
);

/*
12.	fns_data.sat_ul_successor – информация о внесении записи в ЕГРЮЛ о право преемнике юридического лица.
*/

CREATE TABLE fns_data.sat_ul_successor (
link_ul_suc_key		integer,
successor_grn 			varchar(13),
successor_grn_dt 		date,
actual_flg			bool,
load_date 			date default current_date,
FOREIGN KEY (link_ul_suc_key) REFERENCES fns_data.link_ul_successor (link_ul_suc_key)
);

/*
13.	fns_data.sat_ul_reg_organ – информация о внесении записи в ЕГРЮЛ об органе, зарегистрировавшем юридическое лицо.
*/

CREATE TABLE fns_data.sat_ul_reg_organ (
link_ul_reg_organ_key	integer,
reg_organ_grn 			varchar(13),
reg_organ_grn_dt 		date,
actual_flg			bool,
load_date 			date default current_date,
FOREIGN KEY (link_ul_reg_organ_key) REFERENCES fns_data.link_ul_reg_organ (link_ul_reg_organ_key)
);
