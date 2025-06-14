-- таблица, принимающая данные ЦБ РФ о ЮЛ от программного модуля

CREATE TABLE fns_storage.ul_cbrf_extract_data (
	ul_name text NULL,
	sname text NULL,
	fname text NULL,
	inn text NULL,
	ogrn text NULL,
	kpp text NULL,
	activity text NULL,
	address text NULL,
	country text NULL,
	status text NULL
);