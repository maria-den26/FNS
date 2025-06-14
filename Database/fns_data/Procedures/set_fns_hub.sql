CREATE OR REPLACE PROCEDURE fns_data.set_fns_hub()
 LANGUAGE plpgsql
AS $procedure$
	BEGIN
		INSERT into fns_data.hub_ul_fns(ul_hash_key, inn, ogrn)
		select ul_hash_key, nalog_ul_inn, ogrn
		from fns_storage.ul_fns_all_data
		where load_flg = true and ul_hash_key is not null
		except
 		select ul_hash_key, inn, ogrn
		from fns_data.hub_ul_fns;
		
		INSERT into fns_data.hub_ul_predecessor(ul_hash_key, inn, ogrn)
		select distinct ul_pred_hash_key, predecessor_inn, predecessor_ogrn
		from fns_storage.ul_fns_all_data
		where load_flg = true
		  and ul_pred_hash_key is not null 
		except
 		select ul_hash_key, inn, ogrn
		from fns_data.hub_ul_predecessor;

		INSERT into fns_data.hub_ul_successor(ul_hash_key, inn, ogrn)
		select distinct ul_suc_hash_key, successor_inn, successor_ogrn
		from fns_storage.ul_fns_all_data
		where load_flg = true
		  and ul_suc_hash_key is not null
		except
 		select ul_hash_key, inn, ogrn
		from fns_data.hub_ul_successor;

		INSERT into fns_data.hub_reg_organ(reg_organ_hash_key, reg_organ)
		select distinct reg_organ_hash_key, reg_organ
		from fns_storage.ul_fns_all_data
		where load_flg = true and reg_organ_hash_key is not null
		except
 		select reg_organ_hash_key, reg_organ
		from fns_data.hub_reg_organ;
	exception
		when others then raise notice '% - %', SQLSTATE, SQLERRM;
	END;
$procedure$
;
