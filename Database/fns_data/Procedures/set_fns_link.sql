CREATE OR REPLACE PROCEDURE fns_data.set_fns_link()
 LANGUAGE plpgsql
AS $procedure$
	BEGIN
		INSERT into fns_data.link_ul_reg_organ(link_ul_reg_organ_key, ul_hash_key, reg_organ_hash_key)
		select hashtext(ul_hash_key::text || reg_organ_hash_key::text), ul_hash_key, reg_organ_hash_key
		from fns_storage.ul_fns_all_data
		where load_flg = true and reg_organ_hash_key is not null
		except
		select link_ul_reg_organ_key, ul_hash_key, reg_organ_hash_key
		from fns_data.link_ul_reg_organ;
		
		INSERT into fns_data.link_ul_predecessor(link_ul_pred_key, ul_hash_key, ul_pred_hash_key)
		select hashtext(ul_hash_key::text || ul_pred_hash_key::text), ul_hash_key, ul_pred_hash_key
		from fns_storage.ul_fns_all_data
		where load_flg = true and ul_pred_hash_key is not null
		except
		select link_ul_pred_key, ul_hash_key, ul_pred_hash_key
		from fns_data.link_ul_predecessor;

		INSERT into fns_data.link_ul_successor(link_ul_suc_key, ul_hash_key, ul_suc_hash_key)
		select hashtext(ul_hash_key::text || ul_suc_hash_key::text), ul_hash_key, ul_suc_hash_key
		from fns_storage.ul_fns_all_data
		where load_flg = true and ul_suc_hash_key is not null
		except
		select link_ul_suc_key, ul_hash_key, ul_suc_hash_key
		from fns_data.link_ul_successor;

		exception
				when others then raise notice '% - %', SQLSTATE, SQLERRM;
	END;
$procedure$
;