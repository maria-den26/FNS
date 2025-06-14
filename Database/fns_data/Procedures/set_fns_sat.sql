CREATE OR REPLACE PROCEDURE fns_data.set_fns_sat()
 LANGUAGE plpgsql
AS $procedure$
	BEGIN
		-- 1. fns_data.sat_ul_name
		INSERT into fns_data.sat_ul_name(ul_hash_key, fname, fname_grn, fname_grn_dt, 
										 sname, sname_grn, sname_grn_dt, actual_flg)
		SELECT d.ul_hash_key, 
			   d.fname, 
			   substring(d.fname_grn_dt for 13)  as fname_grn, 
			   to_date(substring(d.fname_grn_dt from 14), 'dd.mm.yyyy') as fname_grn_dt, 
			   d.sname, 
			   substring(d.sname_grn_dt for 13) as sname_grn, 
			   to_date(substring(d.sname_grn_dt from 14), 'dd.mm.yyyy') as sname_grn_dt, 
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(d.fname,'') || 
									  coalesce(d.sname,''))  not in ( select md5(coalesce(t.fname, '') || coalesce(t.sname, ''))
														              from fns_data.sat_ul_name t
													                  where t.ul_hash_key = d.ul_hash_key
																	  and actual_flg = True);
		UPDATE fns_data.sat_ul_name
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and ul_hash_key in (select ul_hash_key 
							  from fns_data.sat_ul_name 
							  where actual_flg = True 
							  group by ul_hash_key, actual_flg
                              having count(*) > 1);
		-- 2. fns_data.sat_ul_loc_addr
		INSERT into fns_data.sat_ul_loc_addr(ul_hash_key, location_ul, location_grn, location_grn_dt, 
											 address, address_grn, address_grn_dt, actual_flg)
		SELECT d.ul_hash_key, 
			   d.location_ul, 
			   substring(d.location_grn_dt for 13) as location_grn, 
			   to_date(substring(d.location_grn_dt from 14), 'dd.mm.yyyy') as location_grn_dt, 
			   d.address, 
			   substring(d.address_grn_dt for 13) as address_grn, 
			   to_date(substring(d.address_grn_dt from 14), 'dd.mm.yyyy') as address_grn_dt,
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(d.location_ul,'') || 
								      coalesce(d.address,'')) not in (select md5(coalesce(t.location_ul,'') || coalesce(t.address,''))
															          from fns_data.sat_ul_loc_addr t
														              where t.ul_hash_key = d.ul_hash_key
																	  and actual_flg = True);
		UPDATE fns_data.sat_ul_loc_addr
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and ul_hash_key in (select ul_hash_key 
							  from fns_data.sat_ul_loc_addr 
							  where actual_flg = True 
							  group by ul_hash_key, actual_flg
                              having count(*) > 1);
		-- 3. fns_data.sat_ul_registration
		INSERT into fns_data.sat_ul_registration(ul_hash_key, formation, ogrn_dt, registration_dt, 
												 registration_grn, registration_grn_dt, actual_flg)
		SELECT d.ul_hash_key, 
			   d.formation, 
			   to_date(d.ogrn_dt, 'dd.mm.yyyy') as ogrn_dt, 
			   to_date(d.registration_dt, 'dd.mm.yyyy') as registration_dt,
			   substring(d.registration_grn_dt for 13) as registration_grn, 
			   to_date(substring(d.registration_grn_dt from 14), 'dd.mm.yyyy') as registration_grn_dt, 
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(d.formation,'') || coalesce(d.ogrn_dt::text,'') ||
								      coalesce(d.registration_dt::text,'')) not in (select  md5(coalesce(t.formation,'') || 
																					      coalesce(t.ogrn_dt::text,'') ||
																					      coalesce(t.registration_dt::text,''))
															          from fns_data.sat_ul_registration t
														              where t.ul_hash_key = d.ul_hash_key
																	  and actual_flg = True);
		UPDATE fns_data.sat_ul_registration
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and ul_hash_key in (select ul_hash_key 
							  from fns_data.sat_ul_registration 
							  where actual_flg = True 
							  group by ul_hash_key, actual_flg
                              having count(*) > 1);

		-- 4. fns_data.sat_ul_info
		INSERT into fns_data.sat_ul_info(ul_hash_key, status, main_activity, main_act_grn, main_act_grn_dt, additional_activity_cnt, 
										 first_egrul_grn, first_egrul_grn_dt, last_egrul_grn, last_egrul_grn_dt, actual_flg)
		SELECT d.ul_hash_key, 
			   d.status, 
			   d.main_activity,
			   substring(d.main_act_grn_dt for 13) as main_act_grn, 
			   to_date(substring(d.main_act_grn_dt from 14), 'dd.mm.yyyy') as main_act_grn_dt, 
			   coalesce(d.additional_activity_cnt::numeric::integer, 0) as additional_activity_cnt, 
			   substring(d.first_egrul for 13) as first_egrul_grn, 
			   to_date(substring(d.first_egrul from 14), 'dd.mm.yyyy') as first_egrul_grn_dt,
			   substring(d.last_egrul for 13) as last_egrul_grn, 
			   to_date(substring(d.last_egrul from 14), 'dd.mm.yyyy') as last_egrul_grn_dt,
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true 
		and md5(coalesce(d.status,'') || coalesce(d.main_activity,'') || 
				coalesce(d.additional_activity_cnt::text,'') || coalesce(d.reorg_form,'') || 
				coalesce(substring(d.first_egrul for 13),'') || coalesce( substring(d.last_egrul for 13),'')) not in 
			(select md5(coalesce(t.status,'') || coalesce(t.main_activity,'') || 
						coalesce(t.additional_activity_cnt::text,'') || coalesce(t.reorg_form,'') || 
						coalesce(t.first_egrul_grn,'') || coalesce(t.last_egrul_grn,''))
			 from fns_data.sat_ul_info t
			 where t.ul_hash_key = d.ul_hash_key and actual_flg = True);

		UPDATE fns_data.sat_ul_info
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and ul_hash_key in (select ul_hash_key 
							  from fns_data.sat_ul_info
							  where actual_flg = True 
							  group by ul_hash_key, actual_flg
                              having count(*) > 1);

		-- 5. fns_data.sat_ul_end 
		INSERT into fns_data.sat_ul_end(ul_hash_key, ending, end_dt, end_organ, end_grn, end_grn_dt)
		SELECT d.ul_hash_key, 
			   d.ending, 
			   to_date(d.end_dt, 'dd.mm.yyyy') as end_dt,
			   d.end_organ, 
			   substring(d.end_grn_dt for 13) as end_grn, 
			   to_date(substring(d.end_grn_dt from 14), 'dd.mm.yyyy') as end_grn_dt
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(d.ending,'') || coalesce(d.end_dt::text,'') ||
								      coalesce(d.end_organ,'')) not in (select  md5(coalesce(t.ending,'') || 
																				coalesce(t.end_dt::text,'') ||
																				coalesce(t.end_organ,''))
															            from fns_data.sat_ul_end t
														                where t.ul_hash_key = d.ul_hash_key);
		-- 6. fns_data.sat_ul_reorg 
		INSERT into fns_data.sat_ul_reorg(ul_hash_key, reorg_form, reorg_grn, reorg_grn_dt)
		SELECT d.ul_hash_key, 
			   d.reorg_form, 
			   substring(d.reorg_grn_dt for 13) as reorg_grn, 
			   to_date(substring(d.reorg_grn_dt from 14), 'dd.mm.yyyy') as reorg_grn_dt
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(d.reorg_form,'')) not in (select  md5(coalesce(t.reorg_form,''))
															            from fns_data.sat_ul_reorg t
														                where t.ul_hash_key = d.ul_hash_key);
		-- 7. fns_data.sat_ul_capital
		INSERT into fns_data.sat_ul_capital(ul_hash_key, capital, capital_size, capital_grn, capital_grn_dt, actual_flg)
		SELECT d.ul_hash_key, 
			   d.capital, 
			   d.capital_size::numeric as capital_size,
			   substring(d.capital_grn_dt for 13) as capital_grn, 
			   to_date(substring(d.capital_grn_dt from 14), 'dd.mm.yyyy') as capital_grn_dt, 
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(d.capital,'') || 
								      coalesce(d.capital_size::text,'')) not in (select md5(coalesce(t.capital,'') || 
																					  coalesce(t.capital_size::text,''))
																           from fns_data.sat_ul_capital t
															               where t.ul_hash_key = d.ul_hash_key
																		   and actual_flg = True);
		UPDATE fns_data.sat_ul_capital
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and ul_hash_key in (select ul_hash_key 
							  from fns_data.sat_ul_capital 
							  where actual_flg = True 
							  group by ul_hash_key, actual_flg
                              having count(*) > 1);
		-- 8. fns_data.sat_ul_nalog
		INSERT into fns_data.sat_ul_nalog(ul_hash_key, nalog_ul_kpp, nalog_accounting_date, 
										  nalog_organ, nalog_grn, nalog_grn_dt, actual_flg)
		SELECT d.ul_hash_key, 
			   d.nalog_ul_kpp, 
			   to_date(d.nalog_accounting_date, 'dd.mm.yyyy') as nalog_accounting_date, 
			   d.nalog_organ,
			   substring(d.nalog_grn_dt for 13) as nalog_grn, 
			   to_date(substring(d.nalog_grn_dt from 14), 'dd.mm.yyyy') as nalog_grn_dt, 
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(d.nalog_ul_kpp,'') || coalesce(d.nalog_accounting_date::text, '')||
								      coalesce(d.nalog_organ,'')) not in (select md5(coalesce(t.nalog_ul_kpp,'') || 
																					 coalesce(t.nalog_accounting_date::text, '')||
								      												 coalesce(t.nalog_organ,''))
																           from fns_data.sat_ul_nalog t
															               where t.ul_hash_key = d.ul_hash_key
																		   and actual_flg = True);
		UPDATE fns_data.sat_ul_nalog
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and ul_hash_key in (select ul_hash_key 
							  from fns_data.sat_ul_nalog 
							  where actual_flg = True 
							  group by ul_hash_key, actual_flg
                              having count(*) > 1);
		-- 9. fns_data.sat_ul_face
		INSERT into fns_data.sat_ul_face(ul_hash_key, ul_face, ul_face_inn, ul_face_grn, ul_face_grn_dt, 
										 ul_face_post, ul_face_post_grn, ul_face_post_grn_dt, actual_flg)
		SELECT d.ul_hash_key, 
			   d.ul_face, 
			   d.ul_face_inn,
			   substring(d.ul_face_grn_dt for 13) as ul_face_grn, 
			   to_date(substring(d.ul_face_grn_dt from 14), 'dd.mm.yyyy') as ul_face_grn_dt,  
			   d.ul_face_post,
			   substring(d.ul_face_post_grn_dt for 13) as ul_face_post_grn, 
			   to_date(substring(d.ul_face_post_grn_dt from 14), 'dd.mm.yyyy') as ul_face_post_grn_dt, 
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(d.ul_face,'') || coalesce(d.ul_face_inn, '')||
								      coalesce(d.ul_face_post,'')) not in (select md5(coalesce(t.ul_face,'') || 
																					 coalesce(t.ul_face_inn, '')||
								      												 coalesce(t.ul_face_post,''))
																           from fns_data.sat_ul_face t
															               where t.ul_hash_key = d.ul_hash_key
																		   and actual_flg = True);
		UPDATE fns_data.sat_ul_face
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and ul_hash_key in (select ul_hash_key 
							  from fns_data.sat_ul_face 
							  where actual_flg = True 
							  group by ul_hash_key, actual_flg
                              having count(*) > 1);
		-- 10. fns_data.sat_reg_organ
		INSERT into fns_data.sat_reg_organ(reg_organ_hash_key, reg_organ_addr, actual_flg)
		SELECT d.reg_organ_hash_key, 
			   d.reg_organ_addr, 
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(d.reg_organ_addr,'')) not in (select md5(coalesce(t.reg_organ_addr,''))
																	           from fns_data.sat_reg_organ t
																               where t.reg_organ_hash_key = d.reg_organ_hash_key
																			   and actual_flg = True);
		UPDATE fns_data.sat_reg_organ
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and reg_organ_hash_key in ( select reg_organ_hash_key
									  from fns_data.sat_reg_organ
									  where actual_flg = True 
									  group by reg_organ_hash_key, actual_flg
		                              having count(*) > 1);
		-- 11. fns_data.sat_ul_predecessor
		INSERT into fns_data.sat_ul_predecessor(link_ul_pred_key, predecessor_grn, predecessor_grn_dt, actual_flg)
		SELECT hashtext(d.ul_hash_key::text || d.ul_pred_hash_key::text) as link_ul_pred_key, 
			   substring(d.predecessor_grn_dt for 13) as predecessor_grn, 
			   to_date(substring(d.predecessor_grn_dt from 14), 'dd.mm.yyyy') as predecessor_grn_dt,  
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(substring(d.predecessor_grn_dt for 13),'')) not in (select md5(coalesce(t.predecessor_grn,''))
																	           from fns_data.sat_ul_predecessor t
																               where t.link_ul_pred_key = hashtext(d.ul_hash_key::text  || d.ul_pred_hash_key::text )
																			   and actual_flg = True);
		UPDATE fns_data.sat_ul_predecessor
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and link_ul_pred_key  in ( select link_ul_pred_key 
									  from fns_data.sat_ul_predecessor
									  where actual_flg = True 
									  group by link_ul_pred_key, actual_flg
		                              having count(*) > 1);
		-- 12. fns_data.sat_ul_successor
		INSERT into fns_data.sat_ul_successor(link_ul_suc_key, successor_grn, successor_grn_dt, actual_flg)
		SELECT hashtext(d.ul_hash_key::text || d.ul_suc_hash_key::text) as link_ul_suc_key, 
			   substring(d.successor_grn_dt for 13) as successor_grn, 
			   to_date(substring(d.successor_grn_dt from 14), 'dd.mm.yyyy') as successor_grn_dt,  
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(substring(d.successor_grn_dt for 13),'')) not in (select md5(coalesce(t.successor_grn,''))
																	           from fns_data.sat_ul_successor t
																               where t.link_ul_suc_key = hashtext(d.ul_hash_key::text  || d.ul_suc_hash_key::text )
																			   and actual_flg = True);
		UPDATE fns_data.sat_ul_successor
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and link_ul_suc_key  in ( select link_ul_suc_key 
									  from fns_data.sat_ul_successor
									  where actual_flg = True 
									  group by link_ul_suc_key, actual_flg
		                              having count(*) > 1);
		-- 13. fns_data.sat_ul_reg_organ
		INSERT into fns_data.sat_ul_reg_organ(link_ul_reg_organ_key, reg_organ_grn, reg_organ_grn_dt, actual_flg)
		SELECT hashtext(d.ul_hash_key::text || d.reg_organ_hash_key::text) as link_ul_reg_organ_key, 
			   substring(d.reg_organ_grn_dt for 13) as reg_organ_grn, 
			   to_date(substring(d.reg_organ_grn_dt from 14), 'dd.mm.yyyy') as reg_organ_grn_dt,  
			   True as actual_flg
		FROM fns_storage.ul_fns_all_data d
		WHERE load_flg = true and md5(coalesce(substring(d.reg_organ_grn_dt for 13),'')) not in (select md5(coalesce(t.reg_organ_grn,''))
																	           from fns_data.sat_ul_reg_organ t
																               where t.link_ul_reg_organ_key =  hashtext(d.ul_hash_key::text  || d.reg_organ_hash_key::text )
																			   and actual_flg = True);
		UPDATE fns_data.sat_ul_reg_organ
		SET actual_flg = False
		WHERE load_date <> current_date 
		  and link_ul_reg_organ_key  in ( select link_ul_reg_organ_key 
									  from fns_data.sat_ul_reg_organ
									  where actual_flg = True 
									  group by link_ul_reg_organ_key , actual_flg
		                              having count(*) > 1);
		exception
				when others then raise notice '% - %', SQLSTATE, SQLERRM;

	END;
$procedure$
;
