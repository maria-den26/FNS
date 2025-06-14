CREATE OR REPLACE PROCEDURE fns_storage.upload_fns_all_data()
 LANGUAGE plpgsql
AS $procedure$
	BEGIN
        -- обновляем рассчет хэша для новых данных в таблице fns_storage.ul_fns_extract_data_hash, объединяя данные из источников
		execute 'truncate table fns_storage.ul_fns_extract_data_hash';
		insert into fns_storage.ul_fns_extract_data_hash(rep_date, --1
														rep_num, --2
														fname, --3
														fname_grn_dt, --4
														sname, --5
														sname_grn_dt, --6
														location_ul, --7
														location_grn_dt, --8
														address, -- 9
														address_grn_dt, --10
														formation, --11
														ogrn,
														ogrn_dt,
														registration_dt,
														registration_grn_dt,
														reg_organ,
														reg_organ_addr,
														reg_organ_grn_dt,
														status,
														ending, --20
														end_dt,
														end_organ,
														end_grn_dt,
														predecessor_ogrn,
														predecessor_inn, --25
														predecessor_fname,
														predecessor_grn_dt,
														successor_ogrn,
														successor_inn,
														successor_fname, --30
														successor_grn_dt,
														ul_face,
														ul_face_inn,
														ul_face_grn_dt,
														ul_face_post, --35
														ul_face_post_grn_dt,
														capital,
														capital_size,
														capital_grn_dt,
														nalog_ul_inn, --40
														nalog_ul_kpp,
														nalog_accounting_date,
														nalog_organ,
														nalog_grn_dt,
														main_activity, --45
														main_act_grn_dt,
														additional_activity_cnt,
														reorg_form,
														reorg_grn_dt,
														first_egrul,
														last_egrul,
														rep_hash) --52
		select exf.rep_date, exf.rep_num, --2
			coalesce(exf.fname, excb.fname) as fname, --3
			exf.fname_grn_dt,
			coalesce(exf.sname, excb.sname) as sname,
			exf.sname_grn_dt,
			exf.location_ul,
			exf.location_grn_dt,
			exf.address,
			exf.address_grn_dt, --10
			exf.formation,
			coalesce(exf.ogrn, excb.ogrn) as ogrn, 
			exf.ogrn_dt,
			exf.registration_dt,
			exf.registration_grn_dt,
			exf.reg_organ,
			exf.reg_organ_addr,
			exf.reg_organ_grn_dt,
			coalesce(exf.status, excb.status) as status,
			exf.ending, --20
			exf.end_dt,
			exf.end_organ,
			exf.end_grn_dt,
			exf.predecessor_ogrn,
			exf.predecessor_inn,
			exf.predecessor_fname,
			exf.predecessor_grn_dt,
			exf.successor_ogrn,
			exf.successor_inn,
			exf.successor_fname, --30
			exf.successor_grn_dt,
			exf.ul_face,
			exf.ul_face_inn,
			exf.ul_face_grn_dt,
			exf.ul_face_post,
			exf.ul_face_post_grn_dt,
			exf.capital,
			exf.capital_size,
			exf.capital_grn_dt,
			coalesce(exf.nalog_ul_inn, excb.inn) as nalog_ul_inn, --40
			coalesce(exf.nalog_ul_kpp, excb.kpp) as nalog_ul_kpp,
			exf.nalog_accounting_date,
			exf.nalog_organ,
			exf.nalog_grn_dt,
			exf.main_activity,
			exf.main_act_grn_dt,
			exf.additional_activity_cnt,
			exf.reorg_form,
			exf.reorg_grn_dt,
			exf.first_egrul,--50
			exf.last_egrul, 
		MD5(coalesce(exf.fname, excb.fname, '')||
			coalesce(exf.fname_grn_dt, '')||
			coalesce(exf.sname, excb.sname, '')||
			coalesce(exf.sname_grn_dt,'')||
			coalesce(exf.location_ul,'')||
			coalesce(exf.location_grn_dt,'')||
			coalesce(exf.address,'')||
			coalesce(exf.address_grn_dt,'')||
			coalesce(exf.formation,'')||
			coalesce(exf.ogrn, excb.ogrn,'')||
			coalesce(exf.ogrn_dt,'')||
			coalesce(exf.registration_dt,'')||
			coalesce(exf.registration_grn_dt,'')||
			coalesce(exf.reg_organ,'')||
			coalesce(exf.reg_organ_addr,'')||
			coalesce(exf.reg_organ_grn_dt,'')||
			coalesce(exf.status, excb.status,'')||
			coalesce(exf.ending,'')||
			coalesce(exf.end_dt,'')||
			coalesce(exf.end_organ,'')||
			coalesce(exf.end_grn_dt,'')||
			coalesce(exf.predecessor_ogrn,'')||
			coalesce(exf.predecessor_inn,'')||
			coalesce(exf.predecessor_fname,'')||
			coalesce(exf.predecessor_grn_dt,'')||
			coalesce(exf.successor_ogrn,'')||
			coalesce(exf.successor_inn,'')||
			coalesce(exf.successor_fname,'')||
			coalesce(exf.successor_grn_dt,'')||
			coalesce(exf.ul_face,'')||
			coalesce(exf.ul_face_inn,'')||
			coalesce(exf.ul_face_grn_dt,'')||
			coalesce(exf.ul_face_post,'')||
			coalesce(exf.ul_face_post_grn_dt,'')||
			coalesce(exf.capital,'')||
			coalesce(exf.capital_size,'')||
			coalesce(exf.nalog_ul_inn, excb.inn, '')||
			coalesce(exf.nalog_ul_kpp, excb.kpp, '')||
			coalesce(exf.nalog_accounting_date,'')||
			coalesce(exf.nalog_organ,'')||
			coalesce(exf.nalog_grn_dt,'')||
			coalesce(exf.main_activity,'')||
			coalesce(exf.main_act_grn_dt,'')||
			coalesce(exf.additional_activity_cnt,'')||
			coalesce(exf.reorg_form,'')||
			coalesce(exf.reorg_grn_dt,'')||
			coalesce(exf.first_egrul,'')||
			coalesce(exf.last_egrul, '')) as rep_hash
		from fns_storage.ul_fns_extract_data exf
		join fns_storage.ul_cbrf_extract_data excb
		on exf.nalog_ul_inn = excb.inn;

        -- обновляем данные итоговой таблицы, являющейся источником для хранилища
		MERGE INTO fns_storage.ul_fns_all_data as t
		USING ( select  h.*,
						true as load_flg,
						hashtext(h.ogrn || h.nalog_ul_inn) as ul_hash_key,
						hashtext(coalesce(h.predecessor_ogrn, '') || coalesce(h.predecessor_inn, '')) as ul_pred_hash_key,
						hashtext(coalesce(h.successor_ogrn, '') || coalesce(h.successor_inn, '')) as ul_suc_hash_key,
						hashtext(h.reg_organ) as reg_organ_hash_key
				from fns_storage.ul_fns_extract_data_hash h) AS s
		ON t.rep_hash = s.rep_hash
		WHEN MATCHED THEN UPDATE
		    SET load_flg = false
		WHEN NOT MATCHED THEN
			INSERT (rep_date, rep_num, rep_hash, 
				fname, fname_grn_dt, sname, sname_grn_dt, 
				location_ul, location_grn_dt, address, address_grn_dt, 
				formation, ogrn, ogrn_dt, registration_dt, registration_grn_dt, 
				reg_organ, reg_organ_addr, reg_organ_grn_dt, status, 
				ending, end_dt, end_organ, end_grn_dt, 
				predecessor_ogrn, predecessor_inn, predecessor_fname, predecessor_grn_dt, 
				successor_ogrn, successor_inn, successor_fname, successor_grn_dt, 
				ul_face, ul_face_inn, ul_face_grn_dt, ul_face_post, ul_face_post_grn_dt, 
				capital, capital_size, capital_grn_dt, 
				nalog_ul_inn, nalog_ul_kpp, nalog_accounting_date, nalog_organ, nalog_grn_dt, 
				main_activity, main_act_grn_dt, additional_activity_cnt, 
				reorg_form,reorg_grn_dt, 
				first_egrul, last_egrul,
				load_flg, ul_hash_key, ul_pred_hash_key, ul_suc_hash_key, reg_organ_hash_key)
			VALUES (s.rep_date, s.rep_num, s.rep_hash, 
				s.fname, s.fname_grn_dt, s.sname, s.sname_grn_dt, 
				s.location_ul, s.location_grn_dt, s.address, s.address_grn_dt, 
				s.formation, s.ogrn, s.ogrn_dt, s.registration_dt, s.registration_grn_dt, 
				s.reg_organ, s.reg_organ_addr, s.reg_organ_grn_dt, s.status, 
				s.ending, s.end_dt, s.end_organ, s.end_grn_dt, 
				s.predecessor_ogrn, s.predecessor_inn, s.predecessor_fname, s.predecessor_grn_dt, 
				s.successor_ogrn, s.successor_inn, s.successor_fname, s.successor_grn_dt, 
				s.ul_face, s.ul_face_inn, s.ul_face_grn_dt, s.ul_face_post, s.ul_face_post_grn_dt, 
				s.capital, s.capital_size, s.capital_grn_dt, 
				s.nalog_ul_inn, s.nalog_ul_kpp, s.nalog_accounting_date, s.nalog_organ, s.nalog_grn_dt, 
				s.main_activity, s.main_act_grn_dt, s.additional_activity_cnt, 
				s.reorg_form, s.reorg_grn_dt, 
				s.first_egrul, s.last_egrul,
				s.load_flg, s.ul_hash_key, s.ul_pred_hash_key, s.ul_suc_hash_key, s.reg_organ_hash_key);
        -- оставляем только последние версии записей
        DELETE from fns_storage.ul_fns_all_data t
		WHERE to_date(t.rep_date, 'dd.mm.yyyy') <> ( select max(to_date(s.rep_date, 'dd.mm.yyyy')) 
													 from fns_storage.ul_fns_all_data s 
													 where s.ul_hash_key = t.ul_hash_key);
							 
		execute 'truncate table fns_storage.ul_fns_extract_data';
		execute 'truncate table fns_storage.ul_cbrf_extract_data';
		
	END;
$procedure$
;
