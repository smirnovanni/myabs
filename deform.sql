pragma macro( msg, 'debug_pipe(&METHOD$||TB$||[1], 0)',substitute);
pragma macro( lib, '[DATA_DEFORMATION]::[LIB]');

procedure run_streams(p_text string(1000), p_count_strems number, p_stream_name varchar2(100)) is
 res string(32000);
 err string(32000);
begin

	res := [TEXT_JOBS]::[APP_LIB].run_streams_wait(p_text
												  ,p_count_strems
												  ,p_stream_name);
	if Not res is null then
		err := 'Ошибка при исполнении кода через динамический PL/PLUS:'||NL$||res||NL$||'. Проверьте список текстовых заданий.';
	end if;

end;

procedure DeformationClOrg(P#STREAM#COUNT number(2,0), P#STREAM#NUM number(2,0), P_COMMIT number(4,0)) is
	counter		number default 0;
	class_		string(128);
	cl_org 		::[CL_ORG];
	i_name		::[CLIENT].[I_NAME]%type;
	o_name		::[CLIENT].[NAME]%type;		
begin

	/*
	
	для ускорения процедуры обезличивания предлагаю работать только со следующими полями:
	
	ФИО
	Возраст
	Номер паспорта
	ИНН
	Наименование организации
	Телефоны
	email
	PAN платежных карт
	
	*/
	
	declare
		type Q_1 is
			select a( a%rowid									: rid
					, to_char( a, 'TM9' )						: id
					, cast_to( [number], a.[ADDRESSES])			: addr
					--, cast_to( [number], a.[INSPECT])			: inspect
					--, cast_to( [number], a.[DOC_ARR])			: doc_arr
					--, cast_to( [number], a.[LINKS_OTHER])		: links_other
					--, cast_to( [number], a.[RELATIVES_CLIENT])	: rels
					--, cast_to( [number], a.[ACC_BANK])			: acc_bank
					--, cast_to( [number], a.[CONTACTS])			: contacts	-- контакты для связи
					--, cast_to( [number], a.[DECL_FIO])			: decl_fio	-- склонения наименований
					--, cast_to( [number], a.[NAMES])				: names		-- допустимые наименования
					--, cast_to( [number], a.[DOCS])				: docs		-- образцы печатей и подписей
					--, cast_to( [number], a.[RES_INFO])			: res_info	-- резервирование - информация о клиенте
					--, cast_to( [number], a.[FONDS_AR])			: fonds		-- социальные фонды
					)
				in	::[CL_ORG]
			 where MOD(a%id,P#STREAM#COUNT) = P#STREAM#NUM;

		type tp_cur is ref cursor return Q_1;
		cur tp_cur;	
		
		type tp_tab is table of Q_1;
		tab tp_tab;
		type t_rowid_tab is table of rowid;
		ri_tab t_rowid_tab;
		type t_cert_tab is table of [CERTIFICATE_ARR];
		tab_cert t_cert_tab;
		err_count integer;		
	begin
		cur.open(Q_1);
		counter	:= 0;

		loop
			cur.fetch_limit(P_COMMIT, tab);		-- Выборка с лимитом
			exit when tab.count = 0;
			
			ri_tab.delete;
			for ri in 1..tab.count loop
				ri_tab(ri) := tab(ri).RID;
			end loop;

			counter	:= counter + tab.count;

			&msg(TB$||' Поток '||to_char(P#STREAM#NUM+1)||' информация о клиентах - юридических лиц прочитана в количестве: '||&lib.n2ch(tab.count,counter));

		--	::[CL_ORG]
			begin
				execute immediate '
				begin
					forall j in indices of :tab save exceptions
					update Z#CL_ORG set
						SN = nvl(SN, 1) + 1, SU = rtl.uid$
						where ROWID = :tab(j);
				exception when others then
					if SQLCODE = -24381 then
						:ec := SQL%BULK_EXCEPTIONS.count;
					end if;
				end;'
				using ri_tab, in out err_count
				;
			end;
			commit;		
		
		--	::[CLIENT]
			update for j in tab exceptionloop
				y	( y.[INN]		= null	-- ИНН
					, y.[I_NAME]	= 'Organization ' || to_char(y)	-- интернациональное наименование
					, y.[SI_NAME]	= null	-- Краткое интернациональное наименование
					, y.[NAME]		= 'Организация ' || y%id
					, y.[NOTES]		= null	-- примечания
					)
				in	::[CLIENT]
			where	y%id = tab(j).id;
			commit;
			&msg(TB$||TB$||' Поток '||to_char(P#STREAM#NUM+1)||' ...данные ::[CLIENT] деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- деформация данных об адресах
			update for j in tab exceptionloop
				y	( y.[FLAT] 			= '1'
					, y.[HOUSE]			= '456'
					, y.[KORPUS]		= 'Б'
					, y.[POST_CODE]		= '654321'
					, y.[STREET_STR]	= 'улица Неизвестного героя'
					, y.[STREET_REF]	= null
					, y.[IMP_STR]		= null
					)
				in	::[PERSONAL_ADDRESS] all
			where	cast_to( [number], y%collection ) = tab(j).addr;
			commit;
			&msg(TB$||TB$||'...данные о клиентах - юридических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));
		
		exit when cur.notfound;
		end loop;
		cur.close;
		tab.delete;
	end;

	&msg(' Поток '||to_char(P#STREAM#NUM+1)||' Обработано '|| to_char( counter, 'TM9' ) || ' клиентов - юридических лиц');

end;

procedure run_DeformationClOrg(p_count_strems number, p_commit number) is
	v_text string(1000);
	ts1 number;
	ts2 number;
	error_flag	boolean default false;
	error_text	varchar2(32000);	
begin

	ts1 := utils.get_time;

	v_text := '::[CLIENT].[BRK_DEFORMATION].[DeformationClOrg](P$S$C,P$S$N,'||to_char(p_commit)||');'||nl$;	
	
	begin
		-- begin pl/sql
		--Z#CLIENT

	    execute immediate 'alter trigger "IBS"."CL_HIST_Z#CLIENT" disable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CLIENT"  disable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CLIENT_SET_PN"  disable';
	
	    --Z#CL_CORP
	    execute immediate 'alter trigger "IBS"."CL_HIST_Z#CL_CORP" disable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CL_CORP" disable';
	
	    --Z#PERSONAL_ADDR
	    execute immediate 'alter trigger "IBS"."LOG_Z#PERSONAL_ADDRESS" disable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#PERSONAL_ADDR" disable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#PERSONAL_ADDR" disable';	
	    execute immediate 'alter trigger "IBS"."USR_Z#PERSONAL_ADDRESS_MDFDAT" disable';	
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#PERSONAL_ADDR" disable';	
	
	    execute immediate 'alter trigger "IBS"."DEL_Z#CL_HIST" disable';

		--Z#CERTIFICATE
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#CERTIFICATE"   disable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CERTIFICATE" disable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#CERTIFICATE" disable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#CERTIFICATE" disable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CERTIFICATE_MODIFY_DATE" disable';
	
	    --Z#CONTACTS
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#CONTACTS" disable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#CONTACTS" disable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#CONTACTS" disable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CONTACTS" disable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CONTACTS_MODIFY_DATE" disable';
	
	    --Z#NAMES
	    execute immediate 'alter trigger "IBS"."DEL_Z#NAMES" disable';
 		
 		-- end pl/sql  	
	
		commit;
 	exception when others then
 		error_text := 'Ошибка при отключении триггеров!!! '||SQLERRM;
 		error_flag := true;
	end;	
	
	if not error_flag then
		run_streams(v_text, p_count_strems, 'ClOrgDeformation');
	end if;
	
 	-- begin pl/sql
		--Z#CLIENT
	    execute immediate 'alter trigger "IBS"."CL_HIST_Z#CLIENT" enable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CLIENT"  enable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CLIENT_SET_PN"  enable';
	
	    --Z#CL_CORP
	    execute immediate 'alter trigger "IBS"."CL_HIST_Z#CL_CORP" enable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CL_CORP" enable';
	
	    --Z#PERSONAL_ADDR
	    execute immediate 'alter trigger "IBS"."LOG_Z#PERSONAL_ADDRESS" enable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#PERSONAL_ADDR" enable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#PERSONAL_ADDR" enable';	
	    execute immediate 'alter trigger "IBS"."USR_Z#PERSONAL_ADDRESS_MDFDAT" enable';	
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#PERSONAL_ADDR" enable';	
	
	    execute immediate 'alter trigger "IBS"."DEL_Z#CL_HIST" enable';

		--Z#CERTIFICATE
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#CERTIFICATE"   enable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CERTIFICATE" enable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#CERTIFICATE" enable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#CERTIFICATE" enable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CERTIFICATE_MODIFY_DATE" enable';
	
	    --Z#CONTACTS
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#CONTACTS" enable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#CONTACTS" enable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#CONTACTS" enable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CONTACTS" enable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CONTACTS_MODIFY_DATE" enable';
	
	    --Z#NAMES
	    execute immediate 'alter trigger "IBS"."DEL_Z#NAMES" enable';
 	-- end pl/sql  	
	commit;	
	
	ts2 := utils.get_time;
	
	&msg('Обезличивание юрлиц выполнено за '||to_char((ts2-ts1)/100/60,'999990.00')||' мин.');
	
end;


procedure DeformationClPriv(P#STREAM#COUNT number(2,0), P#STREAM#NUM number(2,0), P_COMMIT number(4,0)) is
	counter		number default 0;
	cl_priv 	::[CL_PRIV];
begin

	/*
	
	для ускорения процедуры обезличивания предлагаю работать только со следующими полями:
	
	ФИО
	Возраст
	Номер паспорта
	ИНН
	Наименование организации
	Телефоны
	email
	PAN платежных карт
	
	*/

	declare
		type Q_1 is
			select a( a%rowid									: rid
					, to_char( a, 'TM9' )						: id
					
					, cast_to( [number], a.[ADDRESSES])			: addr
					, cast_to( [number], a.[INSPECT])			: inspect
					, cast_to( [number], a.[DOC_ARR])			: doc_arr
					, cast_to( [number], a.[LINKS_OTHER])		: links_other
					, cast_to( [number], a.[RELATIVES_CLIENT])	: rels
					, cast_to( [number], a.[ACC_BANK])			: acc_bank
					, cast_to( [number], a.[CONTACTS])			: contacts	-- контакты для связи
					, cast_to( [number], a.[DECL_FIO])			: decl_fio	-- склонения наименований
					, cast_to( [number], a.[NAMES])				: names		-- допустимые наименования
					, cast_to( [number], a.[DOCS])				: docs		-- образцы печатей и подписей
					, cast_to( [number], a.[RES_INFO])			: res_info	-- резервирование - информация о клиенте
					, cast_to( [number], a.[FONDS_AR])			: fonds		-- социальные фонды
					)
				in	::[CL_PRIV]
			 where MOD(a%id,P#STREAM#COUNT) = P#STREAM#NUM;

		type tp_cur is ref cursor return Q_1;
		cur tp_cur;

		type tp_tab is table of Q_1;
		tab tp_tab;
		type t_rowid_tab is table of rowid;
		ri_tab t_rowid_tab;
		
		type t_id_tab is table of number;
		id_tab t_id_tab;		
		
		type t_cert_tab is table of [CERTIFICATE_ARR];
		tab_cert t_cert_tab;
		err_count integer;
	begin
		cur.open(Q_1);
		counter	:= 0;

		loop
			cur.fetch_limit(P_COMMIT, tab);		-- Выборка с лимитом
			exit when tab.count = 0;
			
			counter	:= counter + tab.count;

			&msg(TB$||' Поток '||to_char(P#STREAM#NUM+1)||' информация о клиентах - физических лиц прочитана в количестве: '||&lib.n2ch(tab.count,counter));
			
			ri_tab.delete;
			for ri in 1..tab.count loop
				ri_tab(ri) := tab(ri).RID;
			end loop;
		--	::[CL_PRIV]
			begin
				execute immediate '
				begin
					forall j in indices of :tab save exceptions
					update Z#CL_PRIV set
						SN = nvl(SN, 1) + 1, SU = rtl.uid$
						, C_DOC#BIRTH_PLACE = ''_Место_ _рождения_ '' || to_char(ID)
						, C_DOC#NUM = ''1234567890''
						, C_DOC#SER = ''XXYY''
						, C_DOC#WHO = ''ПВС УВД МВД РБ''
						, C_DATE_PERS = to_date(''09/05/1945'', ''DD/MM/YYYY'')
						, C_BORN = null
						, C_LOW = null
						, C_FAMILY_CL = ''Физ_лицо'' || to_char(ID)
						, C_NAME_CL = ''Имя'' || substr(to_char(ID), -3)
						, C_SNAME_CL = ''Отчество'' || substr(to_char(ID), -6, 3)
						, C_SNILS = null
						where ROWID = :tab(j);
				exception when others then
					if SQLCODE = -24381 then
						:ec := SQL%BULK_EXCEPTIONS.count;
					end if;
				end;'
				using ri_tab, in out err_count
				;
			end;
			commit;
			
			id_tab.delete;
			for idd in 1..tab.count loop
				id_tab(idd) := tab(idd).id;
			end loop;
			--	::[CLIENT]
			begin
				execute immediate '
				begin
					forall j in indices of :tab save exceptions
					update Z#CLIENT set
						SN = nvl(SN, 1) + 1, SU = rtl.uid$
						, C_INN 		= null
						, C_I_NAME 		= ''PRIVATE '' || to_char(ID)
						, C_SI_NAME 	= null
						, C_NAME		= ''Физ_лицо'' || to_char(ID) ||'' Имя''||substr(to_char(ID),-3) ||'' Отчество''||substr(to_char(ID),-6,3)
						, C_NOTES		= null
						where ID = :tab(j).id;
				exception when others then
					if SQLCODE = -24381 then
						:ec := SQL%BULK_EXCEPTIONS.count;
					end if;
				end;'
				using id_tab
				;
			end;
			commit;			

		--	::[CLIENT]
		/*
			update for j in tab exceptionloop
				y	( y.[INN]		= null	-- ИНН
					, y.[I_NAME]	= 'PRIVATE ' || to_char(y)	-- интернациональное наименование
					, y.[SI_NAME]	= null	-- Краткое интернациональное наименование
				--	, y.[NAME]		= y.[FAMILY_CL] || ' ' || y.[NAME_CL] || ' ' || y.[SNAME_CL] --было 'Физическое лицо ' || a%id;
					, y.[NAME]		= 'Физ_лицо' || y%id ||' Имя'||substr(to_char(y),-3) ||' Отчество'||substr(to_char(y),-6,3)
					, y.[NOTES]		= null	-- примечания
					)
				in	::[CLIENT]
			where	y%id = tab(j).id;
			commit;*/
			&msg(TB$||TB$||' Поток '||to_char(P#STREAM#NUM+1)||' ...данные ::[CLIENT] деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));
/*
			-- деформация данных об адресах
			update for j in tab loop
				y	( y.[FLAT] 			= '1'
					, y.[HOUSE]			= '456'
					, y.[KORPUS]		= 'Б'
					, y.[POST_CODE]		= '654321'
					, y.[STREET_STR]	= 'улица Неизвестного героя'
					, y.[STREET_REF]	= null
					, y.[IMP_STR]		= null
					)
				in	::[PERSONAL_ADDRESS] all
			where	cast_to( [number], y%collection ) = tab(j).addr;
			commit;
			&msg(TB$||TB$||'...данные о клиентах - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- деформация данных о доп. удостоверениях
			tab_cert.delete;
			for i in 1..tab.count loop
				tab_cert(i) := tab(i).doc_arr;
			end loop;
			execute immediate '
			begin
				forall j in indices of :tab save exceptions
				update Z#CERTIFICATE
				set
					SN = nvl(SN, 1) + 1, SU = rtl.uid$
					, C_BIRTH_PLACE = ''_Место_ _рождения_ '' || :tab(j)
					, C_NUM = ''1234567890''
					, C_SER = ''XXYY''
					, C_WHO = ''ПВС УВД МВД РБ''
				where COLLECTION_ID = :tab(j);
			end;'
			using tab_cert;
			commit;
			&msg(TB$||TB$||'...данные о доп. удостоверениях клиентов - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- Склонения наименований
			update for j in tab exceptionloop
				y	( y.[NAME]	= null )
				in	::[DECLENSION] all
			where	cast_to( [number], y%collection ) = tab(j).decl_fio;
			commit;
			&msg(TB$||TB$||' Поток '||to_char(P#STREAM#NUM+1)||' ...данные о склонений наименований клиентов - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));
			*//*
			-- Допустимые наименования
			update for j in tab exceptionloop
				y	( y.[NAME]	= null )
				in	::[NAMES] all
			where	cast_to( [number], y%collection ) = tab(j).names;
			commit;
			&msg(TB$||TB$||' Поток '||to_char(P#STREAM#NUM+1)||' ...данные о допустимых наименований клиентов - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));
			*/
		exit when cur.notfound;
		end loop;
		cur.close;
		tab.delete;
	end;

	&msg(' Поток '||to_char(P#STREAM#NUM+1)||' Обработано '|| to_char( counter, 'TM9' ) || ' клиентов - физических лиц');

end;

procedure run_DeformationClPriv(p_count_strems number, p_commit number) is
	v_text string(1000);
	ts1 number;
	ts2 number;
	error_flag	boolean default false;
	error_text	varchar2(32000);
begin
	
	ts1 := utils.get_time;
	
	v_text := '::[CLIENT].[BRK_DEFORMATION].[DeformationClPriv](P$S$C,P$S$N,'||to_char(p_commit)||');'||nl$;	

	begin
		-- begin pl/sql
		--Z#CLIENT

	    execute immediate 'alter trigger "IBS"."CL_HIST_Z#CLIENT" disable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CLIENT"  disable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CLIENT_SET_PN"  disable';
	
	    --Z#CL_PRIV
	    execute immediate 'alter trigger "IBS"."CL_HIST_Z#CL_PRIV" disable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CL_PRIV" disable';
	
	    --Z#PERSONAL_ADDR
	    execute immediate 'alter trigger "IBS"."LOG_Z#PERSONAL_ADDRESS" disable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#PERSONAL_ADDR" disable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#PERSONAL_ADDR" disable';	
	    execute immediate 'alter trigger "IBS"."USR_Z#PERSONAL_ADDRESS_MDFDAT" disable';	
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#PERSONAL_ADDR" disable';	
	
	    execute immediate 'alter trigger "IBS"."DEL_Z#CL_HIST" disable';

		--Z#CERTIFICATE
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#CERTIFICATE"   disable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CERTIFICATE" disable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#CERTIFICATE" disable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#CERTIFICATE" disable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CERTIFICATE_MODIFY_DATE" disable';
	
	    --Z#CONTACTS
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#CONTACTS" disable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#CONTACTS" disable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#CONTACTS" disable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CONTACTS" disable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CONTACTS_MODIFY_DATE" disable';
	
	    --Z#NAMES
	    execute immediate 'alter trigger "IBS"."DEL_Z#NAMES" disable';
 		
 		-- end pl/sql  	
	
		commit;
 	exception when others then
 		error_text := 'Ошибка при отключении триггеров!!! '||SQLERRM;
 		error_flag := true;
	end;

 		
	if not error_flag then
		run_streams(v_text, p_count_strems, 'ClPrivDeformation');
	end if;
	
 	-- begin pl/sql
		--Z#CLIENT
	    execute immediate 'alter trigger "IBS"."CL_HIST_Z#CLIENT" enable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CLIENT"  enable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CLIENT_SET_PN"  enable';
	
	    --Z#CL_PRIV
	    execute immediate 'alter trigger "IBS"."CL_HIST_Z#CL_PRIV" enable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CL_PRIV" enable';
	
	    --Z#PERSONAL_ADDR
	    execute immediate 'alter trigger "IBS"."LOG_Z#PERSONAL_ADDRESS" enable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#PERSONAL_ADDR" enable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#PERSONAL_ADDR" enable';	
	    execute immediate 'alter trigger "IBS"."USR_Z#PERSONAL_ADDRESS_MDFDAT" enable';	
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#PERSONAL_ADDR" enable';	
	
	    execute immediate 'alter trigger "IBS"."DEL_Z#CL_HIST" enable';

		--Z#CERTIFICATE
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#CERTIFICATE"   enable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CERTIFICATE" enable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#CERTIFICATE" enable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#CERTIFICATE" enable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CERTIFICATE_MODIFY_DATE" enable';
	
	    --Z#CONTACTS
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#CONTACTS" enable';
	    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#CONTACTS" enable';
	    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#CONTACTS" enable';
	    execute immediate 'alter trigger "IBS"."LOG_Z#CONTACTS" enable';
	    execute immediate 'alter trigger "IBS"."USR_Z#CONTACTS_MODIFY_DATE" enable';
	
	    --Z#NAMES
	    execute immediate 'alter trigger "IBS"."DEL_Z#NAMES" enable';
 	-- end pl/sql  	
	commit;	

	-- begin pl/sql
	execute immediate 'truncate table Z#CL_HIST';
	-- end pl/sql
	commit;
	
	ts2 := utils.get_time;
	
	&msg('Обезличивание физлиц выполнено за '||to_char((ts2-ts1)/100/60,'999990.00')||' мин.');
	
end;

procedure DeformationPANCards(P#STREAM#COUNT number(2,0), P#STREAM#NUM number(2,0), P_COMMIT number(4,0)) is
	counter		number default 0;
begin

	declare
		type Q_1 is
			select a( a%rowid									: rid
					, to_char( a, 'TM9' )						: id
					)
				in	::[IP_CARDS]
			 where MOD(a%id,P#STREAM#COUNT) = P#STREAM#NUM;

		type tp_cur is ref cursor return Q_1;
		cur tp_cur;
		type t_rowid_tab is table of rowid;
		ri_tab t_rowid_tab;
		type tp_tab is table of Q_1;
		tab tp_tab;
	begin
		cur.open(Q_1);
		counter	:= 0;

		loop
			cur.fetch_limit(P_COMMIT, tab);		-- Выборка с лимитом
			exit when tab.count = 0;
			
			ri_tab.delete;
			for ri in 1..tab.count loop
				ri_tab(ri) := tab(ri).RID;
			end loop;			
			
			counter	:= counter + tab.count;

			&msg(TB$||' Поток '||to_char(P#STREAM#NUM+1)||' информация о пластиковых картах прочитана в количестве: '||&lib.n2ch(tab.count,counter));

			begin
				execute immediate '
				begin
					forall j in indices of :tab save exceptions
					update Z#IP_CARDS set
						SN = nvl(SN, 1) + 1, SU = rtl.uid$
						, C_PAN = substr(C_PAN, 1, 4)||substr(to_char(ID), -8)||substr(C_PAN, -4)
						where ROWID = :tab(j);
				exception when others then
					if SQLCODE = -24381 then
						:ec := SQL%BULK_EXCEPTIONS.count;
					end if;
				end;'
				using ri_tab
				;
			end;
			commit;
			
			update for j in tab exceptionloop
				y	( y.[EMBOSS_LAST_NAME]	= case when not y.[EMBOSS_LAST_NAME]	is null then	'EMBOSS_'||substr(to_char(y), -3)													end
					, y.[EMBOSSING_NAME]	= case when not y.[EMBOSSING_NAME]		is null then	'NAME_'||substr(to_char(y), -3)														end
					, y.[EMB_COMP_NAME]		= case when not y.[EMB_COMP_NAME]		is null then	'COMP_'||to_char(y)																	end
					, y.[SOC_CARD_NUM]		= case when not y.[SOC_CARD_NUM]		is null then	substr(y.[SOC_CARD_NUM], 1, 4)||substr(to_char(y), -7)||substr(y.[SOC_CARD_NUM], -4)end
					)
					in	::[VZ_CARDS]
				where	y%id = tab(j).id;

			&msg(TB$||TB$||' Поток '||to_char(P#STREAM#NUM+1)||' ...[1/2] данные ::[VZ_CARDS] о картах МПС деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));
			commit;			
			
		exit when cur.notfound;
		end loop;
		cur.close;
		tab.delete;
	end;

	&msg(' Поток '||to_char(P#STREAM#NUM+1)||' Обработано '|| to_char( counter, 'TM9' ) || ' пластиковых карт');

end;

procedure run_DeformationPANCards(p_count_strems number, p_commit number) is
	v_text string(1000);
	ts1 number;
	ts2 number;
begin
	
	ts1 := utils.get_time;
	
	v_text := '::[CLIENT].[BRK_DEFORMATION].[DeformationPANCards](P$S$C,P$S$N,'||to_char(p_commit)||');'||nl$;	
	run_streams(v_text, p_count_strems, 'PANCARDSDeformation');
	
	ts2 := utils.get_time;
	
	&msg('Обезличивание пластиковых карт выполнено за '||to_char((ts2-ts1)/100/60,'999990.00')||' мин.');
	
end;

procedure DeformContacts(P#STREAM#COUNT number(2,0), P#STREAM#NUM number(2,0), P_COMMIT number(4,0)) is
	counter		number default 0;
begin

	declare
		
		type Q_1 is
			select a( a%rowid									: rid
					, to_char( a, 'TM9' )						: id
					)
				in	::[CONTACTS] all
			 where MOD(a%id,P#STREAM#COUNT) = P#STREAM#NUM
			   lock nowait(a);
			   --lock nowait(a) skiplocked;
		
		type tp_cur is ref cursor return Q_1;
		cur tp_cur;
		type t_rowid_tab is table of rowid;
		ri_tab t_rowid_tab;
		type tp_tab is table of Q_1;
		tab tp_tab;

	begin
		cur.open(Q_1);
		counter	:= 0;

		loop
			cur.fetch_limit(P_COMMIT, tab);		-- Выборка с лимитом
			exit when tab.count = 0;

			ri_tab.delete;
			for ri in 1..tab.count loop
				ri_tab(ri) := tab(ri).RID;
			end loop;			
			
			counter	:= counter + tab.count;

			&msg(TB$||' Поток '||to_char(P#STREAM#NUM+1)||' информация о контактах прочитана в количестве: '||&lib.n2ch(tab.count,counter));

			begin
				execute immediate '
				begin
					forall j in indices of :tab save exceptions
					update Z#CONTACTS set
						SN = nvl(SN, 1) + 1, SU = rtl.uid$
						, С_NUMB	= substr(to_char(ID),1,10) )
						where ROWID = :tab(j);
				exception when others then
					if SQLCODE = -24381 then
						:ec := SQL%BULK_EXCEPTIONS.count;
					end if;
				end;'
				using ri_tab
				;
			end;
			commit;

			&msg(TB$||TB$||' Поток '||to_char(P#STREAM#NUM+1)||' ...данные о контактах клиентов - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));
			
		exit when cur.notfound;
		end loop;
		cur.close;
		tab.delete;
	end;
	
end;

procedure run_DeformContacts(p_count_strems number, p_commit number) is
	v_text string(1000);
	ts1 number;
	ts2 number;
begin
	
	ts1 := utils.get_time;

    --Z#CONTACTS
    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#CONTACTS" disable';
    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#CONTACTS" disable';
    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#CONTACTS" disable';
    execute immediate 'alter trigger "IBS"."LOG_Z#CONTACTS" disable';
    execute immediate 'alter trigger "IBS"."USR_Z#CONTACTS_MODIFY_DATE" disable';

	v_text := '::[CLIENT].[BRK_DEFORMATION].[DeformContacts](P$S$C,P$S$N,'||to_char(p_commit)||');'||nl$;
	run_streams(v_text, p_count_strems, 'DeformContacts');
	
    --Z#CONTACTS
    execute immediate 'alter trigger "IBS"."HIST_AFTER_FER_Z#CONTACTS" enable';
    execute immediate 'alter trigger "IBS"."HIST_AFTER_Z#CONTACTS" enable';
    execute immediate 'alter trigger "IBS"."HIST_BEFORE_Z#CONTACTS" enable';
    execute immediate 'alter trigger "IBS"."LOG_Z#CONTACTS" enable';
    execute immediate 'alter trigger "IBS"."USR_Z#CONTACTS_MODIFY_DATE" enable';

	ts2 := utils.get_time;

	&msg('Обезличивание контактов выполнено за '||to_char((ts2-ts1)/100/60,'999990.00')||' мин.');
	
end;
