pragma macro( msg, 'debug_pipe(&METHOD$||TB$||[1], 0)',substitute);
pragma macro( lib, '[DATA_DEFORMATION]::[LIB]');

procedure run_streams(text string(2000), v_streams number)
is
res varchar2(32000);
begin

		res := [TEXT_JOBS]::[APP_LIB].run_streams_wait( text
													 ,v_streams
													,'Обезличка');

		if Not res is null then
			pragma error('Ошибка при исполнении кода через динамический PL/PLUS: '||res);
		end if;
		
end;

procedure ClearContacts(P_STREAM_COUNT number(2,0), P_STREAM_NUM number(2,0)) is
begin

	declare
		type Q_1 is
			select a( a%rowid									: rid
					, to_char( a, 'TM9' )						: id
					)
				in	::[CONTACTS] all
			 where MOD(a%id,P_STREAM_COUNT) = P_STREAM_NUM;

		type tp_cur is ref cursor return Q_1;
		cur tp_cur;
		
		type tp_tab is table of Q_1;
		tab tp_tab;
	begin

		cur.open(Q_1);
		
			cur.fetch(tab);
	
			update for j in tab exceptionloop
				y	( y.[NUMB]	= to_char(y%id) )
				in	::[CONTACTS] all
			where	y%rowid = tab(j).rid;
			commit;
		
		tab.delete;
		cur.close;
	end;
	
end;

procedure RunCleanContacts(P_STREAMS number)
is
	text string(2000);
begin
	debug_pipe('START',0);
	text := 'begin ::[CLIENT].[AKB_OBEZL].ClearContacts(P_STREAM_COUNT == P$S$C, P_STREAM_NUM == P$S$N); end;';
	run_streams(text,P_STREAMS);
	debug_pipe('OK',0);
end;

procedure CleanClPriv(P_STREAM_COUNT number(2,0), P_STREAM_NUM number(2,0)) is

counter number;

begin
	
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
			 where MOD(a%id,P_STREAM_COUNT) = P_STREAM_NUM;

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
		--cur%lock;
		counter	:= 0;

		loop
			cur.fetch(tab);
			--cur.fetch_limit(5000, tab);		-- Выборка с лимитом
			exit when tab.count = 0;
			
			ri_tab.delete;
			for ri in 1..tab.count loop
				ri_tab(ri) := tab(ri).RID;
			end loop;

			counter	:= counter + tab.count;

			&msg(TB$||' Поток '||to_char(P_STREAM_NUM+1)||' информация о клиентах - физических лиц прочитана в количестве: '||&lib.n2ch(tab.count,counter));

		--	::[CL_PRIV] , C_SNILS = substr(''1234567890''||to_char(ID), -11)
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
						, C_DATE_PERS = to_date(''10/05/1945'', ''DD/MM/YYYY'')
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

		--	::[CLIENT]
			update for j in tab exceptionloop
				y	( y.[INN]		= substr(to_char(y%id),1,10)	-- ИНН
					, y.[I_NAME]	= 'PRIVATE ' || to_char(y)	-- интернациональное наименование
					, y.[SI_NAME]	= null	-- Краткое интернациональное наименование
				--	, y.[NAME]		= y.[FAMILY_CL] || ' ' || y.[NAME_CL] || ' ' || y.[SNAME_CL] --было 'Физическое лицо ' || a%id;
					, y.[NAME]		= 'Физ_лицо' || y%id ||' Имя'||substr(to_char(y),-3) ||' Отчество'||substr(to_char(y),-6,3)
					, y.[NOTES]		= null	-- примечания
					)
				in	::[CLIENT]
			where	y%id = tab(j).id;
			commit;
			&msg(TB$||TB$||' Поток '||to_char(P_STREAM_NUM+1)||' ...данные ::[CLIENT] деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));

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
			&msg(TB$||TB$||'...данные о клиентах - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- деформация данных о налоговых инспекциях
			update for j in tab exceptionloop
				y	( y.[INSPECTOR]		= 'Инспектор'
					, y.[NOTES]			= null
					)
				in	::[TAX_INSP] all
			where	cast_to( [number], y%collection ) = tab(j).inspect;
			commit;
			&msg(TB$||TB$||'...данные о налоговых инспекциях клиентов - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));

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

			-- деформация данных о взаимосвязанных клиентах
			update for j in tab exceptionloop
				y	( y.[CLIENT_NAME]	= 'Взаимосвязь ' || tab(j).id )
				in	::[LINKS_CL] all
			where	cast_to( [number], y%collection ) = tab(j).links_other;
			commit;
			&msg(TB$||TB$||'...данные о доп. удостоверениях клиентов - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			var	dSysDate	date;
			dSysDate	:= sysdate;

			-- деформация данных о родственниках
			/*
			update for j in tab exceptionloop
				y	( y.[NAME]				= 'Родственник '||tab(j).id
					, y.[COGNATE_STATUS]	= rCognate
					, y.[BIRTHDATE]			= dSysDate - random(36500)
					, y.[RECOGNATE_STATUS]	= rCognate
					, y.[DEPENDANT]			= null
					, y.[SIGN_FAM]			= null
					)
				in	::[RELATIVES] all
			where	cast_to( [number], y%collection ) = tab(j).rels;

			&msg(TB$||TB$||'...данные о родственниках клиентов - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));
			commit;
*/
--			::[DATA_DEFORMATION].[LIB].DelAccBank(a.[ACC_BANK]);
			-- деформация данных о счетах в банках
			update for j in tab exceptionloop
				y	( y.[ACC_BANKS]	= null )
				in	::[AUR_DOCUM] all
			where	y.[ACC_BANKS] in (select b(b)	in	::[BANKS_ACC] all
															where	cast_to( [number], b%collection ) = tab(j).acc_bank);
			commit;
			&msg(TB$||TB$||TB$||'...[1/4]	"Хозяйственные договоры". '||&lib.ShErr(BULK_EXCEPTIONS.count));
			
			update for j in tab exceptionloop
				y	( y.[ACC_BANKS]	= null )
				in	::[CALENDAR_CALC] all
			where	y.[ACC_BANKS]  in (select b(b)	in	::[BANKS_ACC] all
															where	cast_to( [number], b%collection ) = tab(j).acc_bank);
			
			update for j in tab exceptionloop
				y	( y.[ACC_ADD_RASH_FIL]	= null )
				in	::[CALENDAR_CALC] all
			where	y.[ACC_ADD_RASH_FIL]  in (select b(b)	in	::[BANKS_ACC] all
															where	cast_to( [number], b%collection ) = tab(j).acc_bank);
			
			update for j in tab exceptionloop
				y	( y.[ACC_GRT_RASH_FIL]	= null )
				in	::[CALENDAR_CALC] all
			where	y.[ACC_GRT_RASH_FIL]  in (select b(b)	in	::[BANKS_ACC] all
															where	cast_to( [number], b%collection ) = tab(j).acc_bank);
			
			update for j in tab exceptionloop
				y	( y.[ACC_RASHOD_FIL]	= null )
				in	::[CALENDAR_CALC] all
			where	y.[ACC_RASHOD_FIL]  in (select b(b)	in	::[BANKS_ACC] all
															where	cast_to( [number], b%collection ) = tab(j).acc_bank);
			commit;
			&msg(TB$||TB$||TB$||'...[2/4]	"ТМЦ. Календарь расчетов". '||&lib.ShErr(BULK_EXCEPTIONS.count));

			update for j in tab exceptionloop
				y	( y.[ACC_BANKS]	= null )
				in	::[TMC_ADD_AGR] all
			where	 y.[ACC_BANKS] in (select b(b)	in	::[BANKS_ACC] all
															where	cast_to( [number], b%collection ) = tab(j).acc_bank);
			commit;
			&msg(TB$||TB$||TB$||'...[3/4]	"ТМЦ. Дополнительные соглашения". '||&lib.ShErr(BULK_EXCEPTIONS.count));
			

			delete for j in tab exceptionloop
				y	in	::[BANKS_ACC] all
			where	cast_to( [number], y%collection ) = tab(j).acc_bank;
			commit;
			&msg(TB$||TB$||TB$||'...[4/4]	Очищен массив "Счета в банках". '||&lib.ShErr(BULK_EXCEPTIONS.count));


			-- Контакты для связи
			update for j in tab exceptionloop
				y	( y.[NUMB]	= null )
				in	::[CONTACTS] all
			where	cast_to( [number], y%collection ) = tab(j).contacts;
			commit;
			&msg(TB$||TB$||'...данные о контактах клиентов - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- Склонения наименований
			update for j in tab exceptionloop
				y	( y.[NAME]	= null )
				in	::[DECLENSION] all
			where	cast_to( [number], y%collection ) = tab(j).decl_fio;
			commit;
			&msg(TB$||TB$||'...данные о склонений наименований клиентов - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- Допустимые наименования
			update for j in tab exceptionloop
				y	( y.[NAME]	= null )
				in	::[NAMES] all
			where	cast_to( [number], y%collection ) = tab(j).names;
			commit;
			&msg(TB$||TB$||'...данные о допустимых наименований клиентов - физических лиц деформированы. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- "Документы и изображения" - образцы печатей и подписей
			delete for j in tab exceptionloop
				y	in	::[DOSSIER_DOC] all
			where	cast_to( [number], y%collection ) = tab(j).docs;
			commit;
			&msg(TB$||TB$||TB$||'...Очищен массив образцов печатей и подписей. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- резервирование - информация о клиенте
			delete for j in tab exceptionloop
				y	in	::[RES_CUST_INFO] all
			where	cast_to( [number], y%collection ) = tab(j).res_info;
			commit;
			&msg(TB$||TB$||TB$||'...Очищен массив резервирования с информацией о клиенте. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- социальные фонды
			delete for j in tab exceptionloop
				y	in	::[SOC_FOUNDS] all
			where	cast_to( [number], y%collection ) = tab(j).fonds;
			commit;
			&msg(TB$||TB$||TB$||'...Очищен массив с информацией о социальных фондах. '||&lib.ShErr(BULK_EXCEPTIONS.count));


		exit when cur.notfound;
		end loop;
		cur.close;
		tab.delete;
	end;

	&msg(' Поток '||to_char(P_STREAM_NUM+1)||' Обработано '|| to_char( counter, 'TM9' ) || ' клиентов - физических лиц');

end;

procedure RunCleanClPriv
is
	text string(2000);
begin

	text := 'begin ::[CLIENT].[AKB_OBEZL].CleanClPriv(P_STREAM_COUNT == P$S$C, P_STREAM_NUM == P$S$N); end;';
	run_streams(text,V_STREAMS);

end;