pragma macro( msg, 'debug_pipe(&METHOD$||TB$||[1], 0)',substitute);
pragma macro( lib, '[DATA_DEFORMATION]::[LIB]');

procedure run_streams(text string(2000), v_streams number)
is
res varchar2(32000);
begin

		res := [TEXT_JOBS]::[APP_LIB].run_streams_wait( text
													 ,v_streams
													,'���������');

		if Not res is null then
			pragma error('������ ��� ���������� ���� ����� ������������ PL/PLUS: '||res);
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
					, cast_to( [number], a.[CONTACTS])			: contacts	-- �������� ��� �����
					, cast_to( [number], a.[DECL_FIO])			: decl_fio	-- ��������� ������������
					, cast_to( [number], a.[NAMES])				: names		-- ���������� ������������
					, cast_to( [number], a.[DOCS])				: docs		-- ������� ������� � ��������
					, cast_to( [number], a.[RES_INFO])			: res_info	-- �������������� - ���������� � �������
					, cast_to( [number], a.[FONDS_AR])			: fonds		-- ���������� �����
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
			--cur.fetch_limit(5000, tab);		-- ������� � �������
			exit when tab.count = 0;
			
			ri_tab.delete;
			for ri in 1..tab.count loop
				ri_tab(ri) := tab(ri).RID;
			end loop;

			counter	:= counter + tab.count;

			&msg(TB$||' ����� '||to_char(P_STREAM_NUM+1)||' ���������� � �������� - ���������� ��� ��������� � ����������: '||&lib.n2ch(tab.count,counter));

		--	::[CL_PRIV] , C_SNILS = substr(''1234567890''||to_char(ID), -11)
			begin
				execute immediate '
				begin
					forall j in indices of :tab save exceptions
					update Z#CL_PRIV set
						SN = nvl(SN, 1) + 1, SU = rtl.uid$
						, C_DOC#BIRTH_PLACE = ''_�����_ _��������_ '' || to_char(ID)
						, C_DOC#NUM = ''1234567890''
						, C_DOC#SER = ''XXYY''
						, C_DOC#WHO = ''��� ��� ��� ��''
						, C_DATE_PERS = to_date(''10/05/1945'', ''DD/MM/YYYY'')
						, C_BORN = null
						, C_LOW = null
						, C_FAMILY_CL = ''���_����'' || to_char(ID)
						, C_NAME_CL = ''���'' || substr(to_char(ID), -3)
						, C_SNAME_CL = ''��������'' || substr(to_char(ID), -6, 3)
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
				y	( y.[INN]		= substr(to_char(y%id),1,10)	-- ���
					, y.[I_NAME]	= 'PRIVATE ' || to_char(y)	-- ����������������� ������������
					, y.[SI_NAME]	= null	-- ������� ����������������� ������������
				--	, y.[NAME]		= y.[FAMILY_CL] || ' ' || y.[NAME_CL] || ' ' || y.[SNAME_CL] --���� '���������� ���� ' || a%id;
					, y.[NAME]		= '���_����' || y%id ||' ���'||substr(to_char(y),-3) ||' ��������'||substr(to_char(y),-6,3)
					, y.[NOTES]		= null	-- ����������
					)
				in	::[CLIENT]
			where	y%id = tab(j).id;
			commit;
			&msg(TB$||TB$||' ����� '||to_char(P_STREAM_NUM+1)||' ...������ ::[CLIENT] �������������. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- ���������� ������ �� �������
			update for j in tab exceptionloop
				y	( y.[FLAT] 			= '1'
					, y.[HOUSE]			= '456'
					, y.[KORPUS]		= '�'
					, y.[POST_CODE]		= '654321'
					, y.[STREET_STR]	= '����� ������������ �����'
					, y.[STREET_REF]	= null
					, y.[IMP_STR]		= null
					)
				in	::[PERSONAL_ADDRESS] all
			where	cast_to( [number], y%collection ) = tab(j).addr;
			commit;
			&msg(TB$||TB$||'...������ � �������� - ���������� ��� �������������. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- ���������� ������ � ��������� ����������
			update for j in tab exceptionloop
				y	( y.[INSPECTOR]		= '���������'
					, y.[NOTES]			= null
					)
				in	::[TAX_INSP] all
			where	cast_to( [number], y%collection ) = tab(j).inspect;
			commit;
			&msg(TB$||TB$||'...������ � ��������� ���������� �������� - ���������� ��� �������������. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- ���������� ������ � ���. ��������������
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
					, C_BIRTH_PLACE = ''_�����_ _��������_ '' || :tab(j)
					, C_NUM = ''1234567890''
					, C_SER = ''XXYY''
					, C_WHO = ''��� ��� ��� ��''
				where COLLECTION_ID = :tab(j);
			end;'
			using tab_cert;
			commit;
			&msg(TB$||TB$||'...������ � ���. �������������� �������� - ���������� ��� �������������. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- ���������� ������ � ��������������� ��������
			update for j in tab exceptionloop
				y	( y.[CLIENT_NAME]	= '����������� ' || tab(j).id )
				in	::[LINKS_CL] all
			where	cast_to( [number], y%collection ) = tab(j).links_other;
			commit;
			&msg(TB$||TB$||'...������ � ���. �������������� �������� - ���������� ��� �������������. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			var	dSysDate	date;
			dSysDate	:= sysdate;

			-- ���������� ������ � �������������
			/*
			update for j in tab exceptionloop
				y	( y.[NAME]				= '����������� '||tab(j).id
					, y.[COGNATE_STATUS]	= rCognate
					, y.[BIRTHDATE]			= dSysDate - random(36500)
					, y.[RECOGNATE_STATUS]	= rCognate
					, y.[DEPENDANT]			= null
					, y.[SIGN_FAM]			= null
					)
				in	::[RELATIVES] all
			where	cast_to( [number], y%collection ) = tab(j).rels;

			&msg(TB$||TB$||'...������ � ������������� �������� - ���������� ��� �������������. '||&lib.ShErr(BULK_EXCEPTIONS.count));
			commit;
*/
--			::[DATA_DEFORMATION].[LIB].DelAccBank(a.[ACC_BANK]);
			-- ���������� ������ � ������ � ������
			update for j in tab exceptionloop
				y	( y.[ACC_BANKS]	= null )
				in	::[AUR_DOCUM] all
			where	y.[ACC_BANKS] in (select b(b)	in	::[BANKS_ACC] all
															where	cast_to( [number], b%collection ) = tab(j).acc_bank);
			commit;
			&msg(TB$||TB$||TB$||'...[1/4]	"������������� ��������". '||&lib.ShErr(BULK_EXCEPTIONS.count));
			
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
			&msg(TB$||TB$||TB$||'...[2/4]	"���. ��������� ��������". '||&lib.ShErr(BULK_EXCEPTIONS.count));

			update for j in tab exceptionloop
				y	( y.[ACC_BANKS]	= null )
				in	::[TMC_ADD_AGR] all
			where	 y.[ACC_BANKS] in (select b(b)	in	::[BANKS_ACC] all
															where	cast_to( [number], b%collection ) = tab(j).acc_bank);
			commit;
			&msg(TB$||TB$||TB$||'...[3/4]	"���. �������������� ����������". '||&lib.ShErr(BULK_EXCEPTIONS.count));
			

			delete for j in tab exceptionloop
				y	in	::[BANKS_ACC] all
			where	cast_to( [number], y%collection ) = tab(j).acc_bank;
			commit;
			&msg(TB$||TB$||TB$||'...[4/4]	������ ������ "����� � ������". '||&lib.ShErr(BULK_EXCEPTIONS.count));


			-- �������� ��� �����
			update for j in tab exceptionloop
				y	( y.[NUMB]	= null )
				in	::[CONTACTS] all
			where	cast_to( [number], y%collection ) = tab(j).contacts;
			commit;
			&msg(TB$||TB$||'...������ � ��������� �������� - ���������� ��� �������������. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- ��������� ������������
			update for j in tab exceptionloop
				y	( y.[NAME]	= null )
				in	::[DECLENSION] all
			where	cast_to( [number], y%collection ) = tab(j).decl_fio;
			commit;
			&msg(TB$||TB$||'...������ � ��������� ������������ �������� - ���������� ��� �������������. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- ���������� ������������
			update for j in tab exceptionloop
				y	( y.[NAME]	= null )
				in	::[NAMES] all
			where	cast_to( [number], y%collection ) = tab(j).names;
			commit;
			&msg(TB$||TB$||'...������ � ���������� ������������ �������� - ���������� ��� �������������. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- "��������� � �����������" - ������� ������� � ��������
			delete for j in tab exceptionloop
				y	in	::[DOSSIER_DOC] all
			where	cast_to( [number], y%collection ) = tab(j).docs;
			commit;
			&msg(TB$||TB$||TB$||'...������ ������ �������� ������� � ��������. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- �������������� - ���������� � �������
			delete for j in tab exceptionloop
				y	in	::[RES_CUST_INFO] all
			where	cast_to( [number], y%collection ) = tab(j).res_info;
			commit;
			&msg(TB$||TB$||TB$||'...������ ������ �������������� � ����������� � �������. '||&lib.ShErr(BULK_EXCEPTIONS.count));

			-- ���������� �����
			delete for j in tab exceptionloop
				y	in	::[SOC_FOUNDS] all
			where	cast_to( [number], y%collection ) = tab(j).fonds;
			commit;
			&msg(TB$||TB$||TB$||'...������ ������ � ����������� � ���������� ������. '||&lib.ShErr(BULK_EXCEPTIONS.count));


		exit when cur.notfound;
		end loop;
		cur.close;
		tab.delete;
	end;

	&msg(' ����� '||to_char(P_STREAM_NUM+1)||' ���������� '|| to_char( counter, 'TM9' ) || ' �������� - ���������� ���');

end;

procedure RunCleanClPriv
is
	text string(2000);
begin

	text := 'begin ::[CLIENT].[AKB_OBEZL].CleanClPriv(P_STREAM_COUNT == P$S$C, P_STREAM_NUM == P$S$N); end;';
	run_streams(text,V_STREAMS);

end;