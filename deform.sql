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
		counter	:= 0;

		loop
			cur.fetch_limit(5000, tab);		-- ������� � �������
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