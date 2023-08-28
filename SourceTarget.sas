%include "/sas/prd/SAS94/SAS/Home/ab0296q/libnames.sas";

%macro SourceTarget(input_value, input_value1);

  %let date_var = &input_value;
  %let date_var1 = &input_value1;

  proc sql;
	connect to odbc (datasrc=denodo_odbc_driver user="&username." pass="&password.");
	create table sourcetarget as
		select *
			from connection to odbc
				(
			select information_date,
				txn_date,
				transaction_channel_code,
				internet_cell_txn_type_code,
				user_number,
				error_code,
				from_account_number,
				to_account_number,
				amt,
				limit_amt,
				used_amt,
				processing_date,
				internet_instruction_number,
				beneficiary_name,
				origination_code,
				division
			from oracle_edw_db.absa_transaction_internet_cel_daily
				where to_account_number is not null and 
                   information_date between &date_var and &date_var1
				);
	disconnect from odbc;
	quit;

	proc sql;
		create table sourcetarget1
			as select *,
				input(from_account_number,32.) as source_account_number,
				input(to_account_number,32.) as target_account_number
			from sourcetarget;
	quit;

	proc export data=sourcetarget1
		outfile="/sas/prd/shareu/compliance/SourceTarget_&date_var..csv"
		dbms=csv
		replace;
	run;
%mend;

%SourceTarget('2019-07-01','2019-12-31');

