<?php

$file_name = $argv[1];
$sql_model = $argv[2];

 $subs = [
	'authorized_official_' => 'ao_',
	'provider_business_practice_location_' => 'pbpl_',
	'provider_license_' => 'pl_',
	'healthcare_provider_' => 'hp_',
	'other_provider_identifier_' => 'opi_'
];

//connect to database
$database = "playmaker";
$host = "192.168.103.102";
$username = "integration";
$pword = "(qaswedfr{};')";
$db_conn = new PDO("pgsql:host=$host;dbname=$database", $username, $pword);
$db_conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$db_conn->setAttribute(PDO::ATTR_ORACLE_NULLS, PDO::NULL_NATURAL);
$db_conn->setAttribute(PDO::ATTR_EMULATE_PREPARES, 0);

$do_table = true;
$doc_count = 0;
$fac_count = 0;

$npi_insert_sth = null;
$fac_insert_sth = null;

$schema_sql = file_get_contents($sql_model);
$create_tables = $db_conn->exec($schema_sql);

$lines = load_file($file_name);

$start = microtime(true);
$num_lines = 0;
$db_conn->beginTransaction();

foreach ($lines as $line) {
	$num_lines++;

	//if we haven't created the table yet, do it
	if ($do_table) {
		$table_cols = [];
		$lut = [];
		//transform the header names to the DB col names
		$cols = array_keys($line);
		foreach ($cols as $ocol) {
			$col = $ocol;
			foreach ($subs as $s => $r) {
				$col = str_replace($s, $r, $col);
			}
			$table_cols[] = "$col text";
			$lut[$col] = $ocol;
		}

		$sql = "CREATE TABLE IF NOT EXISTS rdb_temp.npi (" . implode(",\n", $table_cols) . ");";
		$result = $db_conn->query($sql);

		if (!$result) {
			die("Table creation failed!");
		}

		//create the npi insert statement
		$keys = [];
		$vals = [];

		foreach ($lut as $k => $v) {
			$keys[] = $k;
			$vals[] = $v;
		}

		$sql = "insert into rdb_temp.npi (" . implode(", ", $keys) . ") values (:" . implode(", :", $vals) . ");";
		$npi_insert_sth = $db_conn->prepare($sql);

		$sql = "insert into rdb_temp.npi_taxonomy (npi, code, state) values (:npi, :code, :state)";
		$npi_tax_insert_sth = $db_conn->prepare($sql);

		$fac_lut = [
			'npi' => 'npi',
			'npi_char' => 'npi',
			'replacement_npi' => 'replacement_npi',
			'primary_name' => 'primary_name',
			'organization_name' => 'provider_organization_name',
			'other_organization' => 'provider_other_organization_name',
			'other_organization_type' => 'provider_other_organization_name_type_code',
			'address_1' => 'provider_first_line_business_mailing_address',
			'address_2' => 'provider_second_line_business_mailing_address',
			'city' => 'provider_business_mailing_address_city_name',
			'state' => 'provider_business_mailing_address_state_name',
			'zip' => 'provider_business_mailing_address_postal_code',
			'country_code' => 'provider_business_mailing_address_country_code',
			'phone' => 'provider_business_mailing_address_telephone_number',
			'fax' => 'provider_business_mailing_address_fax_number',
			'bp_address_1' => 'provider_first_line_business_practice_location_address',
			'bp_address_2' => 'provider_second_line_business_practice_location_address',
			'bp_city' => 'provider_business_practice_location_address_city_name',
			'bp_state' => 'provider_business_practice_location_address_state_name',
			'bp_zip' => 'provider_business_practice_location_address_postal_code',
			'bp_zip4' => 'provider_business_practice_location_address_postal_code',
			'bp_country_code' => 'provider_business_practice_location_address_country_code',
			'bp_phone' => 'provider_business_practice_location_address_telephone_number',
			'bp_fax' => 'provider_business_practice_location_address_fax_number',
			'enumeration_date' => 'provider_enumeration_date',
			'last_update' => 'last_update_date',
			'npi_deactivation_reason' => 'npi_deactivation_reason_code',
			'npi_deactivation_date' => 'npi_deactivation_date',
			'npi_reactivation_date' => 'npi_reactivation_date',
			'ao_last_name' => 'authorized_official_last_name',
			'ao_first_name' => 'authorized_offical_first_name',
			'ao_middle_name' => 'authorized_official_middle_name',
			'ao_title' => 'authorized_official_title',
			'ao_phone' => 'authorized_official_phone',
			'hc_taxonomy_code_1' => 'healthcare_provider_taxonomy_code_1',
		];

		$sql = "insert into rdb_temp.facilities (" . implode(', ', array_keys($fac_lut)) . ") values (:" . implode(', :', array_keys($fac_lut)) . ");";
		$fac_insert_sth = $db_conn->prepare($sql);

		$sql = "insert into rdb_temp.facility_taxonomy (npi, code, state) values (:npi, :code, :state)";
		$fac_tax_insert_sth = $db_conn->prepare($sql);

		$do_table = false;
	}
	if (strlen($line['provider_business_mailing_address_state_name']) > 2
		|| strlen($line['provider_business_mailing_address_country_code']) > 2) {
			continue;
	}


	if ($line['entity_type_code'] == 1) {
		$doc_count++;
		$state = $line['provider_business_practice_location_address_state_name'];

		if (strlen($state) != 2) {
			continue;
		}

		$result = $npi_insert_sth->execute($line);

		if (!$result) {
			var_dump($line);
			die("npi insert failed");
		}

		$vals = [
			'npi' => $line['npi'],
			'state' => $state
		];

		for ($i=1; $i < 16; $i++) {
			$code = $line["healthcare_provider_taxonomy_code_" . $i];
			if ($code) {
				$vals['code'] = $code;
				$result = $npi_tax_insert_sth->execute($vals);
				if (!$result) {
					var_dump($vals);
					die("taxonomy insert failed");
				}
			}
		}
	} else {
		$fac_count++;
		$row = [];

		foreach($fac_lut as $k => $v) {
			$row[$k] = $line[$v];
		}

		if ($line['provider_other_organization_name'] && in_array($line['provider_organization_name_type_code'], array(2,3,5))) {
			$row['primary_name'] = $line['provider_other_organization_name'];
		} else {
			$row['primary_name'] = $line['provider_organization_name'];
		}
		$row['bp_zip'] = substr($line['provider_business_practice_location_address_postal_code'], 0, 5);
		$row['bp_zip4'] = substr($line['provider_business_practice_location_address_postal_code'], -1, 4);

		if ($row['npi'] == "") {
			$row['npi'] = 0;
		}

		if ($row['other_organization_type'] == "") {
			$row['other_organization_type'] = 0;
		}

		if ($row['replacement_npi'] == "") {
			$row['replacement_npi'] = 0;
		}

		if ($row['enumeration_date'] == "") {
			$row['enumeration_date'] = null;
		}

		if ($row['last_update'] == "") {
			$row['last_update'] = null;
		}

		if ($row['npi_deactivation_date'] == "") {
			$row['npi_deactivation_date'] = null;
		}

		if ($row['npi_reactivation_date'] == "") {
			$row['npi_reactivation_date'] = null;
		}

		if (strlen($row['state']) > 2) {
			$row['state'] = '';
		}

		if (strlen($row['bp_state']) > 2) {
			$row['bp_state'] = '';
		}

		if (strlen($row['bp_country_code']) > 2) {
			$row['bp_country_code'] = '';
		}

		if (strlen($row['country_code']) > 2) {
			$row['country_code'] = '';
		}

		$row['primary_name'] = substr($row['primary_name'], 0, 100);
		$row['organization_name'] = substr($row['organization_name'], 0, 100);
		$row['other_organization'] = substr($row['other_organization'], 0 , 100);
		$row['city'] = substr($row['city'], 0, 75);
		$row['address_1'] = substr($row['address_1'], 0, 255);
		$row['address_2'] = substr($row['address_2'], 0, 255);

		$result = $fac_insert_sth->execute($row);

		if (!$result) {
			var_dump($line);
			die("facility insert failed");
		}

		$tax_vals = [
			'npi' => $row['npi'],
			'state' => $row['state'],
		];

		for ($i=1; $i < 15; $i++) {
			$code = $line["healthcare_provider_taxonomy_code_" . $i];
			if ($code) {
				$tax_vals['code'] = $code;
				$result = $fac_tax_insert_sth->execute($tax_vals);
				if (!$result) {
					var_dump($tax_vals);
					die("facility taxonomy insert failed");
				}
			}
		}
	}

	if($num_lines % 1000 == 0){
		$elap = microtime(true) - $start;
		$ps = $num_lines / $elap;

		print implode("\t", [number_format($num_lines), $ps]) . "\n";
		$db_conn->commit();
		$db_conn->beginTransaction();
	}

}
$db_conn->commit();

error_log("Doctors processed: $doc_count"."\n");
error_log("Facilities processed: $fac_count"."\n");

//Load State tables for Doctors


$state_list = "SELECT DISTINCT abbrev FROM rdb_temp.states;";
$results = $db_conn->query($state_list)->fetchAll();


foreach ($results as $row) {
	$upper_state = $row['abbrev'];
	$state = strtolower($upper_state);
	$npi_state_create = "CREATE TABLE rdb_temp.npi_$state (
		id serial primary key,
		npi bigint not null,
		npi_char char(11) not null,
		replacement_npi bigint default 0,
		last_name varchar(60),
		first_name varchar(60),
		middle_name varchar(60),
		prefix varchar(5),
		suffix varchar(5),
		credential varchar(100),
		contact_type varchar(35),
		other_last_name varchar(60),
		other_first_name varchar(60),
		other_middle_name varchar(60),
		other_prefix varchar(5),
		other_suffix varchar(5),
		other_credential varchar(100),
		other_last_name_type smallint,
		address_1 varchar(255),
		address_2 varchar(255),
		city varchar(50),
		state varchar(2),
		zip char(5),
		zip4 char(4),
		address_id int,
		phone varchar(15),
		fax varchar(15),
		bp_address_1 varchar(255),
		bp_address_2 varchar(255),
		bp_city varchar(50),
		bp_state varchar(2),
		bp_zip varchar(10),
		bp_phone varchar(15),
		bp_fax varchar(15),
		enumeration_date date,
		last_update date,
		npi_deactivation_date date,
		npi_reactivation_date date,
		gender_code char(1),
		ao_last_name varchar(50),
		ao_first_name varchar(50),
		ao_middle_name varchar(50),
		ao_title varchar(50),
		ao_phone varchar(50),
		last_modified_on timestamptz default now(),
		_last_geocoded_on timestamptz default now(),
		_lat float default null,
		_lon float default null);";
	$db_conn->query($npi_state_create);
	$state_doc_sql = "INSERT INTO rdb_temp.npi_" . $state .
		" SELECT nextval('rdb_temp.npi_" . $state . "_id_seq'::regclass),
		case when npi = '' then null else npi::bigint end,
		npi,
		case when replacement_npi = '' then null else replacement_npi::bigint end,
		LEFT(provider_last_name, 60),
		LEFT(provider_first_name, 60),
		LEFT(provider_middle_name, 60),
		provider_name_prefix_text,
		provider_name_suffix_text,
		provider_credential_text,
		NULL,
		LEFT(provider_other_last_name, 60),
		LEFT(provider_other_first_name, 60),
		LEFT(provider_other_middle_name, 60),
		provider_other_name_prefix_text,
		provider_other_name_suffix_text,
		provider_other_credential_text,
		case when provider_other_last_name_type_code = '' then null else
		provider_other_last_name_type_code::smallint end,
		LEFT(provider_first_line_business_practice_location_address, 255),
		LEFT(provider_second_line_business_practice_location_address, 255),
		LEFT(pbpl_address_city_name, 50),
		pbpl_address_state_name,
		LEFT(pbpl_address_postal_code, 5) as zip,
		TRIM(RIGHT(pbpl_address_postal_code, 4)) as zip4,
		0,
		pbpl_address_telephone_number,
		pbpl_address_fax_number,
		provider_first_line_business_mailing_address,
		provider_second_line_business_mailing_address,
		provider_business_mailing_address_city_name,
		provider_business_mailing_address_state_name,
		provider_business_mailing_address_postal_code,
		provider_business_mailing_address_telephone_number,
		provider_business_mailing_address_fax_number,
		case when provider_enumeration_date = '' then null else provider_enumeration_date::date end,
		case when last_update_date = '' then null else last_update_date::date end,
		case when npi_deactivation_date = '' then null else npi_deactivation_date::date end,
		case when npi_reactivation_date = '' then null else npi_reactivation_date::date end,
		provider_gender_code,
		ao_last_name,
		ao_first_name,
		ao_middle_name,
		ao_title_or_position,
		ao_telephone_number,
		NOW(),
		NULL,
		NULL,
		NULL
		FROM rdb_temp.npi WHERE
		pbpl_address_state_name = '$upper_state';";
	$db_conn->query($state_doc_sql);
	for ($i=1; $i<16; $i++) {
		if ($i == 1) {
			$state_doc_tax_sql = "SELECT npi::bigint, hp_taxonomy_code_$i AS code INTO
				rdb_temp.npi_taxonomy_$state FROM rdb_temp.npi where
				pbpl_address_state_name = '$upper_state' AND
				hp_taxonomy_code_$i != '';";
		} else {
			$state_doc_tax_sql = "INSERT INTO rdb_temp.npi_taxonomy_$state SELECT
				npi::bigint, hp_taxonomy_code_$i as code FROM rdb_temp.npi WHERE
				pbpl_address_state_name = '$upper_state' AND
				hp_taxonomy_code_$i != '';";
		}
		$db_conn->query($state_doc_tax_sql);
	}
}

$db_conn->query("alter schema master_lists rename to master_lists_old;");
$db_conn->query("alter schema rdb_temp rename to master_lists;");


function load_file($filename) {

	$handle = fopen($filename, "r");
	if (!$handle) {
		print "can't open file: $file\n";
		exit;
	}
	$keys = [];
	$line_count = -1;
	while (($tmp = fgetcsv($handle, 4096)) !== false) {
		$line_count++;
		if ($line_count == 0) {
			$keys = array_map("strtolower", $tmp);
			$keys = str_replace(" ", "_", $keys);
			$keys = str_replace('"', "", $keys);
			foreach ($keys as $n => $k) {
				$spos = strpos($k, '(');
				if ($spos > 0) {
					$keys[$n] = substr($k, 0, $spos - 1);
				} elseif ($spos == 0) {
					$keys[$n] = trim(trim($k, "("), ")");
				} else {
					$keys[$n] = $k;
				}
			}
			$num_keys = count($keys);
		} else {
			//the rows are not padded
			if ($num_keys != count($tmp)) {
				$diff = $num_keys - count($tmp);
				while ($diff-- > 0) {
					$tmp[] = '';
				}
			}
			yield array_combine($keys, $tmp);
		}
	}
	if (!feof($handle)) {
		echo "Error: unexpected fgetcsv() fail\ on file: $file_name";
	}
	fclose($handle);
}

