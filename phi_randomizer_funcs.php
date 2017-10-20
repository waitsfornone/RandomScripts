<?php

function date_adjuster($file_name, $date_shift) {
    $i = 0;
    $raw_data = ff($file_name);
    $out_file_name = "dte_".$file_name;
    $out_file = fopen($out_file_name, "a");
    
    foreach ($raw_data as $line) {
        if ($i == 0) {
            $header = array_keys($line);
            fputcsv($out_file, $header);
        }
        $i++;
        foreach($line as $k => $v) {
            if ((strpos($k, 'DTL') === false && strpos($k, 'DT') !== false) && strlen($v) > 0 ) {
                //Format in files 1/1/1997 12:00:00 AM
                $ymd = DateTime::createFromFormat('m/d/Y h:i:s A', $v);
                if ($ymd) {
                    $ymd->add(new DateInterval($date_shift));
                    $new_dte = $ymd->format('m/d/Y h:i:s A');
                } else {
                    $new_dte = "";
                }

                $line[$k] = $new_dte;
            }
        }
        fputcsv($out_file, $line);
    }
    fclose($out_file);
    return $out_file_name;
}

function note_replacement($file_name) {
    $i = 0;
    $raw_data = ff($file_name);
    $out_file_name = "note_".$file_name;
    $out_file = fopen($out_file_name, "w");
    $content = str_replace("<p>","",file_get_contents('http://loripsum.net/api'));
    
    foreach ($raw_data as $line) {
        if ($i == 0) {
            $header = array_keys($line);
            fputcsv($out_file, $header);
        }
        $i++;
        foreach ($line as $k => $v) {
            if (strpos($k, 'NOTE') !== false && strlen($v) > 0 ) {
                $note_len = strlen($v);
                $lorem = substr($content, 0, $note_len);
                $line[$k] = $lorem;
            }
        }
        fputcsv($out_file, $line);
    }
    fclose($out_file);
    return $out_file_name;
}

function email_replacement($file_name) {
    $i = 0;
    $raw_data = ff($file_name);
    $out_file_name = "email_".$file_name;
    $out_file = fopen($out_file_name, "w");

    foreach ($raw_data as $line) {
        if ($i == 0) {
            $header = array_keys($line);
            fputcsv($out_file, $header);
        }
        $i++;
        foreach ($line as $k => $v) {
            if (strpos($k, "EMAIL") !== false && strpos($v, "@") !== false) {
                $new_email = $line["FIRSTNAME"].".".$line["LASTNAME"]."@pmcrm.net";
                $line[$k] = $new_email;
            } 
            if (strpos($k, "EMAIL") !== false && strpos($v, "@") === false && strlen($v) > 0) {
                $line[$k] = "";
            }
        }
        fputcsv($out_file, $line);        
    }
    fclose($out_file);
    return $out_file_name;
}

function marketer_replacement($file_name) {
    $rep_name_file = fopen("rep_name_lookup.csv", "r");
    if (!$rep_name_file) {
        $rep_lookup = array();
        $rep_name_out = fopen("rep_name_lookup.csv", "a");
        fputcsv($rep_name_out, array("REPKEY","NAME"));
        fclose($rep_name_out);  
    } else {
        $rep_lookup = array();
        while (($name = fgetcsv($rep_name_file, 1000, ",")) !== false) {
            if ($name[0] == "REPKEY") {
                continue;
            }
            $rep_lookup[$name[0]] = $name;
        }
        fclose($rep_name_file);    
    }

    $ln_file = fopen("CSV_Database_of_Last_Names.csv", "r");
    $fn_file = fopen("CSV_Database_of_First_Names.csv", "r");
    
    while (($name = fgetcsv($fn_file, 1000, ",")) !== false) {
        $first_names[] = $name[0];
    }
    
    while (($name = fgetcsv($ln_file, 1000, ",")) !== false) {
        $last_names[] = $name[0];
    }

    $file_data = ff($file_name);
    foreach($file_data as $line) {
        $repkey = $line['MARKETINGREPKEY'];
        if (in_array($repkey, $rep_lookup)) {
            continue;
        } else {
            $fkey = array_rand($first_names);
            $f_name = $first_names[$fkey];
            $lkey = array_rand($last_names);
            $l_name = $last_names[$lkey];
            $rep_name = $l_name . ", " . $f_name;
            $tmp_arr = array("MARKETINGREPKEY"=>$repkey, "NAME"=>$rep_name);
            $rep_lookup[$repkey] = $tmp_arr;
        }
    }

    $rep_name_out = fopen("rep_name_lookup.csv", "w");
    fputcsv($rep_name_out, array("REPKEY","NAME"));
    foreach ($rep_lookup as $out_line) {
        fputcsv($rep_name_out, $out_line);    
    }
    fclose($rep_name_out);

    $file_data = ff($file_name);
    $out_file_name = "reps_".$file_name;
    $out_file = fopen($out_file_name, "w");
    $i = 0;
    
    foreach($file_data as $line) {
        if ($i == 0) {
            $header = array_keys($line);
            fputcsv($out_file, $header);
        }
        $i++;
        $line_rep = [];
        if (strlen($line["MARKETINGREPKEY"] > 0)) {
            $line_rep = $rep_lookup[$line["MARKETINGREPKEY"]];        
        }
        foreach ($line as $k => $v) {
            if (($k == "MARKTEINGREP" || $k == "MARKETINGREP") && strlen($v) > 0) {
                $line[$k] = $line_rep["NAME"];
            }
        }
        fputcsv($out_file, $line);
    }
    fclose($out_file);
    return $out_file_name;
}

function name_replacement($file_name) {
    //use the filename to figure out what lookup file and column headers to use
    if (strpos($file_name, "Doctor") !== false) {
        $lookup_file = "doc_name_lookup.csv";
        $key_field = "DOCKEY";
    } elseif (strpos($file_name, "Contact") !== false) {
        $lookup_file = "contact_name_lookup.csv";
        $key_field = "CONTACTKEY";
    } elseif (strpos($file_name, "SO") !== false && strpos($file_name, "Dtl") === false) {
        $lookup_file = "patient_name_lookup.csv";
        $key_field = "PATIENTID";
    } else {
        return $file_name;
    }

    $lookup_name_file = fopen($lookup_file, "r");
    if (!$lookup_name_file) {
        $name_lookup = array();
        $name_out = fopen($lookup_file, "a");
        fputcsv($name_out, array($key_field,"FIRSTNAME","LASTNAME"));
        fclose($name_out);  
    } else {
        $name_lookup = array();
        while (($name = fgetcsv($lookup_name_file, 1000, ",")) !== false) {
            if ($name[0] == $key_field) {
                continue;
            }
            $name_lookup[$name[0]] = $name;
        }
        fclose($lookup_name_file);    
    }

    $ln_file = fopen("CSV_Database_of_Last_Names.csv", "r");
    $fn_file = fopen("CSV_Database_of_First_Names.csv", "r");
    
    while (($name = fgetcsv($fn_file, 1000, ",")) !== false) {
        $first_names[] = $name[0];
    }
    
    while (($name = fgetcsv($ln_file, 1000, ",")) !== false) {
        $last_names[] = $name[0];
    }

    $file_data = ff($file_name);

    foreach($file_data as $line) {
        $key_val = $line[$key_field];
        if (in_array($key_val, $name_lookup)) {
            continue;
        } else {
            $fkey = array_rand($first_names);
            $f_name = $first_names[$fkey];
            $lkey = array_rand($last_names);
            $l_name = $last_names[$lkey];
            $tmp_arr = array($key_field=>$key_val, "FIRSTNAME"=>$f_name, "LASTNAME"=>$l_name);
            $name_lookup[$key_val] = $tmp_arr;
        }
    }
    
    $name_out = fopen($lookup_file, "w");
    fputcsv($name_out, array($key_field,"FIRSTNAME","LASTNAME"));
    foreach ($name_lookup as $out_line) {
        fputcsv($name_out, $out_line);    
    }
    fclose($name_out);

    $file_data = ff($file_name);
    $out_file_name = "names_".$file_name;
    $out_file = $out_file = fopen($out_file_name, "w");
    $i = 0;
    
    foreach($file_data as $line) {
        if ($i == 0) {
            $header = array_keys($line);
            fputcsv($out_file, $header);
        }
        $i++;
        $replace_obj = $name_lookup[$line[$key_field]];
        foreach ($line as $k => $v) {
            if (($k == "LASTNAME" || $k == "PATIENTLASTNAME") && strlen($v) > 0) {
                $line[$k] = $replace_obj["LASTNAME"];
            }
            if (($k == "FIRSTNAME" || $k == "PATIENTFIRSTNAME")&& strlen($v) > 0) {
                $line[$k] = $replace_obj["FIRSTNAME"];
            }
        }
        fputcsv($out_file, $line);
    }
    fclose($out_file);
    return $out_file_name;
}

function ff($file){
	if (preg_match('/csv$/', $file)){
		return ff_both($file, ",");
	}else{
		return ff_both($file, "\t");
	}
}

//File load function
function ff_both($file, $sep){
	global $input_filter;
	global $_ff_batch_size;

	$batch_size = $_ff_batch_size ?? 1000;

	$handle = fopen($file, "r");
	if (!$handle){
		print "can't open file: $file\n";
		exit;
	}
	$start = microtime(true);
	$num_keys = null;
	$keys = [];
	$line = -1;
	while (($tmp = fgetcsv($handle, 4096, $sep)) !== false) {
		$line++;

		if($line == 0){
			$keys = array_map("strtoupper", $tmp);
			$num_keys = count($keys);
		}else{
			if($input_filter){
				if(in_array($input_filter, $tmp) === false){
					continue;
				}
			}
			// the rows are not padded
			if($num_keys != count($tmp)){
				$diff = $num_keys - count($tmp);
				while($diff-- > 0){
					$tmp[] = '';
				}
			}
			yield array_combine($keys, $tmp);
		}
	}
	if (!feof($handle)) {
		echo "Error: unexpected fgetcsv() fail\ on file: $filen";
	}
	fclose($handle);
}