<?php

$file_name = $argv[1];

require("phi_randomizer_funcs.php");

//we need to take the file type from the file name and only run the necessary functions
//they need to be in the correct order

if (strpos($file_name, "SO") !== false && strpos($file_name, "Dtl") === false) {
    $date_done = date_adjuster($file_name, 'P1Y');
    $note_done = note_replacement($date_done);
    $patient_done = name_replacement($note_done);
    $final_so_file = marketer_replacement($patient_done);
} elseif (strpos($file_name, "SODtl") !== false) {
    date_adjuster($file_name, 'P1Y');    
} elseif (strpos($file_name, "Contact") !== false) {
    $date_done = date_adjuster($file_name, 'P1Y');
    $name_done = name_replacement($date_done);
    $email_done = email_replacement($name_done);
} elseif (strpos($file_name, "Facility") !== false) {
    $date_done = date_adjuster($file_name, 'P1Y');
    $rep_done = marketer_replacement($date_done);    
} elseif (strpos($file_name, "Doctor") !== false) {
    $date_done = date_adjuster($file_name, 'P1Y');
    $name_done = name_replacement($date_done);
    $rep_done = marketer_replacement($name_done);
    $email_done = email_replacement($rep_done);    
} else {
    exit;
}