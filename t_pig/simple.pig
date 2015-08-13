rmf -r $out_dir;

-------------------------- Func --------------------------
-------------------------- Main --------------------------

-- Load

INDATA = LOAD '$in_dir' USING PigStorage('\u0001') AS ();

DESCRIBE OUT;
STORE OUT INTO '$out_dir';

fs -chmod 777 $out_dir/..;fs -chmod -R 777 $out_dir;
