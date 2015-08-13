dir_from="/Users/mengwang/Work/root_git/util"
dir_to="/Users/mengwang/百度云同步盘/Work/util"
dir_log="/Users/mengwang/Work/bin"

mkdir -p $dir_to
rm -rf $dir_to/*
cp -r $dir_from/[0-9a-z]_* $dir_to/
date
