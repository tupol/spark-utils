this is a test -r h;s/^\S+\s(\S+).*/md5sum <<<"\1"/e;G;s/^(\S+).*
(\S+)\s\S+\s(.*)/\2 \1 \3/
