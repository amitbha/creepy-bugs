#!/usr/bin/awk
# 将连续的非空行连成一行。若为非中文行，则将末尾换行替换为空格

BEGIN {
    save = 1
    out_dir = "talks/"
}

function output(str) {
    if (save == 1) 
        printf("%s", str) > out_dir"/"FILENAME
    else
        printf("%s", str)
}

# 连续行处理
# 跳过元数据段
NR > 21 && /^[^\n]+/, /^$/ {
    if (/^$/) {
        output("\n\n")
    } else {
        # 中文行
        if ($1 ~ /^[^\x00-\xff]/)
            output($0)
        else
            output($0" ")
    }
    next
}

{
    output($0"\n")
}