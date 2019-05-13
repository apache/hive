# Reads a java property file in a shell
# Source: https://stackoverflow.com/questions/1682442/reading-java-properties-file-from-bash
BEGIN {
    FS="=";
    print "# BEGIN";
    n="";
    v="";
    c=0; # Not a line continuation.
}
/^\#/ { # The line is a comment.  Breaks line continuation.
    c=0;
    next;
}
/\\$/ && (c==0) && (NF>=2) { # Name value pair with a line continuation...
    e=index($0,"=");
    n=substr($0,1,e-1);
    v=substr($0,e+1,length($0) - e - 1);    # Trim off the backslash.
    c=1;                                    # Line continuation mode.
    next;
}
/^[^\\]+\\$/ && (c==1) { # Line continuation.  Accumulate the value.
    v= "" v substr($0,1,length($0)-1);
    next;
}
((c==1) || (NF>=2)) && !/^[^\\]+\\$/ { # End of line continuation, or a single line name/value pair
    if (c==0) {  # Single line name/value pair
        e=index($0,"=");
        n=substr($0,1,e-1);
        v=substr($0,e+1,length($0) - e);
    } else { # Line continuation mode - last line of the value.
        c=0; # Turn off line continuation mode.
        v= "" v $0;
    }
    # Make sure the name is a legal shell variable name
    gsub(/[^A-Za-z0-9_]/,"_",n);
    # Remove newlines from the value.
    gsub(/[\n\r]/,"",v);
    print n "=\"" v "\"";
    n = "";
    v = "";
}
END {
    print "# END";
}
